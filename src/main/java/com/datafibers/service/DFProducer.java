package com.datafibers.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.StaticHandler;

import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

/**
 * DF Producer is used to route producer service to kafka connect rest or lunch locally
 * The overall status is maintained in the its local database - mongo
 */

public class DFProducer extends AbstractVerticle {

  public static final String COLLECTION = "df_trans";
  private MongoClient mongo;

  /**
   * This method is called when the verticle is deployed. It creates a HTTP server and registers a simple request
   * handler.
   * <p/>
   * Notice the `listen` method. It passes a lambda checking the port binding result. When the HTTP server has been
   * bound on the port, it call the `complete` method to inform that the starting has completed. Else it reports the
   * error.
   *
   * @param fut the future
   */
  @Override
  public void start(Future<Void> fut) {

    // Create a Mongo client
    mongo = MongoClient.createShared(vertx, config());


    createSomeData(
        (nothing) -> startWebApp(
            (http) -> completeStartup(http, fut)
        ), fut);
  }

  private void startWebApp(Handler<AsyncResult<HttpServer>> next) {
    // Create a router object.
    Router router = Router.router(vertx);

    // Bind "/" to our hello message.
    router.route("/").handler(routingContext -> {
      HttpServerResponse response = routingContext.response();
      response
          .putHeader("content-type", "text/html")
          .end("<h1>Hello from DF producer service</h1>");
    });

    router.route("/assets/*").handler(StaticHandler.create("assets"));

    router.get("/api/df/ps").handler(this::getAll);
    router.route("/api/df/ps*").handler(BodyHandler.create());
    router.post("/api/df/ps").handler(this::addOne);
    router.get("/api/df/ps/:id").handler(this::getOne);
    router.put("/api/df/ps/:id").handler(this::updateOne);
    router.delete("/api/df/ps/:id").handler(this::deleteOne);


    // Create the HTTP server and pass the "accept" method to the request handler.
    vertx
        .createHttpServer()
        .requestHandler(router::accept)
        .listen(
            // Retrieve the port from the configuration,
            // default to 8080.
            config().getInteger("http.port", 8080),
            next::handle
        );
  }

  private void completeStartup(AsyncResult<HttpServer> http, Future<Void> fut) {
    if (http.succeeded()) {
      fut.complete();
    } else {
      fut.fail(http.cause());
    }
  }


  @Override
  public void stop() throws Exception {
    mongo.close();
  }

  private void addOne(RoutingContext routingContext) {

    System.out.println("received the body is:" + routingContext.getBodyAsString());

    final DFJob dfJob = Json.decodeValue(routingContext.getBodyAsString(), DFJob.class);
    //TODO add to Kafka REST

    mongo.insert(COLLECTION, dfJob.toJson(), r ->
        routingContext.response()
            .setStatusCode(201)
            .putHeader("content-type", "application/json; charset=utf-8")
            .end(Json.encodePrettily(dfJob.setId(r.result()))));
  }

  private void getOne(RoutingContext routingContext) {
    final String id = routingContext.request().getParam("id");
    if (id == null) {
      routingContext.response().setStatusCode(400).end();
    } else {
      mongo.findOne(COLLECTION, new JsonObject().put("_id", id), null, ar -> {
        if (ar.succeeded()) {
          if (ar.result() == null) {
            routingContext.response().setStatusCode(404).end();
            return;
          }
          DFJob dfJob = new DFJob(ar.result());
          routingContext.response()
              .setStatusCode(200)
              .putHeader("content-type", "application/json; charset=utf-8")
              .end(Json.encodePrettily(dfJob));
        } else {
          routingContext.response().setStatusCode(404).end();
        }
      });
    }
  }

  private void updateOne(RoutingContext routingContext) {
    final String id = routingContext.request().getParam("id");
    JsonObject json = routingContext.getBodyAsJson();
    if (id == null || json == null) {
      routingContext.response().setStatusCode(400).end();
    } else {
      mongo.update(COLLECTION,
          new JsonObject().put("_id", id), // Select a unique document
          // The update syntax: {$set, the json object containing the fields to update}
          new JsonObject()
              .put("$set", json),
          v -> {
            if (v.failed()) {
              routingContext.response().setStatusCode(404).end();
            } else {
              routingContext.response()
                  .putHeader("content-type", "application/json; charset=utf-8")
                  .end(Json.encodePrettily(new DFJob(id, json.getString("name"), json.getString("origin"))));
            }
          });
    }
  }

  private void deleteOne(RoutingContext routingContext) {
    String id = routingContext.request().getParam("id");
    if (id == null) {
      routingContext.response().setStatusCode(400).end();
    } else {
      mongo.removeOne(COLLECTION, new JsonObject().put("_id", id),
          ar -> routingContext.response().setStatusCode(204).end());
    }
  }

  private void getAll(RoutingContext routingContext) {
    mongo.find(COLLECTION, new JsonObject(), results -> {
      List<JsonObject> objects = results.result();
      List<DFJob> jobs = objects.stream().map(DFJob::new).collect(Collectors.toList());
      routingContext.response()
          .putHeader("content-type", "application/json; charset=utf-8")
          .end(Json.encodePrettily(jobs));
    });
  }

  private void createSomeData(Handler<AsyncResult<Void>> next, Future<Void> fut) {

    HashMap<String, String> hm = new HashMap<String, String>();
    hm.put("path","/tmp/a.json");
    DFJob job1 = new DFJob("Stream files job", "File Streamer Connector", "Register", hm, hm);
    DFJob job2 = new DFJob("Batch files job", "File Batch Connector", "Register", hm, hm);


    // Do we have data in the collection ?
    mongo.count(COLLECTION, new JsonObject(), count -> {
      if (count.succeeded()) {
        if (count.result() == 0) {
          // no jobs, insert data
          mongo.insert(COLLECTION, job1.toJson(), ar -> {
            if (ar.failed()) {
              fut.fail(ar.cause());
            } else {
              mongo.insert(COLLECTION, job2.toJson(), ar2 -> {
                if (ar2.failed()) {
                  fut.failed();
                } else {
                  next.handle(Future.<Void>succeededFuture());
                }
              });
            }
          });
        } else {
          next.handle(Future.<Void>succeededFuture());
        }
      } else {
        // report the error
        fut.fail(count.cause());
      }
    });
  }
}
