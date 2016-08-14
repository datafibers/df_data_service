package com.datafibers.service;

import com.datafibers.model.DFJobPOPJ;
import com.datafibers.util.Runner;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.hubrick.vertx.rest.MediaType;
import com.hubrick.vertx.rest.RestClient;
import com.hubrick.vertx.rest.RestClientOptions;
import com.hubrick.vertx.rest.RestClientRequest;
import com.hubrick.vertx.rest.converter.FormHttpMessageConverter;
import com.hubrick.vertx.rest.converter.HttpMessageConverter;
import com.hubrick.vertx.rest.converter.JacksonJsonHttpMessageConverter;
import com.hubrick.vertx.rest.converter.StringHttpMessageConverter;
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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

/**
 * DF Producer is used to route producer service to kafka connect rest or lunch locally
 * The overall status is maintained in the its local database - mongo
 */

public class DFProducer extends AbstractVerticle {

  public static final String COLLECTION = "df_prod";
  private MongoClient mongo;
  private Boolean kafka_connect_enabled;
  private String kafka_connect_rest_host;
  private Integer kafka_connect_rest_port;
  private RestClient rc;

  public static void main(String[] args) //For debug purpose only
  {

    Runner.runExample(DFProducer.class);

  }

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
    router.post("/api/df/ps").handler(this::addOne); // Need kafka connect (KC) forward
    router.get("/api/df/ps/:id").handler(this::getOne);
    router.put("/api/df/ps/:id").handler(this::updateOne); // Need kafka connect (KC) forward
    router.delete("/api/df/ps/:id").handler(this::deleteOne); // Need kafka connect (KC) forward
    router.options("/api/df/ps/:id").handler(this::corsHandle);
    router.options("/api/df/ps").handler(this::corsHandle);


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

    // Check if Kafka Connect is enabled from configuration and other settings
    this.kafka_connect_enabled = config().getBoolean("kafka_connect_enable", Boolean.TRUE );
    this.kafka_connect_rest_host = config().getString("kafka_connect_rest_host", "localhost");
    this.kafka_connect_rest_port = config().getInteger("kafka_connect_rest_port", 8083);

    // Start REST API Client for Kafka Connect if needed
    if(this.kafka_connect_enabled) {

      final ObjectMapper objectMapper = new ObjectMapper();
      final List<HttpMessageConverter> httpMessageConverters = ImmutableList.of(
              new FormHttpMessageConverter(),
              new StringHttpMessageConverter(),
              new JacksonJsonHttpMessageConverter(objectMapper)
      );

      final RestClientOptions restClientOptions = new RestClientOptions()
              .setConnectTimeout(1000)
              .setGlobalRequestTimeout(5000)
              .setDefaultHost(this.kafka_connect_rest_host)
              .setDefaultPort(this.kafka_connect_rest_port)
              .setKeepAlive(true)
              .setMaxPoolSize(500);

      this.rc = RestClient.create(vertx, restClientOptions, httpMessageConverters);
    }


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

    final DFJobPOPJ dfJob = Json.decodeValue(routingContext.getBodyAsString(), DFJobPOPJ.class);

    System.out.println("repack for kafka is:" + dfJob.toKafkaConnectJson().toString());

    //TODO add to generic function to validate the connect first
    //TODO fetch all available connectors to submit job
    //TODO Connector|Job Status
    if(this.kafka_connect_enabled && dfJob.getConnectorType().equalsIgnoreCase("KAFKA_CONNECT")) {

      // add to kafka connect first simply to string
      final RestClientRequest postRestClientRequest = rc.post("/connectors", String.class, portRestResponse -> {

        String rs = portRestResponse.getBody();
        System.out.println("receiving response form Kafka Connect: " + rs);
        JsonObject jo = new JsonObject(rs);
        System.out.println("json object name: " + jo.getString("name"));
        System.out.println("json object config: " + jo.getJsonObject("config"));
        System.out.println("json object tasks: " + jo.getMap().get("tasks"));
        System.out.println("receiving response from kafka connect: " + portRestResponse.statusMessage());
        System.out.println("receiving response from kafka connect: " + portRestResponse.statusCode());

        // Add records to local repository
        mongo.insert(COLLECTION, dfJob.toJson(), r ->
                routingContext.response()
                        .setStatusCode(201)
                        .putHeader("content-type", "application/json; charset=utf-8")
                        .end(Json.encodePrettily(dfJob.setId(r.result()))));
      });

      postRestClientRequest.exceptionHandler(exception -> {

        routingContext.response()
                .setStatusCode(409)
                .putHeader("content-type", "application/json; charset=utf-8")
                .end(errorMsg(10, "Response exception from Kafka Connect for duplicated connectors - "
                        + exception.toString()));
      });

      postRestClientRequest.setContentType(MediaType.APPLICATION_JSON);
      postRestClientRequest.setAcceptHeader(Arrays.asList(MediaType.APPLICATION_JSON));
      postRestClientRequest.end(dfJob.toKafkaConnectJson().toString());

    } else {
      mongo.insert(COLLECTION, dfJob.toJson(), r ->
              routingContext.response()
                      .setStatusCode(201)
                      .putHeader("content-type", "application/json; charset=utf-8")
                      .end(Json.encodePrettily(dfJob.setId(r.result()))));
    }

  }

  private void getOne(RoutingContext routingContext) {
    final String id = routingContext.request().getParam("id");
    if (id == null) {
      routingContext.response().setStatusCode(400).end(errorMsg(20, "id is null in your request."));
    } else {
      mongo.findOne(COLLECTION, new JsonObject().put("_id", id), null, ar -> {
        if (ar.succeeded()) {
          if (ar.result() == null) {
            routingContext.response().setStatusCode(404).end(errorMsg(21, "id cannot find in repository."));
            return;
          }
          DFJobPOPJ dfJob = new DFJobPOPJ(ar.result());
          routingContext.response()
              .setStatusCode(200)
              .putHeader("content-type", "application/json; charset=utf-8")
              .end(Json.encodePrettily(dfJob));
        } else {
          routingContext.response().setStatusCode(404).end(errorMsg(22, "Search id in repository failed."));
        }
      });
    }
  }

  private void updateOne(RoutingContext routingContext) {

    System.out.println("received the body is from updateOne:" + routingContext.getBodyAsString());

    final String id = routingContext.request().getParam("id");
    final DFJobPOPJ dfJob = Json.decodeValue(routingContext.getBodyAsString(), DFJobPOPJ.class);
    JsonObject json = dfJob.toJson();
    if (id == null || json == null) {
      routingContext.response().setStatusCode(400).end("ERROR-00004: id is null in your request.");
    } else {
      mongo.update(COLLECTION,
          new JsonObject().put("_id", id), // Select a unique document
          // The update syntax: {$set, the json object containing the fields to update}
          new JsonObject()
              .put("$set", json),
          v -> {
            if (v.failed()) {
              routingContext.response().setStatusCode(404).end(errorMsg(30, "updateOne to repository is failed."));
            } else {
              routingContext.response()
                  .putHeader("content-type", "application/json; charset=utf-8")
                  .end();
            }
          });
    }
  }

  private void deleteOne(RoutingContext routingContext) {
    String id = routingContext.request().getParam("id");
    if (id == null) {
      routingContext.response().setStatusCode(400).end(errorMsg(30, "id is null in your request."));
    } else {
      mongo.removeOne(COLLECTION, new JsonObject().put("_id", id),
          ar -> routingContext.response().end(id + " is deleted from repository."));
    }
  }

  private void getAll(RoutingContext routingContext) {
    mongo.find(COLLECTION, new JsonObject(), results -> {
      List<JsonObject> objects = results.result();
      List<DFJobPOPJ> jobs = objects.stream().map(DFJobPOPJ::new).collect(Collectors.toList());
      routingContext.response()
          .putHeader("content-type", "application/json; charset=utf-8")
          .end(Json.encodePrettily(jobs));
    });
  }

  public void corsHandle(RoutingContext routingContext) {

    routingContext.response().putHeader("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE")
    .putHeader("Access-Control-Allow-Headers", "X-Requested-With, Content-Type")
    .putHeader("Access-Control-Max-Age", "60").end();
  }
//TODO need initial method to import all available|paused|running connectors from kafka connect
  private void createSomeData(Handler<AsyncResult<Void>> next, Future<Void> fut) {

    HashMap<String, String> hm = new HashMap<String, String>();
    hm.put("path","/tmp/a.json");
    DFJobPOPJ job1 = new DFJobPOPJ("Stream files job", "File Streamer Connector", "Register", hm, hm);
    DFJobPOPJ job2 = new DFJobPOPJ("Batch files job", "File Batch Connector", "Register", hm, hm);


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

  /**
   * Print error message in better JSON format
   * @param error_code
   * @param msg
   * @return
     */
  private String errorMsg(int error_code, String msg) {
    return Json.encodePrettily(new JsonObject()
            .put("code", String.format("%6d", error_code))
            .put("message", msg));
  }
}
