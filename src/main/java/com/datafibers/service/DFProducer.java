package com.datafibers.service;

import com.datafibers.model.DFJobPOPJ;
import com.datafibers.util.ConstantApp;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

/**
 * DF Producer is used to route producer service to kafka connect rest or lunch locally
 * The overall status is maintained in the its local database - mongo
 */

public class DFProducer extends AbstractVerticle {

    public static String COLLECTION;
    private MongoClient mongo;
    private static Boolean kafka_connect_enabled;
    private static String kafka_connect_rest_host;
    private static Integer kafka_connect_rest_port;
    private RestClient rc;

    private static final Logger LOG = LoggerFactory.getLogger(DFProducer.class);

    /**
     * This main() is used for debug purpose only
     *
     * @param args
     */
    public static void main(String[] args) {
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
        LOG.info("Start PDF Producer Service...");
        // Create a Mongo client
        mongo = MongoClient.createShared(vertx, config());
        // Add sample data
        createSomeData((nothing) -> startWebApp((http) -> completeStartup(http, fut)), fut);
    }

    private void startWebApp(Handler<AsyncResult<HttpServer>> next) {
        // Get all variables
        this.COLLECTION = config().getString("db_collection_name", "df_prod");

        // Check if Kafka Connect is enabled from configuration and other settings
        this.kafka_connect_enabled = config().getBoolean("kafka_connect_enable", Boolean.TRUE);
        this.kafka_connect_rest_host = config().getString("kafka_connect_rest_host", "localhost");
        this.kafka_connect_rest_port = config().getInteger("kafka_connect_rest_port", 8083);

        // Create a router object.
        Router router = Router.router(vertx);

        // Bind "/" to our hello message.
        router.route("/").handler(routingContext -> {
            HttpServerResponse response = routingContext.response();
            response.putHeader(ConstantApp.CONTENT_TYPE, ConstantApp.TEXT_HTML).end("<h1>Hello from DF producer service</h1>");
        });

        //router.route("/assets/*").handler(StaticHandler.create("assets"));
        router.get(ConstantApp.DF_PRODUCER_REST_URL).handler(this::getAll);
        router.route(ConstantApp.DF_PRODUCER_REST_URL_WILD).handler(BodyHandler.create());
        router.post(ConstantApp.DF_PRODUCER_REST_URL).handler(this::addOne); // Implemented Kafka Connect REST Forward
        router.get(ConstantApp.DF_PRODUCER_REST_URL_WITH_ID).handler(this::getOne);
        router.put(ConstantApp.DF_PRODUCER_REST_URL_WITH_ID).handler(this::updateOne); // Implemented Kafka Connect REST Forward
        router.delete(ConstantApp.DF_PRODUCER_REST_URL_WITH_ID).handler(this::deleteOne); // Implemented Kafka Connect REST Forward
        router.options(ConstantApp.DF_PRODUCER_REST_URL_WITH_ID).handler(this::corsHandle);
        router.options(ConstantApp.DF_PRODUCER_REST_URL).handler(this::corsHandle);


        // Create the HTTP server and pass the "accept" method to the request handler.
        vertx.createHttpServer().requestHandler(router::accept).listen(config().getInteger("http.port", 8080),
                next::handle
        );

        // Start REST API Client for Kafka Connect if needed
        if (this.kafka_connect_enabled) {
            final ObjectMapper objectMapper = new ObjectMapper();
            final List<HttpMessageConverter> httpMessageConverters = ImmutableList.of(
                    new FormHttpMessageConverter(),
                    new StringHttpMessageConverter(),
                    new JacksonJsonHttpMessageConverter(objectMapper)
            );

            final RestClientOptions restClientOptions = new RestClientOptions()
                    .setConnectTimeout(ConstantApp.REST_CLIENT_CONNECT_TIMEOUT)
                    .setGlobalRequestTimeout(ConstantApp.REST_CLIENT_GLOBAL_REQUEST_TIMEOUT)
                    .setDefaultHost(this.kafka_connect_rest_host)
                    .setDefaultPort(this.kafka_connect_rest_port)
                    .setKeepAlive(ConstantApp.REST_CLIENT_KEEP_LIVE)
                    .setMaxPoolSize(ConstantApp.REST_CLIENT_MAX_POOL_SIZE);

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
        final DFJobPOPJ dfJob = Json.decodeValue(routingContext.getBodyAsString(), DFJobPOPJ.class);
        LOG.info("received the body is:" + routingContext.getBodyAsString());
        LOG.info("repack for kafka is:" + dfJob.toKafkaConnectJson().toString());

        // Start Kafka Connect REST API Forward only if Kafka is enabled and Connector type is Kafka Connect
        if (this.kafka_connect_enabled &&
                dfJob.getConnectorType().equalsIgnoreCase(ConstantApp.DF_CONNECT_TYPE.KAFKA.name())) {

            // Create REST Client for Kafka Connect REST Forward
            final RestClientRequest postRestClientRequest = rc.post(ConstantApp.KAFKA_CONNECT_REST_URL, String.class,
                    portRestResponse -> {
                String rs = portRestResponse.getBody();
                JsonObject jo = new JsonObject(rs);
                LOG.debug("json object name: " + jo.getString("name"));
                LOG.debug("json object config: " + jo.getJsonObject("config"));
                LOG.debug("json object tasks: " + jo.getMap().get("tasks"));
                LOG.info("received response from Kafka server: " + portRestResponse.statusMessage());
                LOG.info("received response from Kafka server: " + portRestResponse.statusCode());

                // Once REST API forward is successful, add the record to the local repository
                mongo.insert(COLLECTION, dfJob.toJson(), r -> routingContext
                        .response().setStatusCode(ConstantApp.STATUS_CODE_OK_CREATED)
                        .putHeader(ConstantApp.CONTENT_TYPE, ConstantApp.APPLICATION_JSON_CHARSET_UTF_8)
                        .end(Json.encodePrettily(dfJob.setId(r.result()))));
            });

            postRestClientRequest.exceptionHandler(exception -> {
                routingContext.response().setStatusCode(ConstantApp.STATUS_CODE_CONFLICT)
                        .putHeader(ConstantApp.CONTENT_TYPE, ConstantApp.APPLICATION_JSON_CHARSET_UTF_8)
                        .end(errorMsg(10, "POST Request exception - " + exception.toString()));
            });

            postRestClientRequest.setContentType(MediaType.APPLICATION_JSON);
            postRestClientRequest.setAcceptHeader(Arrays.asList(MediaType.APPLICATION_JSON));
            postRestClientRequest.end(dfJob.toKafkaConnectJson().toString());

        } else {
            mongo.insert(COLLECTION, dfJob.toJson(), r -> routingContext
                    .response().setStatusCode(ConstantApp.STATUS_CODE_OK_CREATED)
                    .putHeader(ConstantApp.CONTENT_TYPE, ConstantApp.APPLICATION_JSON_CHARSET_UTF_8)
                    .end(Json.encodePrettily(dfJob.setId(r.result()))));
        }
    }

    private void getOne(RoutingContext routingContext) {
        final String id = routingContext.request().getParam("id");
        if (id == null) {
            routingContext.response().setStatusCode(ConstantApp.STATUS_CODE_BAD_REQUEST)
                          .end(errorMsg(20, "id is null in your request."));
        } else {
            mongo.findOne(COLLECTION, new JsonObject().put("_id", id), null, ar -> {
                if (ar.succeeded()) {
                    if (ar.result() == null) {
                        routingContext.response().setStatusCode(ConstantApp.STATUS_CODE_NOT_FOUND)
                                      .end(errorMsg(21, "id cannot find in repository."));
                        return;
                    }
                    DFJobPOPJ dfJob = new DFJobPOPJ(ar.result());
                    routingContext.response().setStatusCode(ConstantApp.STATUS_CODE_OK)
                            .putHeader(ConstantApp.CONTENT_TYPE, ConstantApp.APPLICATION_JSON_CHARSET_UTF_8)
                            .end(Json.encodePrettily(dfJob));
                } else {
                    routingContext.response().setStatusCode(ConstantApp.STATUS_CODE_NOT_FOUND)
                                  .end(errorMsg(22, "Search id in repository failed."));
                }
            });
        }
    }

    private void updateOne(RoutingContext routingContext) {
        final String id = routingContext.request().getParam("id");
        final DFJobPOPJ dfJob = Json.decodeValue(routingContext.getBodyAsString(), DFJobPOPJ.class);
        LOG.debug("received the body is from updateOne:" + routingContext.getBodyAsString());
        String connectorName = dfJob.getConnector();
        String connectorConfigString = dfJob.mapToJsonString(dfJob.getConnectorConfig());

        JsonObject json = dfJob.toJson();

        if (id == null || json == null) {
            routingContext.response().setStatusCode(ConstantApp.STATUS_CODE_BAD_REQUEST)
                    .end(errorMsg(30, "id is null in your request."));
        } else {

            // Implement connectConfig change detection to decide if we need REST API forwarding
            mongo.findOne(COLLECTION, new JsonObject().put("_id", id),
                    new JsonObject().put("connectorConfig", 1), res -> {
                if (res.succeeded()) {
                    String before_update_connectorConfigString = res.result().getString("connectorConfig");

                    // Detect changes in connectConfig
                    if (this.kafka_connect_enabled &&
                            dfJob.getConnectorType().equalsIgnoreCase(ConstantApp.DF_CONNECT_TYPE.KAFKA.name()) &&
                            connectorConfigString.compareTo(before_update_connectorConfigString) != 0) {

                        LOG.info("connectorConfig has change. Will forward to Kafka Connect.");

                        final RestClientRequest postRestClientRequest =
                                rc.put(
                                        ConstantApp.KAFKA_CONNECT_PLUGIN_CONFIG.
                                                replace("CONNECTOR_NAME_PLACEHOLDER", connectorName),
                                        String.class, portRestResponse -> {
                                    LOG.info("received response from Kafka server: " + portRestResponse.statusMessage());
                                    LOG.info("received response from Kafka server: " + portRestResponse.statusCode());
                                });

                        postRestClientRequest.exceptionHandler(exception -> {
                            routingContext.response().setStatusCode(ConstantApp.STATUS_CODE_CONFLICT)
                                    .putHeader(ConstantApp.CONTENT_TYPE, ConstantApp.APPLICATION_JSON_CHARSET_UTF_8)
                                    .end(errorMsg(31, "POST Request exception - " + exception.toString()));
                        });

                        postRestClientRequest.setContentType(MediaType.APPLICATION_JSON);
                        postRestClientRequest.setAcceptHeader(Arrays.asList(MediaType.APPLICATION_JSON));
                        postRestClientRequest.end(connectorConfigString);

                        mongo.updateCollection(COLLECTION, new JsonObject().put("_id", id), // Select a unique document
                                // The update syntax: {$set, the json object containing the fields to update}
                                new JsonObject().put("$set", json), v -> {
                                    if (v.failed()) {
                                        routingContext.response().setStatusCode(404)
                                                .end(errorMsg(32, "updateOne to repository is failed."));
                                    } else {
                                        routingContext.response()
                                                .putHeader(ConstantApp.CONTENT_TYPE,
                                                        ConstantApp.APPLICATION_JSON_CHARSET_UTF_8).end();
                                    }
                                }
                        );

                    } else { // Where there is no change detected
                        LOG.info("connectorConfig has NO change. Update in local repository only.");
                        mongo.updateCollection(COLLECTION, new JsonObject().put("_id", id), // Select a unique document
                                // The update syntax: {$set, the json object containing the fields to update}
                                new JsonObject().put("$set", json), v -> {
                                    if (v.failed()) {
                                        routingContext.response().setStatusCode(404)
                                                .end(errorMsg(33, "updateOne to repository is failed."));
                                    } else {
                                        routingContext.response()
                                                .putHeader(ConstantApp.CONTENT_TYPE,
                                                        ConstantApp.APPLICATION_JSON_CHARSET_UTF_8).end();
                                    }
                                }
                        );
                    }

                } else {
                    LOG.error("Mongo client response exception", res.cause());
                }
            });
        }
    }

    private void deleteOne(RoutingContext routingContext) {
        String id = routingContext.request().getParam("id");
        if (id == null) {
            routingContext.response().setStatusCode(ConstantApp.STATUS_CODE_BAD_REQUEST)
                    .end(errorMsg(30, "id is null in your request."));
        } else {
            mongo.removeDocument(COLLECTION, new JsonObject().put("_id", id),
                    ar -> routingContext.response().end(id + " is deleted from repository."));
        }
    }

    private void getAll(RoutingContext routingContext) {
        mongo.find(COLLECTION, new JsonObject(), results -> {
            List<JsonObject> objects = results.result();
            List<DFJobPOPJ> jobs = objects.stream().map(DFJobPOPJ::new).collect(Collectors.toList());
            routingContext.response()
                    .putHeader(ConstantApp.CONTENT_TYPE, ConstantApp.APPLICATION_JSON_CHARSET_UTF_8)
                    .end(Json.encodePrettily(jobs));
        });
    }

    /**
     * This is mainly to bypass security control for local API testing
     * @param routingContext
     */
    public void corsHandle(RoutingContext routingContext) {
        routingContext.response().putHeader("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE")
                .putHeader("Access-Control-Allow-Headers", "X-Requested-With, Content-Type")
                .putHeader("Access-Control-Max-Age", "60").end();
    }

    //TODO need initial method to import all available|paused|running connectors from kafka connect
    private void createSomeData(Handler<AsyncResult<Void>> next, Future<Void> fut) {
        HashMap<String, String> hm = new HashMap<String, String>();
        hm.put("path", "/tmp/a.json");
        DFJobPOPJ job1 = new DFJobPOPJ("Stream files job", "file-streamer", "register", hm, hm);
        DFJobPOPJ job2 = new DFJobPOPJ("Batch files job", "file-batcher", "register", hm, hm);

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
     *
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
