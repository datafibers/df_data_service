package com.datafibers.service;

import com.datafibers.model.DFJobPOPJ;
import com.datafibers.util.ConstantApp;
import com.datafibers.util.Runner;
import com.fasterxml.jackson.core.type.TypeReference;
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
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
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
    private RestClient rc;

    private static Boolean kafka_connect_enabled;
    private static String kafka_connect_rest_host;
    private static Integer kafka_connect_rest_port;
    private static Boolean kafka_connect_import_start;

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
        LOG.info("Start DF Producer Service...");

        // Get all variables
        this.COLLECTION = config().getString("db_collection_name", "df_prod");

        // Check if Kafka Connect is enabled from configuration and other settings
        this.kafka_connect_enabled = config().getBoolean("kafka_connect_enable", Boolean.TRUE);
        this.kafka_connect_rest_host = config().getString("kafka_connect_rest_host", "localhost");
        this.kafka_connect_rest_port = config().getInteger("kafka_connect_rest_port", 8083);
        this.kafka_connect_import_start = config().getBoolean("kafka_connect_import_start", Boolean.TRUE);

        // Create a Mongo client
        mongo = MongoClient.createShared(vertx, config());

        // Import from remote server. It is blocking at this point.
        if (this.kafka_connect_enabled && this.kafka_connect_import_start) {
           importAllFromKafkaConnect();
        }

        // Start Core application
        startWebApp((http) -> completeStartup(http, fut));

        // Add sample data
        //createSomeData((nothing) -> startWebApp((http) -> completeStartup(http, fut)), fut);

        long timerID = vertx.setPeriodic(ConstantApp.REGULAR_REFRESH_STATUS_TO_REPO, id -> {
            updateKafkaConnectorStatus();
        });

    }

    private void startWebApp(Handler<AsyncResult<HttpServer>> next) {

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
        // Get request as object
        final DFJobPOPJ dfJob = Json.decodeValue(routingContext.getBodyAsString(), DFJobPOPJ.class);
        // Set initial status for the job
        dfJob.setStatus(ConstantApp.DF_STATUS.UNASSIGNED.name());
        LOG.info("received the body is:" + routingContext.getBodyAsString());
        LOG.info("repack for kafka is:" + dfJob.toKafkaConnectJson().toString());

        // Start Kafka Connect REST API Forward only if Kafka is enabled and Connector type is Kafka Connect
        if (this.kafka_connect_enabled && dfJob.getConnectorType().contains("KAFKA")) {
            KafkaConnectProcessor.forwardPOSTAsAddOne(routingContext, rc, mongo, COLLECTION, dfJob);
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
                    if (this.kafka_connect_enabled && dfJob.getConnectorType().contains("KAFKA") &&
                            connectorConfigString.compareTo(before_update_connectorConfigString) != 0) {
                       KafkaConnectProcessor.forwardPUTAsUpdateOne(routingContext, rc, mongo, COLLECTION, dfJob);
                    } else { // Where there is no change detected
                        LOG.info("connectorConfig has NO change. Update in local repository only.");
                        mongo.updateCollection(COLLECTION, new JsonObject().put("_id", id), // Select a unique document
                                // The update syntax: {$set, the json object containing the fields to update}
                                new JsonObject().put("$set", dfJob.toJson()), v -> {
                                    if (v.failed()) {
                                        routingContext.response().setStatusCode(ConstantApp.STATUS_CODE_NOT_FOUND)
                                                .end(errorMsg(33, "updateOne to repository is failed."));
                                    } else {
                                        routingContext.response().putHeader(ConstantApp.CONTENT_TYPE,
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
        final DFJobPOPJ dfJob = Json.decodeValue(routingContext.getBodyAsString(), DFJobPOPJ.class);
        if (id == null) {
            routingContext.response().setStatusCode(ConstantApp.STATUS_CODE_BAD_REQUEST)
                    .end(errorMsg(40, "id is null in your request."));
        } else {
            if (this.kafka_connect_enabled && dfJob.getConnectorType().contains("KAFKA")) {
                KafkaConnectProcessor.forwardDELETEAsDeleteOne(routingContext, rc, mongo, COLLECTION, dfJob);
            } else {
                mongo.removeDocument(COLLECTION, new JsonObject().put("_id", id),
                        ar -> routingContext.response().end(id + " is deleted from repository."));
            }

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

    /**
     * Get initial method to import all available|paused|running connectors from kafka connect
     */
    private void importAllFromKafkaConnect() {
        LOG.info("Starting initial import data from Kafka Connect REST Server.");
        String restURI = "http://" + this.kafka_connect_rest_host+ ":" + this.kafka_connect_rest_port +
                ConstantApp.KAFKA_CONNECT_REST_URL;
        try {
            HttpResponse<String> res = Unirest.get(restURI)
                    .header("accept", "application/json")
                    .asString();
            String resStr = res.getBody();
            for (String connectName: resStr.substring(2,resStr.length()-2).split("\",\"")) {
                // Get connector config
                HttpResponse<JsonNode> resConnector = Unirest.get(restURI + "/" + connectName + "/config")
                        .header("accept", "application/json").asJson();
                JsonNode resConfig = resConnector.getBody();
                String resConnectTypeTmp = resConfig.getObject().getString("connector.class");
                String resConnectType;
                if (resConnectTypeTmp.toUpperCase().contains("SOURCE")) {
                    resConnectType = ConstantApp.DF_CONNECT_TYPE.KAFKA_SOURCE.name();
                } else if (resConnectTypeTmp.toUpperCase().contains("SINK")) {
                    resConnectType = ConstantApp.DF_CONNECT_TYPE.KAFKA_SINK.name();
                } else {
                    resConnectType = ConstantApp.DF_CONNECT_TYPE.NONE.name();
                }
                // Get task status
                HttpResponse<JsonNode> resConnectorStatus = Unirest.get(restURI + "/" + connectName + "/status")
                        .header("accept", "application/json").asJson();
                String resStatus = resConnectorStatus.getBody().getObject().getJSONObject("connector").getString("state");

                mongo.count(COLLECTION, new JsonObject().put("connector", connectName), count -> {
                    if (count.succeeded()) {
                        if (count.result() == 0) {
                            // No jobs found, then insert json data
                            DFJobPOPJ insertJob = new DFJobPOPJ (
                                    new JsonObject().put("name", "imported " + connectName)
                                            .put("taskId", "0")
                                            .put("connector", connectName)
                                            .put("connectorType", resConnectType)
                                            .put("status", resStatus)
                                            .put("jobConfig", new JsonObject().put("comments", "This is imported from Kafka Connect.").toString())
                                            .put("connectorConfig", resConfig.getObject().toString())
                            );
                            mongo.insert(COLLECTION, insertJob.toJson(), ar -> {
                                if (ar.failed()) {
                                    LOG.error("IMPORT Status - failed", ar);
                                } else {
                                  LOG.debug("IMPORT Status - successfully", ar);
                                }
                            });
                        } else { // Update the connectConfig portion from Kafka import
                            mongo.findOne(COLLECTION, new JsonObject().put("connector", connectName), null, findidRes -> {
                                if (findidRes.succeeded()) {
                                    DFJobPOPJ updateJob = new DFJobPOPJ(findidRes.result());
                                    try {
                                        updateJob.setStatus(resStatus).setConnectorConfig(
                                                new ObjectMapper().readValue(resConfig.getObject().toString(),
                                                        new TypeReference<HashMap<String, String>>(){}));

                                    } catch (IOException ioe) {
                                        LOG.error("IMPORT Status - Read Connector Config as Map failed", ioe.getCause());
                                    }

                                    mongo.updateCollection(COLLECTION, new JsonObject().put("_id", updateJob.getId()),
                                            // The update syntax: {$set, the json object containing the fields to update}
                                            new JsonObject().put("$set", updateJob.toJson()), v -> {
                                                if (v.failed()) {
                                                    LOG.error("IMPORT Status - Update Connector Config as Map failed",
                                                            v.cause());
                                                } else {
                                                    LOG.debug("IMPORT Status - Update Connector Config as Map Successfully");
                                                }
                                            }
                                    );

                                } else {
                                    LOG.error("IMPORT-UPDATE failed", findidRes.cause());
                                }
                            });
                        }
                    } else {
                        // report the error
                        LOG.error("count failed",count.cause());
                    }
                });
            }
        } catch (UnirestException ue) {
            LOG.error("Importing from Kafka Connect Server exception", ue);
        }
        LOG.info("Completed initial import data from Kafka Connect REST Server.");
    }

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
     * Keep refreshing the active Kafka connector status against remote Kafka REST Server
     */
    private void updateKafkaConnectorStatus() {
        //TODO Loop existing KAFKA connectors in repository and fetch their latest status from Kafka Server
        LOG.info("Starting refreshing connector status from Kafka Connect REST Server.");
        List<String> list = new ArrayList<String>();
        list.add(ConstantApp.DF_CONNECT_TYPE.KAFKA_SINK.name());
        list.add(ConstantApp.DF_CONNECT_TYPE.KAFKA_SOURCE.name());
        String restURI = "http://" + this.kafka_connect_rest_host+ ":" + this.kafka_connect_rest_port +
                ConstantApp.KAFKA_CONNECT_REST_URL;
        // Container reused for keeping refreshing list of active Kafka jobs
        ArrayList<String> activeKafkaConnector = new ArrayList<String>();
        mongo.find(COLLECTION, new JsonObject().put("connectorType", new JsonObject().put("$in", list)), result -> {
                    if (result.succeeded()) {
                        for (JsonObject json : result.result()) {
                            LOG.debug(json.encodePrettily());
                            String connectName = json.getString("connector");
                            String statusRepo = json.getString("status");
                            // Get task status
                            try {
                                HttpResponse<JsonNode> resConnectorStatus =
                                        Unirest.get(restURI + "/" + connectName + "/status")
                                                .header("accept", "application/json").asJson();
                                String resStatus = resConnectorStatus.getBody().getObject()
                                        .getJSONObject("connector").getString("state");

                                // Do change detection on status
                                if (statusRepo.compareToIgnoreCase(resStatus) != 0) { //status changes
                                    DFJobPOPJ updateJob = new DFJobPOPJ(json);
                                    updateJob.setStatus(resStatus);

                                    mongo.updateCollection(COLLECTION, new JsonObject().put("_id", updateJob.getId()),
                                            // The update syntax: {$set, the json object containing the fields to update}
                                            new JsonObject().put("$set", updateJob.toJson()), v -> {
                                                if (v.failed()) {
                                                    LOG.error("Update Status - Update status failed", v.cause());
                                                } else {
                                                    LOG.debug("Update Status - Update status Successfully");
                                                }
                                            }
                                    );
                                } else {
                                    LOG.debug("Refreshing status - No changes detected on status.");
                                }
                            } catch (UnirestException ue) {
                                LOG.error("Refreshing status REST client exception", ue.getCause());
                            }
                        }
                    } else {
                        LOG.error("Refreshing status Mongo client find active connectors exception", result.cause());
                    }
        });
        LOG.info("Completed refreshing connector status from Kafka Connect REST Server.");
    }

    /**
     * Print error message in better JSON format
     *
     * @param error_code
     * @param msg
     * @return
     */
    public static String errorMsg(int error_code, String msg) {
        return Json.encodePrettily(new JsonObject()
                .put("code", String.format("%6d", error_code))
                .put("message", msg));
    }
}
