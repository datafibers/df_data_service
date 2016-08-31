package com.datafibers.service;

import com.datafibers.model.DFJobPOPJ;
import com.datafibers.processor.KafkaConnectProcessor;
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
import org.apache.flink.api.java.table.StreamTableEnvironment;
import org.apache.flink.api.table.Table;
import org.apache.flink.api.table.TableEnvironment;
import org.apache.flink.api.table.sinks.CsvTableSink;
import org.apache.flink.api.table.sinks.TableSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.Kafka09JsonTableSource;
import org.apache.flink.streaming.connectors.kafka.KafkaJsonTableSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * DF Producer is used to route producer service to kafka connect rest or lunch locally
 * The overall status is maintained in the its local database - mongo
 */

public class DFTransformer extends AbstractVerticle {

    public static String COLLECTION;
    private MongoClient mongo;
    private RestClient rc;

    private static Boolean transform_engine_flink_enabled;
    private static String flink_server_host;
    private static Integer flink_server_port;
    private static String flink_server_host_and_port;
    private static String zookeeper_server_host;
    private static Integer zookeeper_server_port;
    private static String zookeeper_server_host_and_port;

    private static final Logger LOG = LoggerFactory.getLogger(DFTransformer.class);

    /**
     * This main() is used for debug purpose only
     *
     * @param args
     */
    public static void main(String[] args) {
        Runner.runExample(DFTransformer.class);
    }

    @Override
    public void start(Future<Void> fut) {
        LOG.info("Start DF Transform Service...");
        this.testFlinkSQL();

        // Get all variables
        this.COLLECTION = config().getString("db.collection.name.df.transformer", "df_tran");

        // Check if Kafka Connect is enabled from configuration and other settings
        this.transform_engine_flink_enabled = config().getBoolean("transform.engine.flink.enable", Boolean.TRUE);
        this.flink_server_host = config().getString("flink.servers.host", "localhost");
        this.flink_server_port = config().getInteger("flink.servers.port", 9092);
        this.flink_server_host_and_port = this.flink_server_host + ":" + this.flink_server_port.toString();
        this.zookeeper_server_host = config().getString("zookeeprt.server.host", "localhost");
        this.zookeeper_server_port = config().getInteger("zookeeprt.server.port", 8083);
        this.zookeeper_server_host_and_port = this.zookeeper_server_host + ":" + this.zookeeper_server_port.toString();

        // Create a Mongo client
        mongo = MongoClient.createShared(vertx, config());

        // Start Core application
        startWebApp((http) -> completeStartup(http, fut));

        //long timerID = vertx.setPeriodic(ConstantApp.REGULAR_REFRESH_STATUS_TO_REPO, id -> {
        //    updateKafkaConnectorStatus();
        //});

    }

    private void startWebApp(Handler<AsyncResult<HttpServer>> next) {

        Router router = Router.router(vertx);

        router.route("/").handler(routingContext -> {
            HttpServerResponse response = routingContext.response();
            response.putHeader(ConstantApp.CONTENT_TYPE, ConstantApp.TEXT_HTML).end("<h1>Hello from DF transformer service</h1>");
        });

        router.get(ConstantApp.DF_TRANSFOMER_REST_URL).handler(this::getAll);
        router.route(ConstantApp.DF_TRANSFOMER_REST_URL_WILD).handler(BodyHandler.create());
        router.post(ConstantApp.DF_TRANSFOMER_REST_URL).handler(this::addOne); // Implemented Kafka Connect REST Forward
        router.get(ConstantApp.DF_TRANSFOMER_REST_URL_WITH_ID).handler(this::getOne);
        router.put(ConstantApp.DF_TRANSFOMER_REST_URL_WITH_ID).handler(this::updateOne); // Implemented Kafka Connect REST Forward
        router.delete(ConstantApp.DF_TRANSFOMER_REST_URL_WITH_ID).handler(this::deleteOne); // Implemented Kafka Connect REST Forward
        router.options(ConstantApp.DF_TRANSFOMER_REST_URL_WITH_ID).handler(this::corsHandle);
        router.options(ConstantApp.DF_TRANSFOMER_REST_URL).handler(this::corsHandle);


        // Create the HTTP server and pass the "accept" method to the request handler.
        vertx.createHttpServer().requestHandler(router::accept).listen(config().getInteger("http.port.df.transformer", 8081),
                next::handle
        );

        // Start REST API Client for Flink if needed
        if (this.transform_engine_flink_enabled) {
            final ObjectMapper objectMapper = new ObjectMapper();
            final List<HttpMessageConverter> httpMessageConverters = ImmutableList.of(
                    new FormHttpMessageConverter(),
                    new StringHttpMessageConverter(),
                    new JacksonJsonHttpMessageConverter(objectMapper)
            );

            final RestClientOptions restClientOptions = new RestClientOptions()
                    .setConnectTimeout(ConstantApp.REST_CLIENT_CONNECT_TIMEOUT)
                    .setGlobalRequestTimeout(ConstantApp.REST_CLIENT_GLOBAL_REQUEST_TIMEOUT)
                    .setDefaultHost(this.flink_server_host)
                    .setDefaultPort(this.flink_server_port)
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

        // Start Flink job Forward only if it is enabled and Connector type is FLINK
        if (this.transform_engine_flink_enabled && dfJob.getConnectorType().contains("FLINK")) {
            //submit flink sql
            //KafkaConnectProcessor.forwardPOSTAsAddOne(routingContext, rc, mongo, COLLECTION, dfJob);
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
                                .end(errorMsg(121, "id cannot find in repository."));
                        return;
                    }
                    DFJobPOPJ dfJob = new DFJobPOPJ(ar.result());
                    routingContext.response().setStatusCode(ConstantApp.STATUS_CODE_OK)
                            .putHeader(ConstantApp.CONTENT_TYPE, ConstantApp.APPLICATION_JSON_CHARSET_UTF_8)
                            .end(Json.encodePrettily(dfJob));
                } else {
                    routingContext.response().setStatusCode(ConstantApp.STATUS_CODE_NOT_FOUND)
                            .end(errorMsg(122, "Search id in repository failed."));
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
                    .end(errorMsg(130, "id is null in your request."));
        } else {
            // Implement connectConfig change detection to decide if we need REST API forwarding
            mongo.findOne(COLLECTION, new JsonObject().put("_id", id),
                    new JsonObject().put("connectorConfig", 1), res -> {
                        if (res.succeeded()) {
                            String before_update_connectorConfigString = res.result().getString("connectorConfig");
                            // Detect changes in connectConfig
                            if (this.transform_engine_flink_enabled && dfJob.getConnectorType().contains("FLINK") &&
                                    connectorConfigString.compareTo(before_update_connectorConfigString) != 0) {
                                //here update is to delete exiting job and submit a new one
                                //KafkaConnectProcessor.forwardPUTAsUpdateOne(routingContext, rc, mongo, COLLECTION, dfJob);

                            } else { // Where there is no change detected
                                LOG.info("connectorConfig has NO change. Update in local repository only.");
                                mongo.updateCollection(COLLECTION, new JsonObject().put("_id", id), // Select a unique document
                                        // The update syntax: {$set, the json object containing the fields to update}
                                        new JsonObject().put("$set", dfJob.toJson()), v -> {
                                            if (v.failed()) {
                                                routingContext.response().setStatusCode(ConstantApp.STATUS_CODE_NOT_FOUND)
                                                        .end(errorMsg(133, "updateOne to repository is failed."));
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
        if (id == null) {
            routingContext.response().setStatusCode(ConstantApp.STATUS_CODE_BAD_REQUEST)
                    .end(errorMsg(140, "id is null in your request."));
        } else {
            mongo.findOne(COLLECTION, new JsonObject().put("_id", id), null, ar -> {
                if (ar.succeeded()) {
                    if (ar.result() == null) {
                        routingContext.response().setStatusCode(ConstantApp.STATUS_CODE_NOT_FOUND)
                                .end(errorMsg(141, "id cannot find in repository."));
                        return;
                    }
                    DFJobPOPJ dfJob = new DFJobPOPJ(ar.result());
                    System.out.println("DELETE OBJ" + dfJob.toJson());
                    if (this.transform_engine_flink_enabled && dfJob.getConnectorType().contains("FLINK")) {

                        //here delete or kill flink job
                        //KafkaConnectProcessor.forwardDELETEAsDeleteOne(routingContext, rc, mongo, COLLECTION, dfJob);
                    } else {
                        mongo.removeDocument(COLLECTION, new JsonObject().put("_id", id),
                                remove -> routingContext.response().end(id + " is deleted from repository."));
                    }
                }
            });
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
     * Keep refreshing the active Flink connector status against remote Flink REST Server
     */
    private void updateFlinkConnectorStatus() {
        //TODO Loop existing KAFKA connectors in repository and fetch their latest status from Kafka Server
        LOG.info("Starting refreshing connector status from Flink Server.");
        List<String> list = new ArrayList<String>();
        list.add(ConstantApp.DF_CONNECT_TYPE.KAFKA_SINK.name());
        list.add(ConstantApp.DF_CONNECT_TYPE.KAFKA_SOURCE.name());
        // TODO can use unblocking REST Client
        String restURI = "http://" + this.flink_server_host+ ":" + this.flink_server_port +
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
                        String resStatus;
                        HttpResponse<JsonNode> resConnectorStatus =
                                Unirest.get(restURI + "/" + connectName + "/status")
                                        .header("accept", "application/json").asJson();
                        if(resConnectorStatus.getStatus() == 404) {
                            // Not find - Mark status as LOST
                            resStatus = ConstantApp.DF_STATUS.LOST.name();
                        } else {
                            resStatus = resConnectorStatus.getBody().getObject()
                                    .getJSONObject("connector").getString("state");
                        }

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

    public void testFlinkSQL() {
        String kafkaTopic = "receipts";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        // only required for Kafka 0.9
        properties.setProperty("zookeeper.connect", "localhost:2181");
        properties.setProperty("group.id", "consumer3");

        String[] fieldNames =  new String[] { "name", "url"};
        Class<?>[] fieldTypes = new Class<?>[] { String.class, String.class };

        KafkaJsonTableSource kafkaTableSource = new Kafka09JsonTableSource(
                kafkaTopic,
                properties,
                fieldNames,
                fieldTypes);

        tableEnv.registerTableSource("Orders", kafkaTableSource);

        // run a SQL query on the Table and retrieve the result as a new Table
        Table result = tableEnv.sql("SELECT STREAM DISTINCT name, url FROM Orders");
        System.out.println(result.toString());

        //DataSet<WC> re = tableEnv.toDataStream(result,WC.class);


        // create a TableSink
        TableSink sink = new CsvTableSink("/home/vagrant/test.txt", "|");

        // write the result Table to the TableSink
        result.writeToSink(sink);

        // execute the program
        try {
            //re.print();
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
