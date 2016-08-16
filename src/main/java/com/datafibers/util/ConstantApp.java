package com.datafibers.util;

public final class ConstantApp {

    // Vertx REST Client settings
    public static final int REST_CLIENT_CONNECT_TIMEOUT = 1000;
    public static final int REST_CLIENT_GLOBAL_REQUEST_TIMEOUT = 5000;
    public static final Boolean REST_CLIENT_KEEP_LIVE = true;
    public static final int REST_CLIENT_MAX_POOL_SIZE = 500;

    // DF Producer REST endpoint URLs
    public static final String DF_PRODUCER_REST_URL = "/api/df/ps";
    public static final String DF_PRODUCER_REST_URL_WILD = "/api/df/ps*";
    public static final String DF_PRODUCER_REST_URL_WITH_ID = DF_PRODUCER_REST_URL + "/:id";

    // KAFKA CONNECT endpoint URLs
    public static final String KAFKA_CONNECT_REST_URL = "/connectors";
    public static final String KAFKA_CONNECT_PLUGIN_REST_URL = "/connector-plugins";
    public static String KAFKA_CONNECT_PLUGIN_CONFIG = "/connectors/CONNECTOR_NAME_PLACEHOLDER/config";

    // HTTP req/res constants
    public static final String CONTENT_TYPE = "content-type";
    public static final String APPLICATION_JSON_CHARSET_UTF_8 = "application/json; charset=utf-8";
    public static final String TEXT_HTML = "text/html";

    // HTTP status codes
    public static final int STATUS_CODE_OK = 200;
    public static final int STATUS_CODE_OK_CREATED = 201;
    public static final int STATUS_CODE_OK_NO_CONTENT = 204;
    public static final int STATUS_CODE_BAD_REQUEST = 400;
    public static final int STATUS_CODE_NOT_FOUND = 404;
    public static final int STATUS_CODE_CONFLICT = 409;

    public enum DF_CONNECT_TYPE {
        KAFKA, //Kafka Connector and plugins
        EVENTBUS, //The plugin connect to Vertx Event Bus
        HDFS, //The plugin connect to HDFS
        NONE
    }

}
