package com.datafibers.service;

import com.datafibers.util.ConstantApp;
import com.datafibers.util.Runner;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.asyncsql.AsyncSQLClient;
import io.vertx.ext.asyncsql.PostgreSQLClient;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;

/**
 * Created by will on 2016-08-29.
 */
public class DFTransformer extends AbstractVerticle {

    public static void main(String[] args) {
        Runner.runExample(DFTransformer.class);
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
        JsonObject postgreSQLClientConfig = new JsonObject().put("host", "localhost")
                .put("port", 5432).put("username", "vagrant").put("password", "vagrant")
                .put("database", "pipeline");
        AsyncSQLClient client = PostgreSQLClient.createShared(vertx, postgreSQLClientConfig);
        client.getConnection(res -> {
            if (res.succeeded()) {

                SQLConnection connection = res.result();
                connection.query("SELECT * FROM message_count", res0 -> {
                    if (res.succeeded()) {
                        // Get the result set
                        ResultSet resultSet = res0.result();
                    } else {
                        // Failed!
                    }
                });

                // Got a connection

            } else {
                // Failed to get connection - deal with it
            }
        });

    }

}
