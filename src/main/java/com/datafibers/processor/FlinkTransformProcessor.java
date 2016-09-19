package com.datafibers.processor;

import com.datafibers.flinknext.Kafka09JsonTableSink;
import com.datafibers.model.DFJobPOPJ;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.table.StreamTableEnvironment;
import org.apache.flink.api.table.Table;
import org.apache.flink.api.table.TableEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.Kafka09JsonTableSource;
import org.apache.flink.streaming.connectors.kafka.KafkaJsonTableSource;
import org.apache.flink.streaming.connectors.kafka.partitioner.FixedPartitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;
import java.util.Properties;

/* This is sample transform config
    {
        "group.id":"consumer3",
        "column.name.list":"symbol,name",
        "column.schema.list":"string,string",
        "topic.for.query":"finance",
        "topic.for.result":"stock",
        "trans.sql":"SELECT STREAM symbol, name FROM finance"
    }
*/

public class FlinkTransformProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkTransformProcessor.class);

    /**
     * This method first submit a flink job against Kafka streaming in other thread. Then, it captures job_id from console.
     * After that of 8000 milliseconds, it restores the system.out and put newly captured job_id to job config property
     * flink.submit.job.id. At the end, update the record at repo - mongo as response.
     *
     * @param routingContext This is the contect from REST API
     * @param mongoCOLLECTION This is mongodb collection name
     * @param mongoClient This is the client used to insert final data to repository - mongodb
     * @param transSql This is SQL string defined for data transformation
     * @param outputTopic This is the kafka topic to keep transformed data
     * @param inputTopic This is the kafka topic to keep source data
     * @param colSchemaList This is the list of data type for the select columns
     * @param colNameList This is the list of name for the select columns
     * @param groupid This is the lkafka consumer id
     * @param kafkaHostPort This is the hostname and port for kafka
     * @param zookeeperHostPort This is the hostname and port for zookeeper
     * @param flinkEnv This is the flink runtime enter point
     * @param maxRunTime This is the max run time for thread of submitting flink job
     * @param vertx This is the vertx enter point
     * @param dfJob This is the job config object
     */
    public static void submitFlinkSQL(DFJobPOPJ dfJob, Vertx vertx, Integer maxRunTime,
                                      StreamExecutionEnvironment flinkEnv, String zookeeperHostPort,
                                      String kafkaHostPort, String groupid, String colNameList,
                                      String colSchemaList, String inputTopic, String outputTopic,
                                      String transSql, MongoClient mongoClient, String mongoCOLLECTION) {

        String uuid = dfJob.hashCode() + "_" +
                dfJob.getName() + "_" + dfJob.getConnector() + "_" + dfJob.getTaskId();
        // Create a stream to hold the output
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);
        // IMPORTANT: Save the old System.out!
        PrintStream old = System.out;
        // Submit Flink through client in vertx worker thread and terminate it once the job is launched.
        WorkerExecutor exec_flink = vertx.createSharedWorkerExecutor(dfJob.getName() + dfJob.hashCode(),5, maxRunTime);
        // Submit Flink job in separate thread
        exec_flink.executeBlocking(future -> {
            // Tell Java to use your special stream
            System.setOut(ps);

            StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(flinkEnv);
            Properties properties = new Properties();
            properties.setProperty("bootstrap.servers", kafkaHostPort); //9092 for kafka server
            // only required for Kafka 0.9
            properties.setProperty("zookeeper.connect", zookeeperHostPort);
            properties.setProperty("group.id", groupid);

            String[] fieldNames = colNameList.split(",");

            String[] fields = colSchemaList.split(",");
            Class<?>[] fieldTypes = new Class[fields.length];
            String temp;
            for (int i = 0; i < fields.length; i++) {
                try {
                    switch (fields[i].trim().toLowerCase()) {
                        case "string":
                            temp = "java.lang.String";
                            break;
                        case "date":
                            temp = "java.util.Date";
                            break;
                        case "integer":
                            temp = "java.lang.Integer";
                            break;
                        case "long":
                            temp = "java.lang.Long";
                            break;
                        default: temp = fields[i].trim();
                    }
                    fieldTypes[i] = Class.forName(temp);
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
            }

            KafkaJsonTableSource kafkaTableSource =
                    new Kafka09JsonTableSource(inputTopic, properties, fieldNames, fieldTypes);

            tableEnv.registerTableSource(inputTopic, kafkaTableSource);

            // run a SQL query on the Table and retrieve the result as a new Table
            Table result = tableEnv.sql(transSql);

            // create a TableSink
            try {
                FixedPartitioner partition =  new FixedPartitioner();
                Kafka09JsonTableSink sink = new Kafka09JsonTableSink (outputTopic, properties, partition);
                result.writeToSink(sink);
                JobExecutionResult jres = flinkEnv.execute("DF_FLINK_TRANS_" + uuid);
                future.complete(jres);

            } catch (Exception e) {
                LOG.error("Flink Submit Exception", e.getCause());
            }

        }, res -> {
            LOG.debug("@@@@@@@BOLOCKING CODE IS TERMINATE?FINISHED");

        });

        long timerID = vertx.setTimer(8000, id -> {
            // Put things back
            System.out.flush();
            System.setOut(old);
            // Show what happened
            String jobID = StringUtils.substringBetween(baos.toString(),
                    "Submitting job with JobID:", "Waiting for job completion.").trim().replace(".", "");
            // Close previous flink exec thread
            exec_flink.close();
            System.out.println("@@FLINK JOB_ID Submitted - " + jobID);
            dfJob.setFlinkIDToJobConfig(jobID);

            mongoClient.updateCollection(mongoCOLLECTION, new JsonObject().put("_id", dfJob.getId()),
                    new JsonObject().put("$set", dfJob.toJson()), v -> {
                        if (v.failed()) {
                            LOG.error("update Flink JOb_ID Failed.", v.cause());
                        }
                    }
            );
        });

    }
}
