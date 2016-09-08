package com.datafibers.processor;

import com.datafibers.flinknext.Kafka09JsonTableSink;
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

import java.util.Properties;

/**
 * Flink Connect Config Sample
 * {
 *     "group.id":"consumer3",
 *     "column.name.list":"symbol,name",
 *     "column.schema.list":"string,string",
 *     "topic.for.query":"finance",
 *     "topic.for.result":"stock",
 *     "trans.sql":"SELECT STREAM symbol, name FROM finance"
 * }
 */
public class FlinkConnectProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkConnectProcessor.class);

    public static void submitFlinkSQL(StreamExecutionEnvironment flinkEnv, String zookeeperHostPort,
                                      String kafkaHostPort, String groupid, String colNameList,
                                      String colSchemaList, String inputTopic, String outputTopic,
                                      String transSql) {

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

        try {
            // create a TableSink
            FixedPartitioner partition =  new FixedPartitioner();
            Kafka09JsonTableSink sink = new Kafka09JsonTableSink (outputTopic, properties, partition);
            result.writeToSink(sink);

            JobExecutionResult jres = flinkEnv.execute();
            LOG.debug("Flink Job ID is - " + jres.getJobID());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
