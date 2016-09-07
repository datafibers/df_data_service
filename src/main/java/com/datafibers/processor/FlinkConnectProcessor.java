package com.datafibers.processor;

import com.datafibers.flinknext.Kafka09JsonTableSink;
import org.apache.flink.api.java.table.StreamTableEnvironment;
import org.apache.flink.api.table.Table;
import org.apache.flink.api.table.TableEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.Kafka09JsonTableSource;
import org.apache.flink.streaming.connectors.kafka.KafkaJsonTableSource;
import org.apache.flink.streaming.connectors.kafka.partitioner.FixedPartitioner;

import java.util.Properties;

/**
 * Flink Connect Config Sample
 * {
 *     "group.id":"consumer3",
 *     "column.name.list":"symbol,name",
 *     "column.schema.list":"string,string",
 *     "topic.to.query":"finance",
 *     "topic.for.result":"stock",
 *     "trans.sql":"SELECT STREAM symbol, name FROM finance"
 * }
 */
public class FlinkConnectProcessor {

    public void submitFlinkSQL(String flinkHost, String zookeeperHost, String groupid, String colNameList, String colSchemaList,
                               String inputTopic, String outputTopic, String transSql) {
        String resultFile = "/home/vagrant/test.txt";
        //TODO number of parallel and Remote cluster support
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("localhost",9092).setParallelism(1);
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", flinkHost); //9092 for local port and 6123 for remote port
        // only required for Kafka 0.9
        properties.setProperty("zookeeper.connect", zookeeperHost);
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

            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
