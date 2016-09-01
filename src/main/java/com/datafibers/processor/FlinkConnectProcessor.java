package com.datafibers.processor;

import org.apache.flink.api.java.table.StreamTableEnvironment;
import org.apache.flink.api.table.Table;
import org.apache.flink.api.table.TableEnvironment;
import org.apache.flink.api.table.sinks.CsvTableSink;
import org.apache.flink.api.table.sinks.TableSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.Kafka09JsonTableSource;
import org.apache.flink.streaming.connectors.kafka.KafkaJsonTableSource;

import java.nio.file.Files;
import java.nio.file.Paths;
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

    public void submitFlinkSQL(String groupid, String colNameList, String colSchemaList,
                               String inputTopic, String outputTopic, String transSql) {
        String resultFile = "/home/vagrant/test.txt";
        //TODO number of parallel and Remote cluster support
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("localhost",9092).setParallelism(1);
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092"); //9092 for local port and 6123 for remote port
        // only required for Kafka 0.9
        properties.setProperty("zookeeper.connect", "localhost:2181");
        properties.setProperty("group.id", groupid);

        String[] fieldNames =  new String[] {"symbol", "name"};
        Class<?>[] fieldTypes = new Class<?>[] {String.class, String.class };

        KafkaJsonTableSource kafkaTableSource = new Kafka09JsonTableSource(inputTopic, properties, fieldNames,
                fieldTypes);

        tableEnv.registerTableSource(inputTopic, kafkaTableSource);

        // run a SQL query on the Table and retrieve the result as a new Table
        Table result = tableEnv.sql(transSql);
        System.out.println(result.toString());

        try {
            // create a TableSink
            Files.deleteIfExists(Paths.get(resultFile));
            TableSink sink = new CsvTableSink(resultFile, "|");
            result.writeToSink(sink);

            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
