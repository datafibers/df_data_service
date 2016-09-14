# DF Data Processor Service

This project has two components defined to deal with stream ETL (Extract, Transform, and Load).
* **Connects** is to leverage Kafka Connect REST API on Confluent v.3.0.0 to landing or publishing data in or out of Apache Kafka.
* **Transforms** is to leverage streaming processing engine, such as Apache Flink, for data transformation.

## Building

You build the project using:

```
mvn clean package
```

## Testing

The application is tested using [vertx-unit](http://vertx.io/docs/vertx-unit/java/).

## Packaging

The application is packaged as a _fat jar_, using the 
[Maven Shade Plugin](https://maven.apache.org/plugins/maven-shade-plugin/).

## Running

Once packaged, just launch the _fat jar_ as follows:

```
java -jar df-processing-service-1.0-SNAPSHOT-fat.jar
```
**<MORE_START_OPTION>** values are as follows
* **"cluster"**: Deploy as cluster mode.


## Todo
- [x] Add UI from [NG-Admin](https://github.com/marmelab/ng-admin)
- [ ] Add to generic function to do connector validation before creation
- [ ] Add submit job actions
- [x] Fetch all installed connectors/plugins in regularly frequency
- [x] Need to report connector or job status
- [x] Need an initial method to import all available|paused|running connectors from kafka connect
- [ ] Add Flink Table API engine
- [ ] Add Spark Structure Streaming
