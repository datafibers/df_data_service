# DF Processing Service

This project has two [Vertx](http://www.vertx.io) defined to deal with stream ETL (Extract, Transform, and Load).
* **DF Producer** is to leverage Kafka Connect REST API on Confluent v.3.0.0 to landing or publishing data in or out of Apache Kafka.
* **DF Transformer** is to leverage straming processing engine, such as Apache Flink, for data transformation.

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
java -jar df-processing-service-1.0-SNAPSHOT-fat.jar <START_OPTION>
```
**<START_OPTION>** values are as follows
* **"s-all"**: Deploy both Producer & Transform vertical as single mode.
* **"s-c"**: Deploy Producer vertical as single mode.
* **"s-t"**: Deploy Transform vertical as single mode.
* **"c-all**": Deploy both Producer & Transform vertical as cluster mode.
* **"c-c"**: Deploy only Producer vertical as cluster mode.
* **"c-t"**: Deploy only Transform vertical as cluster mode.


## Todo
- [x] Add UI from [NG-Admin](https://github.com/marmelab/ng-admin)
- [ ] Add to generic function to do connector validation before creation
- [x] Fetch all installed connectors/plugins in regularly frequency
- [x] Need to report connector|job status
- [x] Need an initial method to import all available|paused|running connectors from kafka connect
- [ ] Add Flink Table API engine
