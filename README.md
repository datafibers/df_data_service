# DF Producer Service

This project is to test proxy of Kafka Connect REST API on Confluent v.3.0.0

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
java -jar target/df-producer-1.0-SNAPSHOT-fat.jar
```

## Todo
- [x] Add UI from [NG-Admin](https://github.com/marmelab/ng-admin)
- [ ] Add to generic function to do connector validation before creation
- [x] Fetch all installed connectors/plugins in regularly frequency
- [x] Need to report connector|job status
- [x] Need an initial method to import all available|paused|running connectors from kafka connect
