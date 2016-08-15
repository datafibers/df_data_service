# DF Producer Service

This project is to test proxy of Kafka Connect REST API

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

Then, open a browser to http://localhost:8080.

## Connector Status Life Cycle
1. Submit (Submitted): Keep it in df repository only
1. Validate (Validated): Do validation in the remote server, such as Kafka
1. Run (Running): Fetch the job and post it to the remote server, such as Kafka connect
1. Stop (Stopped): Stop the connector
1. Resume (Running from Resumed): Resume the job
1. Completed: Once job finished running. This requires frequent checking

## Todo
- [x] Add UI from [NG-Admin](https://github.com/marmelab/ng-admin)
- [ ] Add to generic function to do connector validation before creation
- [ ] Fetch all installed connectors/plugins
- [ ] Need to report connector|job status
- [ ] Need an initial method to import all available|paused|running connectors from kafka connect
