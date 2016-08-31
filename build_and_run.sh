#!/usr/bin/env bash
mvn package -DskipTests; java -jar target/df-processing-service-1.0-SNAPSHOT-jar-with-dependencies.jar s-all
