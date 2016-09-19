#!/usr/bin/env bash
mvn package -DskipTests; java -jar target/df-data-processor-1.0-SNAPSHOT-fat.jar
