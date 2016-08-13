package com.datafibers.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.json.JsonObject;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

/**
 * Meta Objects for Kafka Connector Configurations Reponse fro REST API
 */

public class KafkaConnectConfig {

  private String name; //name of the connector
  private JsonObject config; //configuration or metadata for the job
  private Integer error_code;
  private String message;
  private List<JsonObject> tasks;

  public KafkaConnectConfig(String name, JsonObject config) {
    this.name = name;
    this.config = config;
  }

  public KafkaConnectConfig(JsonObject json) {
    this.name = json.getString("name");

    try {

      String jobConfig = json.getString("config");
      if (config == null) {
        this.config = null;
      } else {
        this.config = new ObjectMapper().readValue(jobConfig, new TypeReference<HashMap<String, String>>() {});
      }

    } catch (IOException ioe ) {
      ioe.printStackTrace();
    }

  }

  public JsonObject toJson() {

    JsonObject json = new JsonObject()
        .put("name", name)
        .put("config", config)
        .put("error_code", error_code)
        .put("message", message)
        .put("tasks", tasks);

    return json;
  }

  public String getName() {
    return name;
  }

  public JsonObject getConfig() {
    return config;
  }

  public KafkaConnectConfig setName(String name) {
    this.name = name;
    return this;
  }

  public KafkaConnectConfig setConfig(JsonObject config) {
    this.config = config;
    return this;
  }

  public String mapToJsonString(HashMap<String, String> hm) {
    ObjectMapper mapperObj = new ObjectMapper();
    try {
      return mapperObj.writeValueAsString(hm);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }
}