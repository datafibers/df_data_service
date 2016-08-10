package com.datafibers.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.json.JsonObject;

import java.io.IOException;
import java.util.HashMap;

public class DFJob {

  private String id; //id as pk
  private String name; //name of the job
  private String connector; //name of the connector used
  private String status; //job status
  private HashMap<String, String> job_config; //configuration or metadata for the job
  private HashMap<String, String> connector_config; //configuration for the connector used

  public DFJob(String name, String connector, String status, HashMap<String, String> job_config, HashMap<String, String>  connector_config ) {
    this.name = name;
    this.connector = connector;
    this.status = status;
    this.id = "";
    this.job_config = job_config;
    this.connector_config = connector_config;
  }

  public DFJob(String name, String connector, String status) {
    this.name = name;
    this.connector = connector;
    this.status = status;
    this.id = "";
    this.job_config = null;
    this.connector_config = null;
  }

  public DFJob(JsonObject json) {
    this.name = json.getString("name");
    this.connector = json.getString("connector");
    this.status = json.getString("status");
    this.id = json.getString("_id");

    try {
      this.job_config = new ObjectMapper().readValue(json.getString("job_config"), new TypeReference<HashMap<String, String>>() {});
      this.connector_config = new ObjectMapper().readValue(json.getString("connector_config"), new TypeReference<HashMap<String, String>>() {});
    } catch (IOException ioe ) {
      ioe.printStackTrace();
    }

  }

  public DFJob() {
    this.id = "";
  }

  public DFJob(String id, String name, String connector, String status) {
    this.id = id;
    this.name = name;
    this.connector = connector;
    this.status = status;
  }

  public JsonObject toJson() {

    JsonObject json = new JsonObject()
        .put("name", name)
        .put("connector", connector)
        .put("status", status)
        .put("job_config", mapToJsonString(job_config))
        .put("connector_config", mapToJsonString(connector_config));

    if (id != null && !id.isEmpty()) {
      json.put("_id", id);
    }
    return json;
  }

  public String getName() {
    return name;
  }

  public String getConnector() {
    return connector;
  }

  public String getStatus() { return status; }

  public String getId() {
    return id;
  }

  public HashMap<String, String> getJobConfig() {
    return job_config;
  }

  public HashMap<String, String> getConnectorConfig() {
    return connector_config;
  }

  public DFJob setName(String name) {
    this.name = name;
    return this;
  }

  public DFJob setConnector(String connector) {
    this.connector = connector;
    return this;
  }

  public DFJob setStatus(String status) {
      this.status = status;
      return this;
  }

  public DFJob setId(String id) {
    this.id = id;
    return this;
  }

  public DFJob setConnectorConfig(HashMap<String, String>  connector_config) {
    this.connector_config = connector_config;
    return this;
  }

  public DFJob setJobConfig(HashMap<String, String> job_config) {
    this.job_config = job_config;
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