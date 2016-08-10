package com.datafibers.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.json.JsonObject;

import java.io.IOException;
import java.util.HashMap;

public class DFJob {

  private String id; //id as pk, which is also used as job id
  private String taskId; //Identify each task in a job
  private String name; //name of the job
  private String connector; //name of the connector used
  private String connectorType; //Indetify if it is kafka connector or others, such as [KAFKA_CONNECT, HDFS_CONNECT, FILESYS_CONNECT, HIVE_CONNECT]
  private String status; //job status
  private HashMap<String, String> jobConfig; //configuration or metadata for the job
  private HashMap<String, String> connectorConfig; //configuration for the connector used

  public DFJob(String task_id, String name, String connector, String connector_type, String status,
               HashMap<String, String> job_config, HashMap<String, String>  connector_config ) {
    this.taskId = task_id;
    this.name = name;
    this.connector = connector;
    this.connectorType = connector_type;
    this.status = status;
    this.id = "";
    this.jobConfig = job_config;
    this.connectorConfig = connector_config;
  }

  public DFJob(String name, String connector, String status,
               HashMap<String, String> job_config, HashMap<String, String>  connector_config ) {
    this.taskId = "0";
    this.name = name;
    this.connector = connector;
    this.connectorType = "KAFKA_CONNECT";
    this.status = status;
    this.id = "";
    this.jobConfig = job_config;
    this.connectorConfig = connector_config;
  }

  public DFJob(String name, String connector, String status) {
    this.name = name;
    this.connector = connector;
    this.connectorType = "KAFKA_CONNECT";
    this.status = status;
    this.id = "";
    this.taskId = "0";
    this.jobConfig = null;
    this.connectorConfig = null;
  }

  public DFJob(JsonObject json) {
    this.taskId = json.getString("taskId");
    this.name = json.getString("name");
    this.connector = json.getString("connector");
    this.connectorType = json.getString("connectortType");
    this.status = json.getString("status");
    this.id = json.getString("_id");

    try {
      this.jobConfig = new ObjectMapper().readValue(json.getString("jobConfig"), new TypeReference<HashMap<String, String>>() {});
      this.connectorConfig = new ObjectMapper().readValue(json.getString("connectorConfig"), new TypeReference<HashMap<String, String>>() {});
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
        .put("taskId", taskId)
        .put("connector", connector)
        .put("connectortType", connectorType)
        .put("status", status)
        .put("jobConfig", mapToJsonString(jobConfig))
        .put("connectorConfig", mapToJsonString(connectorConfig));

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

  public String getConnectorType() {
    return connectorType;
  }

  public String getStatus() { return status; }

  public String getTaskId() {
    return taskId;
  }

  public String getId() {
    return id;
  }

  public HashMap<String, String> getJobConfig() {
    return jobConfig;
  }

  public HashMap<String, String> getConnectorConfig() {
    return connectorConfig;
  }

  public DFJob setName(String name) {
    this.name = name;
    return this;
  }

  public DFJob setConnector(String connector) {
    this.connector = connector;
    return this;
  }

  public DFJob setConnectorType(String connector_type) {
    this.connectorType = connector_type;
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

  public DFJob setTaskId(String task_id) {
    this.taskId = task_id;
    return this;
  }

  public DFJob setConnectorConfig(HashMap<String, String>  connector_config) {
    this.connectorConfig = connector_config;
    return this;
  }

  public DFJob setJobConfig(HashMap<String, String> job_config) {
    this.jobConfig = job_config;
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