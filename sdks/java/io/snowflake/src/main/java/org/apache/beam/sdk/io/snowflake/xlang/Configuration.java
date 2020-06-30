/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.snowflake.xlang;

/** Parameters abstract class to expose the transforms to an external SDK. */
public abstract class Configuration {
  private String serverName;
  private String username;
  private String password;
  private String privateKeyPath;
  private String privateKeyPassphrase;
  private String oAuthToken;
  private String database;
  private String schema;
  private String table;
  private String query;
  private String stagingBucketName;
  private String storageIntegrationName;

  public String getServerName() {
    return serverName;
  }

  public void setServerName(String serverName) {
    this.serverName = serverName;
  }

  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public String getPrivateKeyPath() {
    return privateKeyPath;
  }

  public void setPrivateKeyPath(String privateKeyPath) {
    this.privateKeyPath = privateKeyPath;
  }

  public String getPrivateKeyPassphrase() {
    return privateKeyPassphrase;
  }

  public void setPrivateKeyPassphrase(String privateKeyPassphrase) {
    this.privateKeyPassphrase = privateKeyPassphrase;
  }

  public String getOAuthToken() {
    return oAuthToken;
  }

  public void setOAuthToken(String oAuthToken) {
    this.oAuthToken = oAuthToken;
  }

  public String getDatabase() {
    return database;
  }

  public void setDatabase(String database) {
    this.database = database;
  }

  public String getSchema() {
    return schema;
  }

  public void setSchema(String schema) {
    this.schema = schema;
  }

  public String getTable() {
    return table;
  }

  public void setTable(String table) {
    this.table = table;
  }

  public String getQuery() {
    return query;
  }

  public void setQuery(String query) {
    this.query = query;
  }

  public String getStagingBucketName() {
    return stagingBucketName;
  }

  public void setStagingBucketName(String stagingBucketName) {
    this.stagingBucketName = stagingBucketName;
  }

  public String getStorageIntegrationName() {
    return storageIntegrationName;
  }

  public void setStorageIntegrationName(String storageIntegrationName) {
    this.storageIntegrationName = storageIntegrationName;
  }
}
