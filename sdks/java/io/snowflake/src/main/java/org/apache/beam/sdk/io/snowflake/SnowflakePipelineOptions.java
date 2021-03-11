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
package org.apache.beam.sdk.io.snowflake;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;

public interface SnowflakePipelineOptions extends PipelineOptions, StreamingOptions {
  String BASIC_CONNECTION_INFO_VALIDATION_GROUP = "BASIC_CONNECTION_INFO_GROUP";
  String AUTH_VALIDATION_GROUP = "AUTH_VALIDATION_GROUP";

  @Description(
      "Snowflake's JDBC-like url including account name and region without any parameters.")
  @Validation.Required(groups = BASIC_CONNECTION_INFO_VALIDATION_GROUP)
  String getUrl();

  void setUrl(String url);

  @Description("Server Name - full server name with account, zone and domain.")
  @Validation.Required(groups = BASIC_CONNECTION_INFO_VALIDATION_GROUP)
  ValueProvider<String> getServerName();

  void setServerName(ValueProvider<String> serverName);

  @Description("Username. Required for username/password and Private Key authentication.")
  @Validation.Required(groups = AUTH_VALIDATION_GROUP)
  ValueProvider<String> getUsername();

  void setUsername(ValueProvider<String> username);

  @Description("OAuth token. Required for OAuth authentication only.")
  @Validation.Required(groups = AUTH_VALIDATION_GROUP)
  String getOauthToken();

  void setOauthToken(String oauthToken);

  @Description("Password. Required for username/password authentication only.")
  @Default.String("")
  ValueProvider<String> getPassword();

  void setPassword(ValueProvider<String> password);

  @Description("Path to Private Key file. Required for Private Key authentication only.")
  @Default.String("")
  String getPrivateKeyPath();

  void setPrivateKeyPath(String privateKeyPath);

  @Description("Private key. Required for Private Key authentication only.")
  @Default.String("")
  ValueProvider<String> getRawPrivateKey();

  void setRawPrivateKey(ValueProvider<String> rawPrivateKey);

  @Description("Private Key's passphrase. Required for Private Key authentication only.")
  @Default.String("")
  ValueProvider<String> getPrivateKeyPassphrase();

  void setPrivateKeyPassphrase(ValueProvider<String> keyPassphrase);

  @Description("Warehouse to use. Optional.")
  @Default.String("")
  ValueProvider<String> getWarehouse();

  void setWarehouse(ValueProvider<String> warehouse);

  @Description("Database name to connect to. Optional.")
  @Default.String("")
  @Validation.Required
  ValueProvider<String> getDatabase();

  void setDatabase(ValueProvider<String> database);

  @Description("Schema to use. Optional.")
  @Default.String("")
  ValueProvider<String> getSchema();

  void setSchema(ValueProvider<String> schema);

  @Description("Table to use. Optional.")
  @Default.String("")
  ValueProvider<String> getTable();

  void setTable(ValueProvider<String> table);

  @Description("Query to use. Optional.")
  @Default.String("")
  ValueProvider<String> getQuery();

  void setQuery(ValueProvider<String> query);

  @Description("Role to use. Optional.")
  @Default.String("")
  ValueProvider<String> getRole();

  void setRole(ValueProvider<String> role);

  @Description("Authenticator to use. Optional.")
  @Default.String("")
  String getAuthenticator();

  void setAuthenticator(String authenticator);

  @Description("Port number. Optional.")
  @Default.String("")
  String getPortNumber();

  void setPortNumber(String portNumber);

  @Description("Login timeout. Optional.")
  @Default.String("")
  String getLoginTimeout();

  void setLoginTimeout(String loginTimeout);

  @Description("Temporary GCS bucket name.")
  @Validation.Required
  ValueProvider<String> getStagingBucketName();

  void setStagingBucketName(ValueProvider<String> stagingBucketName);

  @Description("Storage integration name")
  @Validation.Required
  ValueProvider<String> getStorageIntegrationName();

  void setStorageIntegrationName(ValueProvider<String> storageIntegrationName);

  @Description("SnowPipe name. Optional.")
  ValueProvider<String> getSnowPipe();

  void setSnowPipe(ValueProvider<String> snowPipe);
}
