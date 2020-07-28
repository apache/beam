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
import org.apache.beam.sdk.options.Validation;

public interface SnowflakePipelineOptions extends PipelineOptions {
  String BASIC_CONNECTION_INFO_VALIDATION_GROUP = "BASIC_CONNECTION_INFO_GROUP";
  String AUTH_VALIDATION_GROUP = "AUTH_VALIDATION_GROUP";

  @Description(
      "Snowflake's JDBC-like url including account name and region without any parameters.")
  @Validation.Required(groups = BASIC_CONNECTION_INFO_VALIDATION_GROUP)
  String getUrl();

  void setUrl(String url);

  @Description("Server Name - full server name with account, zone and domain.")
  @Validation.Required(groups = BASIC_CONNECTION_INFO_VALIDATION_GROUP)
  String getServerName();

  void setServerName(String serverName);

  @Description("Username. Required for username/password and Private Key authentication.")
  @Validation.Required(groups = AUTH_VALIDATION_GROUP)
  String getUsername();

  void setUsername(String username);

  @Description("OAuth token. Required for OAuth authentication only.")
  @Validation.Required(groups = AUTH_VALIDATION_GROUP)
  String getOauthToken();

  void setOauthToken(String oauthToken);

  @Description("Password. Required for username/password authentication only.")
  @Default.String("")
  String getPassword();

  void setPassword(String password);

  @Description("Path to Private Key file. Required for Private Key authentication only.")
  @Default.String("")
  String getPrivateKeyPath();

  void setPrivateKeyPath(String privateKeyPath);

  @Description("Private Key's passphrase. Required for Private Key authentication only.")
  @Default.String("")
  String getPrivateKeyPassphrase();

  void setPrivateKeyPassphrase(String keyPassphrase);

  @Description("Warehouse to use. Optional.")
  @Default.String("")
  String getWarehouse();

  void setWarehouse(String warehouse);

  @Description("Database name to connect to. Optional.")
  @Default.String("")
  String getDatabase();

  void setDatabase(String database);

  @Description("Schema to use. Optional.")
  @Default.String("")
  String getSchema();

  void setSchema(String schema);

  @Description("Role to use. Optional.")
  @Default.String("")
  String getRole();

  void setRole(String role);

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
  String getStagingBucketName();

  void setStagingBucketName(String stagingBucketName);

  @Description("Storage integration name")
  String getStorageIntegrationName();

  void setStorageIntegrationName(String storageIntegrationName);
}
