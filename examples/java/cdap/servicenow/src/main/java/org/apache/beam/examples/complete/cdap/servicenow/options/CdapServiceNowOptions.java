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
package org.apache.beam.examples.complete.cdap.servicenow.options;

import org.apache.beam.examples.complete.cdap.options.BaseCdapOptions;
import org.apache.beam.examples.complete.cdap.servicenow.CdapServiceNowToTxt;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;

/**
 * The {@link CdapServiceNowOptions} interface provides the custom execution options passed by the
 * executor at the command-line for {@link CdapServiceNowToTxt} example.
 */
public interface CdapServiceNowOptions extends BaseCdapOptions {

  @Validation.Required
  @Description("The Client ID for ServiceNow Instance.")
  String getClientId();

  void setClientId(String clientId);

  @Validation.Required
  @Description("The Client Secret for ServiceNow Instance.")
  String getClientSecret();

  void setClientSecret(String clientSecret);

  @Validation.Required
  @Description("The user name for ServiceNow Instance.")
  String getUser();

  void setUser(String user);

  @Validation.Required
  @Description("The password for ServiceNow Instance.")
  String getPassword();

  void setPassword(String password);

  @Validation.Required
  @Description(
      "The REST API Endpoint for ServiceNow Instance. For example, https://instance.service-now.com")
  String getRestApiEndpoint();

  void setRestApiEndpoint(String restApiEndpoint);

  @Validation.Required
  @Description(
      "Mode of query. The mode can be one of two values: "
          + "`Reporting` - will allow user to choose application for which data will be fetched for all tables, "
          + "`Table` - will allow user to enter table name for which data will be fetched.")
  String getQueryMode();

  void setQueryMode(String queryMode);

  @Validation.Required
  @Description(
      "The name of the ServiceNow table from which data to be fetched. Note, the Table name value "
          + "will be ignored if the Mode is set to `Reporting`.")
  String getTableName();

  void setTableName(String tableName);

  @Validation.Required
  @Description(
      "The type of values to be returned."
          + "`Actual` -  will fetch the actual values from the ServiceNow tables"
          + "`Display` - will fetch the display values from the ServiceNow tables."
          + "Default is Actual.")
  String getValueType();

  void setValueType(String valueType);

  @Validation.Required
  @Description(
      "Path to output folder with filename prefix."
          + "It will write a set of .txt files with names like {prefix}-###.")
  String getOutputTxtFilePathPrefix();

  void setOutputTxtFilePathPrefix(String outputTxtFilePathPrefix);
}
