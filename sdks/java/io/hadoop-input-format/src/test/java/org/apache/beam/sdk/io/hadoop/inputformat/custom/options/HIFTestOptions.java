/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.beam.sdk.io.hadoop.inputformat.custom.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.testing.TestPipelineOptions;

/**
 * Properties needed when using HadoopInputFormatIO with the Beam SDK.
 */
public interface HIFTestOptions extends TestPipelineOptions {
  @Description("Server IP")
  @Default.String("serverIp")
  String getServerIp();
  void setServerIp(String serverIp);

  @Description("Server port")
  @Default.Integer(0)
  Integer getServerPort();
  void setServerPort(Integer serverPort);

  @Description("User name")
  @Default.String("userName")
  String getUserName();
  void setUserName(String userName);

  @Description("Password")
  @Default.String("password")
  String getPassword();
  void setPassword(String password);
}
