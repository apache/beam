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
package org.apache.beam.sdk.io.influxdb;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.testing.TestPipelineOptions;

/** InfluxDBIO options. */
public interface InfluxDBPipelineOptions extends TestPipelineOptions {
  @Description("InfluxDB host (host name/ip address)")
  @Default.String("http://localhost:8086")
  String getInfluxDBURL();

  void setInfluxDBURL(String value);

  @Description("Username for InfluxDB")
  @Default.String("superadmin")
  String getInfluxDBUserName();

  void setInfluxDBUserName(String value);

  @Description("Password for InfluxDB")
  @Default.String("supersecretpassword")
  String getInfluxDBPassword();

  void setInfluxDBPassword(String value);

  @Description("InfluxDB database name")
  @Default.String("db3")
  String getDatabaseName();

  void setDatabaseName(String value);
}
