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
package org.apache.beam.runners.samza;

import java.util.Map;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.samza.config.ConfigFactory;
import org.apache.samza.config.factories.PropertiesConfigFactory;

/** Options which can be used to configure a Samza PipelineRunner. */
public interface SamzaPipelineOptions extends PipelineOptions {

  @Description(
      "The config file for Samza. It is *optional*. "
          + "Without a config file, Samza uses a default config for local execution.")
  String getConfigFilePath();

  void setConfigFilePath(String filePath);

  @Description(
      "The factory to read config file from config file path. ")
  @Default.Class(PropertiesConfigFactory.class)
  Class<? extends ConfigFactory> getConfigFactory();

  void setConfigFactory(Class<? extends ConfigFactory> configFactory);

  @Description(
      "The config override to set programmatically. It will be applied on "
          + "top of config file if it exits, otherwise used directly as the config.")
  Map<String, String> getConfigOverride();

  void setConfigOverride(Map<String, String> configs);

  @Description("The interval to check for watermarks in milliseconds.")
  @Default.Long(1000)
  long getWatermarkInterval();

  void setWatermarkInterval(long interval);

  @Description("The maximum number of messages to buffer for a given system.")
  @Default.Integer(5000)
  int getSystemBufferSize();

  void setSystemBufferSize(int consumerBufferSize);

  @Description("The maximum parallelism allowed for any data source.")
  @Default.Integer(1)
  int getMaxSourceParallelism();

  void setMaxSourceParallelism(int maxSourceParallelism);

  @Description("The batch get size limit for the state store.")
  @Default.Integer(10000)
  int getStoreBatchGetSize();

  void setStoreBatchGetSize(int storeBatchGetSize);
}
