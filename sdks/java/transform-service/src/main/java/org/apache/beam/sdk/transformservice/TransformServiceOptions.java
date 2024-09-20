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
package org.apache.beam.sdk.transformservice;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public interface TransformServiceOptions extends PipelineOptions {

  @Description("Port for running the transform service.")
  int getPort();

  void setPort(int port);

  @Description("Transform service configuration file.")
  String getTransformServiceConfigFile();

  void setTransformServiceConfigFile(String configFile);

  @Description("Transform service configuration.")
  @Default.InstanceFactory(TransformServiceConfigFactory.class)
  TransformServiceConfig getTransformServiceConfig();

  void setTransformServiceConfig(TransformServiceConfig configFile);

  /** Loads the TransformService config. */
  class TransformServiceConfigFactory implements DefaultValueFactory<TransformServiceConfig> {

    @Override
    public TransformServiceConfig create(PipelineOptions options) {
      String configFile = options.as(TransformServiceOptions.class).getTransformServiceConfigFile();
      if (configFile != null) {
        File configFileObj = new File(configFile);
        if (!configFileObj.exists()) {
          throw new IllegalArgumentException("Config file " + configFile + " does not exist");
        }
        try (InputStream stream = new FileInputStream(configFileObj)) {
          return TransformServiceConfig.parseFromYamlStream(stream);
        } catch (FileNotFoundException e) {
          throw new RuntimeException(
              "Could not parse the provided Transform Service config file " + configFile, e);
        } catch (IOException e) {
          throw new RuntimeException(
              "Could not parse the provided Transform Service config file " + configFile, e);
        }
      }

      return TransformServiceConfig.empty();
    }
  }
}
