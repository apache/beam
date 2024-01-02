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
package org.apache.beam.sdk.expansion.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.File;
import java.io.IOException;
import org.apache.beam.sdk.expansion.service.JavaClassLookupTransformProvider.AllowList;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

/** Options used to configure the {@link ExpansionService}. */
public interface ExpansionServiceOptions extends PipelineOptions {

  @Description("Allow list for Java class based transform expansion")
  @Default.InstanceFactory(JavaClassLookupAllowListFactory.class)
  AllowList getJavaClassLookupAllowlist();

  void setJavaClassLookupAllowlist(AllowList file);

  @Description(
      "Allow list file for Java class based transform expansion, or '*' to allow anything.")
  String getJavaClassLookupAllowlistFile();

  void setJavaClassLookupAllowlistFile(String file);

  @Description("Whether to also start a loopback worker as part of this service.")
  boolean getAlsoStartLoopbackWorker();

  void setAlsoStartLoopbackWorker(boolean value);

  @Description("Expansion service configuration file.")
  String getExpansionServiceConfigFile();

  void setExpansionServiceConfigFile(String configFile);

  @Description("Expansion service configuration.")
  @Default.InstanceFactory(ExpansionServiceConfigFactory.class)
  ExpansionServiceConfig getExpansionServiceConfig();

  void setExpansionServiceConfig(ExpansionServiceConfig configFile);

  /**
   * Loads the allow list from {@link #getJavaClassLookupAllowlistFile}, defaulting to an empty
   * {@link JavaClassLookupTransformProvider.AllowList}.
   */
  class JavaClassLookupAllowListFactory implements DefaultValueFactory<AllowList> {

    @Override
    public AllowList create(PipelineOptions options) {
      String allowListFile =
          options.as(ExpansionServiceOptions.class).getJavaClassLookupAllowlistFile();
      if (allowListFile != null) {
        if (allowListFile.equals("*")) {
          return AllowList.everything();
        }
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        File allowListFileObj = new File(allowListFile);
        if (!allowListFileObj.exists()) {
          throw new IllegalArgumentException(
              "Allow list file " + allowListFile + " does not exist");
        }
        try {
          return mapper.readValue(allowListFileObj, AllowList.class);
        } catch (IOException e) {
          throw new IllegalArgumentException(
              "Could not load the provided allowlist file " + allowListFile, e);
        }
      }

      // By default produces an empty allow-list.
      return AllowList.nothing();
    }
  }

  /** Loads the ExpansionService config. */
  class ExpansionServiceConfigFactory implements DefaultValueFactory<ExpansionServiceConfig> {

    @Override
    public ExpansionServiceConfig create(PipelineOptions options) {
      String configFile = options.as(ExpansionServiceOptions.class).getExpansionServiceConfigFile();
      if (configFile != null) {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        File configFileObj = new File(configFile);
        if (!configFileObj.exists()) {
          throw new IllegalArgumentException("Config file " + configFile + " does not exist");
        }
        try {
          return mapper.readValue(configFileObj, ExpansionServiceConfig.class);
        } catch (IOException e) {
          throw new IllegalArgumentException(
              "Could not load the provided config file " + configFile, e);
        }
      }

      // By default produces null.
      return ExpansionServiceConfig.empty();
    }
  }
}
