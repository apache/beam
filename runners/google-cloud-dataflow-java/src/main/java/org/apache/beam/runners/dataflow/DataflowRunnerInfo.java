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
package org.apache.beam.runners.dataflow;

import static com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Populates versioning and other information for {@link DataflowRunner}.
 */
public final class DataflowRunnerInfo {
  private static final Logger LOG = LoggerFactory.getLogger(DataflowRunnerInfo.class);

  private static final String PROPERTIES_PATH =
      "/org/apache/beam/runners/dataflow/dataflow.properties";

  private static class LazyInit {
    private static final DataflowRunnerInfo INSTANCE = new DataflowRunnerInfo(PROPERTIES_PATH);
  }

  /**
   * Returns an instance of {@link DataflowRunnerInfo}.
   */
  public static DataflowRunnerInfo getDataflowRunnerInfo() {
    return LazyInit.INSTANCE;
  }

  private Properties properties;

  private static final String FNAPI_ENVIRONMENT_MAJOR_VERSION_KEY =
      "fnapi.environment.major.version";
  private static final String LEGACY_ENVIRONMENT_MAJOR_VERSION_KEY =
      "legacy.environment.major.version";
  private static final String CONTAINER_VERSION_KEY = "container.version";

  /** Provides the legacy environment's major version number. */
  public String getLegacyEnvironmentMajorVersion() {
    checkState(
        properties.containsKey(LEGACY_ENVIRONMENT_MAJOR_VERSION_KEY),
        "Unknown legacy environment major version");
    return properties.getProperty(LEGACY_ENVIRONMENT_MAJOR_VERSION_KEY);
  }

  /** Provides the FnAPI environment's major version number. */
  public String getFnApiEnvironmentMajorVersion() {
    checkState(
        properties.containsKey(FNAPI_ENVIRONMENT_MAJOR_VERSION_KEY),
        "Unknown FnAPI environment major version");
    return properties.getProperty(FNAPI_ENVIRONMENT_MAJOR_VERSION_KEY);
  }

  /** Provides the container version that will be used for constructing harness image paths. */
  public String getContainerVersion() {
    checkState(
        properties.containsKey(CONTAINER_VERSION_KEY),
        "Unknown container version");
    return properties.getProperty(CONTAINER_VERSION_KEY);
  }

  private DataflowRunnerInfo(String resourcePath) {
    properties = new Properties();

    try (InputStream in = DataflowRunnerInfo.class.getResourceAsStream(PROPERTIES_PATH)) {
      if (in == null) {
        LOG.warn("Dataflow runner properties resource not found: {}", resourcePath);
        return;
      }

      properties.load(in);
    } catch (IOException e) {
      LOG.warn("Error loading Dataflow runner properties resource: ", e);
    }
  }
}
