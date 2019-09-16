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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import org.apache.beam.sdk.util.ReleaseInfo;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Populates versioning and other information for {@link DataflowRunner}. */
public final class DataflowRunnerInfo extends ReleaseInfo {
  private static final Logger LOG = LoggerFactory.getLogger(DataflowRunnerInfo.class);

  private static final String APACHE_BEAM_DISTRIBUTION_PROPERTIES_PATH =
      "/org/apache/beam/runners/dataflow/dataflow.properties";
  private static final String FNAPI_ENVIRONMENT_MAJOR_VERSION_KEY =
      "fnapi.environment.major.version";
  private static final String LEGACY_ENVIRONMENT_MAJOR_VERSION_KEY =
      "legacy.environment.major.version";
  private static final String CONTAINER_VERSION_KEY = "container.version";

  private static class LazyInit {
    private static final DataflowRunnerInfo INSTANCE;

    static {
      Properties properties;
      try {
        properties = load(APACHE_BEAM_DISTRIBUTION_PROPERTIES_PATH);
        if (properties == null) {
          // Print a warning if we can not load either the Dataflow distribution properties
          // or the
          LOG.warn("Dataflow runner properties resource not found.");
          properties = new Properties();
        }
      } catch (IOException e) {
        LOG.warn("Error loading Dataflow runner properties resource: ", e);
        properties = new Properties();
      }

      // Inherit the name and version from the Apache Beam distribution if this isn't
      // the Dataflow distribution.
      if (!properties.containsKey("name")) {
        properties.setProperty("name", ReleaseInfo.getReleaseInfo().getName());
      }
      if (!properties.containsKey("version")) {
        properties.setProperty("version", ReleaseInfo.getReleaseInfo().getVersion());
      }
      copyFromSystemProperties("java.vendor", properties);
      copyFromSystemProperties("java.version", properties);
      copyFromSystemProperties("os.arch", properties);
      copyFromSystemProperties("os.name", properties);
      copyFromSystemProperties("os.version", properties);
      INSTANCE = new DataflowRunnerInfo(ImmutableMap.copyOf((Map) properties));
    }
  }

  /** Returns an instance of {@link DataflowRunnerInfo}. */
  public static DataflowRunnerInfo getDataflowRunnerInfo() {
    return LazyInit.INSTANCE;
  }

  /** Provides the legacy environment's major version number. */
  public String getLegacyEnvironmentMajorVersion() {
    checkState(
        properties.containsKey(LEGACY_ENVIRONMENT_MAJOR_VERSION_KEY),
        "Unknown legacy environment major version");
    return properties.get(LEGACY_ENVIRONMENT_MAJOR_VERSION_KEY);
  }

  /** Provides the FnAPI environment's major version number. */
  public String getFnApiEnvironmentMajorVersion() {
    checkState(
        properties.containsKey(FNAPI_ENVIRONMENT_MAJOR_VERSION_KEY),
        "Unknown FnAPI environment major version");
    return properties.get(FNAPI_ENVIRONMENT_MAJOR_VERSION_KEY);
  }

  /** Provides the container version that will be used for constructing harness image paths. */
  public String getContainerVersion() {
    checkState(properties.containsKey(CONTAINER_VERSION_KEY), "Unknown container version");
    return properties.get(CONTAINER_VERSION_KEY);
  }

  @Override
  public Map<String, String> getProperties() {
    return ImmutableMap.copyOf((Map) properties);
  }

  private final Map<String, String> properties;

  private DataflowRunnerInfo(Map<String, String> properties) {
    this.properties = properties;
  }

  private static Properties load(String path) throws IOException {
    Properties properties = new Properties();
    try (InputStream in = DataflowRunnerInfo.class.getResourceAsStream(path)) {
      if (in == null) {
        return null;
      }
      properties.load(in);
    }
    return properties;
  }

  private static void copyFromSystemProperties(String property, Properties properties) {
    String value = System.getProperty(property);
    if (value != null) {
      properties.setProperty(property, value);
    }
  }
}
