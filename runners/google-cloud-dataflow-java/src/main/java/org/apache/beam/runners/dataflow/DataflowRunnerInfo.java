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

  private static final String ENVIRONMENT_MAJOR_VERSION_KEY = "environment.major.version";
  private static final String BATCH_WORKER_HARNESS_CONTAINER_IMAGE_KEY = "worker.image.batch";
  private static final String STREAMING_WORKER_HARNESS_CONTAINER_IMAGE_KEY =
      "worker.image.streaming";

  /** Provides the environment's major version number. */
  public String getEnvironmentMajorVersion() {
    checkState(
        properties.containsKey(ENVIRONMENT_MAJOR_VERSION_KEY), "Unknown environment major version");
    return properties.getProperty(ENVIRONMENT_MAJOR_VERSION_KEY);
  }

  /** Provides the batch worker harness container image name. */
  public String getBatchWorkerHarnessContainerImage() {
    checkState(
        properties.containsKey(BATCH_WORKER_HARNESS_CONTAINER_IMAGE_KEY),
        "Unknown batch worker harness container image");
    return properties.getProperty(BATCH_WORKER_HARNESS_CONTAINER_IMAGE_KEY);
  }

  /** Provides the streaming worker harness container image name. */
  public String getStreamingWorkerHarnessContainerImage() {
    checkState(
        properties.containsKey(STREAMING_WORKER_HARNESS_CONTAINER_IMAGE_KEY),
        "Unknown streaming worker harness container image");
    return properties.getProperty(STREAMING_WORKER_HARNESS_CONTAINER_IMAGE_KEY);
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
