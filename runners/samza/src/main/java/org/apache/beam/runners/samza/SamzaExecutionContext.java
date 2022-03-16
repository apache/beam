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

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.UUID;
import org.apache.beam.runners.samza.metrics.SamzaMetricsContainer;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.vendor.grpc.v1p43p2.io.netty.util.internal.StringUtil;
import org.apache.commons.io.FileUtils;
import org.apache.samza.context.ApplicationContainerContext;
import org.apache.samza.context.ApplicationContainerContextFactory;
import org.apache.samza.context.ContainerContext;
import org.apache.samza.context.ExternalContext;
import org.apache.samza.context.JobContext;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Runtime context for the Samza runner. */
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class SamzaExecutionContext implements ApplicationContainerContext {
  private static final Logger LOG = LoggerFactory.getLogger(SamzaExecutionContext.class);

  private final SamzaPipelineOptions options;
  private SamzaMetricsContainer metricsContainer;

  public SamzaExecutionContext(SamzaPipelineOptions options) {
    this.options = options;
  }

  public SamzaPipelineOptions getPipelineOptions() {
    return options;
  }

  public SamzaMetricsContainer getMetricsContainer() {
    return this.metricsContainer;
  }

  void setMetricsContainer(SamzaMetricsContainer metricsContainer) {
    this.metricsContainer = metricsContainer;
  }

  private static void writeFsTokenToFile(String tokenPath, String token) throws IOException {
    final File file = new File(tokenPath);
    if (!file.createNewFile()) {
      LOG.info("Fs token file already exists. Will override.");
    }
    Files.setPosixFilePermissions(file.toPath(), PosixFilePermissions.fromString("rw-------"));
    FileUtils.writeStringToFile(file, token, StandardCharsets.UTF_8);
  }

  @Override
  public void start() {
    if (SamzaRunnerOverrideConfigs.isPortableMode(options)) {
      final String fsTokenPath = SamzaRunnerOverrideConfigs.getFsTokenPath(options);
      final String fsToken = UUID.randomUUID().toString(); // 128 bits
      final boolean useToken = !StringUtil.isNullOrEmpty(fsTokenPath);

      try {
        if (useToken) {
          LOG.info("Creating secure (auth-enabled) channels with fs token path: {}", fsTokenPath);
          writeFsTokenToFile(fsTokenPath, fsToken);
        } else {
          LOG.info("Fs token path not provided. Will create channels in insecure mode.");
        }
      } catch (Exception e) {
        throw new RuntimeException(
            "Running samza in Beam portable mode but failed to create job bundle factory", e);
      }
    }
  }

  @Override
  public void stop() {}

  /** The factory to return this {@link SamzaExecutionContext}. */
  public class Factory implements ApplicationContainerContextFactory<SamzaExecutionContext> {

    @Override
    public SamzaExecutionContext create(
        ExternalContext externalContext, JobContext jobContext, ContainerContext containerContext) {

      final MetricsRegistryMap metricsRegistry =
          (MetricsRegistryMap) containerContext.getContainerMetricsRegistry();
      SamzaMetricsContainer samzaMetricsContainer =
          new SamzaMetricsContainer(metricsRegistry, jobContext.getConfig());
      MetricsEnvironment.setGlobalContainer(
          samzaMetricsContainer.getContainer(SamzaMetricsContainer.GLOBAL_CONTAINER_STEP_NAME));
      SamzaExecutionContext.this.setMetricsContainer(samzaMetricsContainer);
      return SamzaExecutionContext.this;
    }
  }
}
