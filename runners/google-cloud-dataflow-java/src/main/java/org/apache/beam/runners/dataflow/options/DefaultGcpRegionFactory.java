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
package org.apache.beam.runners.dataflow.options;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for a default value for Google Cloud region according to
 * https://cloud.google.com/compute/docs/gcloud-compute/#default-properties. If no other default can
 * be found, returns the empty string.
 */
@VisibleForTesting
public class DefaultGcpRegionFactory implements DefaultValueFactory<String> {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultGcpRegionFactory.class);

  @Override
  public String create(PipelineOptions options) {
    String environmentRegion = getRegionFromEnvironment();
    if (!Strings.isNullOrEmpty(environmentRegion)) {
      LOG.info("Using default GCP region {} from $CLOUDSDK_COMPUTE_REGION", environmentRegion);
      return environmentRegion;
    }
    try {
      String gcloudRegion = getRegionFromGcloudCli();
      if (!gcloudRegion.isEmpty()) {
        LOG.info("Using default GCP region {} from gcloud CLI", gcloudRegion);
        return gcloudRegion;
      }
    } catch (Exception e) {
      // Ignore.
      LOG.debug("Unable to get gcloud compute region", e);
    }
    return "";
  }

  @VisibleForTesting
  public static String getRegionFromEnvironment() {
    return System.getenv("CLOUDSDK_COMPUTE_REGION");
  }

  @VisibleForTesting
  static String getRegionFromGcloudCli() throws IOException, InterruptedException {
    ProcessBuilder pb =
        new ProcessBuilder(Arrays.asList("gcloud", "config", "get-value", "compute/region"));
    Process process = pb.start();
    try (BufferedReader reader =
            new BufferedReader(
                new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8));
        BufferedReader errorReader =
            new BufferedReader(
                new InputStreamReader(process.getErrorStream(), StandardCharsets.UTF_8))) {
      if (process.waitFor(2, TimeUnit.SECONDS) && process.exitValue() == 0) {
        return reader.lines().collect(Collectors.joining());
      } else {
        String stderr = errorReader.lines().collect(Collectors.joining("\n"));
        throw new RuntimeException(
            String.format(
                "gcloud exited with exit value %d. Stderr:%n%s", process.exitValue(), stderr));
      }
    }
  }
}
