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
package org.apache.beam.sdk.extensions.openlineage;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.openlineage.client.OpenLineageConfig;
import io.openlineage.client.job.JobConfig;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * OpenLineage configuration for Beam pipelines, extending the generic {@link OpenLineageConfig}
 * from openlineage-java with Beam-specific entries — the same pattern as {@code
 * SparkOpenLineageConfig} and {@code FlinkOpenLineageConfig} in the official integrations.
 *
 * <p>Deserialized from {@code openlineage.yml} or {@code OPENLINEAGE__} environment variables and
 * merged with values from {@link OpenLineagePipelineOptions}.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class BeamOpenLineageConfig extends OpenLineageConfig<BeamOpenLineageConfig> {

  /** Default interval between RUNNING events, matching the Flink integration. */
  static final int DEFAULT_TRACKING_INTERVAL_SECONDS = 60;

  @JsonProperty("trackingIntervalInSeconds")
  private @Nullable Integer trackingIntervalInSeconds;

  public @Nullable Integer getTrackingIntervalInSeconds() {
    return trackingIntervalInSeconds;
  }

  public void setTrackingIntervalInSeconds(@Nullable Integer trackingIntervalInSeconds) {
    this.trackingIntervalInSeconds = trackingIntervalInSeconds;
  }

  @Override
  public BeamOpenLineageConfig mergeWithNonNull(BeamOpenLineageConfig other) {
    BeamOpenLineageConfig merged = new BeamOpenLineageConfig();
    merged.setTransportConfig(mergePropertyWith(getTransportConfig(), other.getTransportConfig()));
    merged.setFacetsConfig(mergePropertyWith(getFacetsConfig(), other.getFacetsConfig()));
    merged.setDatasetConfig(mergePropertyWith(getDatasetConfig(), other.getDatasetConfig()));
    merged.setCircuitBreaker(mergePropertyWith(getCircuitBreaker(), other.getCircuitBreaker()));
    merged.setMetricsConfig(mergePropertyWith(getMetricsConfig(), other.getMetricsConfig()));
    merged.setRunConfig(mergePropertyWith(getRunConfig(), other.getRunConfig()));
    JobConfig mergedJobConfig = mergeJobConfig(getJobConfig(), other.getJobConfig());
    if (mergedJobConfig != null) {
      merged.setJobConfig(mergedJobConfig);
    }
    merged.setTrackingIntervalInSeconds(
        mergePropertyWith(trackingIntervalInSeconds, other.getTrackingIntervalInSeconds()));
    return merged;
  }

  /**
   * Merges job configs field by field. {@link JobConfig#mergeWithNonNull} assumes a non-null owners
   * map on both sides and throws otherwise, so it cannot be used directly on configs assembled from
   * pipeline options.
   */
  private static @Nullable JobConfig mergeJobConfig(
      @Nullable JobConfig base, @Nullable JobConfig overrides) {
    if (base == null) {
      return overrides;
    }
    if (overrides == null) {
      return base;
    }
    JobConfig merged = new JobConfig();
    merged.setNamespace(
        overrides.getNamespace() != null ? overrides.getNamespace() : base.getNamespace());
    merged.setName(overrides.getName() != null ? overrides.getName() : base.getName());
    merged.setOwners(overrides.getOwners() != null ? overrides.getOwners() : base.getOwners());
    merged.setTags(overrides.getTags() != null ? overrides.getTags() : base.getTags());
    return merged;
  }
}
