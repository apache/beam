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

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Pipeline options controlling OpenLineage emission.
 *
 * <p>These options mirror the engine-native configuration of the official OpenLineage integrations
 * (the {@code spark.openlineage.*} Spark conf keys and the {@code openlineage.*} Flink conf keys).
 * Transport, facet, dataset and circuit-breaker configuration is NOT exposed here; it is read the
 * standard OpenLineage way — from an {@code openlineage.yml} file (located via the {@code
 * OPENLINEAGE_CONFIG} environment variable, the working directory, or {@code ~/.openlineage/}) or
 * from {@code OPENLINEAGE__}-prefixed environment variables. Values set via these pipeline options
 * take precedence over the file, which takes precedence over environment variables, matching the
 * Spark integration's precedence rules.
 *
 * <p>Because these are {@link PipelineOptions}, they are serialized with the pipeline and are
 * visible to every worker JVM, which lets worker-side emission agree on the run identity.
 */
public interface OpenLineagePipelineOptions extends PipelineOptions {

  @Description(
      "Disables OpenLineage emission entirely. The OPENLINEAGE_DISABLED environment variable is "
          + "also honored, matching other OpenLineage integrations.")
  @Default.Boolean(false)
  boolean isOpenLineageDisabled();

  void setOpenLineageDisabled(boolean disabled);

  @Description(
      "OpenLineage job namespace. Overrides the job.namespace from openlineage.yml. "
          + "Defaults to \"beam_jobs\" when not configured anywhere.")
  @Nullable
  String getOpenLineageNamespace();

  void setOpenLineageNamespace(@Nullable String namespace);

  @Description(
      "OpenLineage job name. Overrides the job.name from openlineage.yml. Defaults to the "
          + "Beam application name when not configured anywhere.")
  @Nullable
  String getOpenLineageJobName();

  void setOpenLineageJobName(@Nullable String jobName);

  @Description(
      "Run UUID for this pipeline execution. OpenLineageRunner mints a fresh UUIDv7 at "
          + "submission and stores it here so that all worker JVMs share it. When absent, a "
          + "deterministic UUID is derived from the job identity so independent JVMs still "
          + "agree.")
  @Nullable
  String getOpenLineageRunId();

  void setOpenLineageRunId(@Nullable String runId);

  @Description(
      "Interval in seconds between RUNNING lifecycle events while the pipeline executes, "
          + "matching the Flink integration's openlineage.trackingIntervalInSeconds. Defaults "
          + "to 60. Set openLineageDisableTracking to turn periodic events off.")
  @Nullable
  Integer getOpenLineageTrackingIntervalInSeconds();

  void setOpenLineageTrackingIntervalInSeconds(@Nullable Integer seconds);

  @Description(
      "Disables the periodic RUNNING event tracker, matching the Flink integration's "
          + "openlineage.flink.disableCheckpointTracking.")
  @Default.Boolean(false)
  boolean isOpenLineageDisableTracking();

  void setOpenLineageDisableTracking(boolean disabled);

  @Description(
      "The runner to delegate to when using --runner=OpenLineageRunner. Accepts a simple class "
          + "name registered via PipelineRunnerRegistrar (e.g. FlinkRunner, DirectRunner) or a "
          + "fully qualified class name.")
  @Default.String("DirectRunner")
  String getOpenLineageDelegateRunner();

  void setOpenLineageDelegateRunner(String delegateRunner);

  @Description(
      "Parent job name for the parent run facet, matching spark.openlineage.parentJobName. "
          + "The facet is attached only when parent run id, job name and namespace are all set.")
  @Nullable
  String getOpenLineageParentJobName();

  void setOpenLineageParentJobName(@Nullable String parentJobName);

  @Description(
      "Parent job namespace for the parent run facet, matching "
          + "spark.openlineage.parentJobNamespace.")
  @Nullable
  String getOpenLineageParentJobNamespace();

  void setOpenLineageParentJobNamespace(@Nullable String parentJobNamespace);

  @Description(
      "Parent run id (UUID) for the parent run facet, matching spark.openlineage.parentRunId.")
  @Nullable
  String getOpenLineageParentRunId();

  void setOpenLineageParentRunId(@Nullable String parentRunId);
}
