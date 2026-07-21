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

import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.UUIDUtils;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.sdk.options.ApplicationNameOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Holds the OpenLineage state of one pipeline execution in this JVM — run identity, discovered
 * datasets, and event lifecycle — mirroring the {@code OpenLineageContext} classes of the Spark and
 * Flink integrations. One instance exists per JVM (driver or worker); every JVM of the same
 * pipeline execution resolves the identical run id so events merge into one run downstream.
 */
class OpenLineageContext {

  private static final Logger LOG = LoggerFactory.getLogger(OpenLineageContext.class);
  private static final String DEFAULT_JOB_NAMESPACE = "beam_jobs";

  private static @Nullable OpenLineageContext instance;
  private static @Nullable BeamOpenLineageConfig configOverride;

  /** Returns the per-JVM context, creating it from the given options on first use. */
  static synchronized OpenLineageContext getOrCreate(PipelineOptions options) {
    if (instance == null) {
      instance = new OpenLineageContext(options);
    }
    return instance;
  }

  /** Test seam, mirroring the Spark listener's overrideDefaultFactoryForTests. */
  @VisibleForTesting
  static synchronized void overrideConfigForTests(@Nullable BeamOpenLineageConfig config) {
    configOverride = config;
  }

  @VisibleForTesting
  static synchronized void resetForTests() {
    instance = null;
    configOverride = null;
  }

  private final OpenLineage openLineage = new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI);
  private final BeamOpenLineageConfig config;
  private final EventEmitter emitter;
  private final String jobNamespace;
  private final String jobName;
  private final UUID runId;
  private final OpenLineage.@Nullable ParentRunFacet parentRunFacet;
  private final boolean disabled;

  private final Map<String, DatasetIdentifier> inputs = new ConcurrentHashMap<>();
  private final Map<String, DatasetIdentifier> outputs = new ConcurrentHashMap<>();
  private final AtomicBoolean started = new AtomicBoolean(false);
  private final AtomicBoolean finished = new AtomicBoolean(false);
  private volatile boolean streaming;

  private OpenLineageContext(PipelineOptions options) {
    OpenLineagePipelineOptions olOptions = options.as(OpenLineagePipelineOptions.class);
    BeamOpenLineageConfig resolved = BeamOpenLineageConfigParser.parse(olOptions);
    if (configOverride != null) {
      resolved = resolved.mergeWith(configOverride);
    }
    this.config = resolved;
    this.disabled =
        olOptions.isOpenLineageDisabled()
            || Boolean.parseBoolean(System.getenv("OPENLINEAGE_DISABLED"));
    this.emitter = new EventEmitter(resolved);
    this.jobNamespace = resolveJobNamespace(resolved);
    this.jobName = resolveJobName(resolved, options);
    this.runId = resolveRunId(olOptions, options);
    this.parentRunFacet = buildParentRunFacet(openLineage, olOptions);
    this.streaming = options.as(org.apache.beam.sdk.options.StreamingOptions.class).isStreaming();
  }

  private static String resolveJobNamespace(BeamOpenLineageConfig config) {
    if (config.getJobConfig() != null && config.getJobConfig().getNamespace() != null) {
      return config.getJobConfig().getNamespace();
    }
    return DEFAULT_JOB_NAMESPACE;
  }

  private static String resolveJobName(BeamOpenLineageConfig config, PipelineOptions options) {
    if (config.getJobConfig() != null && config.getJobConfig().getName() != null) {
      return config.getJobConfig().getName();
    }
    String appName = options.as(ApplicationNameOptions.class).getAppName();
    return appName != null ? appName : options.getJobName();
  }

  /**
   * Resolves the run id. An explicit id (minted by {@link OpenLineageRunner} at submission and
   * propagated through serialized options, like the Spark integration's driver-minted application
   * run id) wins. Otherwise a deterministic UUID is derived from the job identity with the same
   * helper the Airflow provider uses, so independent worker JVMs agree.
   */
  private static UUID resolveRunId(OpenLineagePipelineOptions olOptions, PipelineOptions options) {
    String explicit = olOptions.getOpenLineageRunId();
    if (explicit != null) {
      return UUID.fromString(explicit);
    }
    byte[] identity =
        (options.getJobName() + "/" + options.getOptionsId()).getBytes(StandardCharsets.UTF_8);
    return UUIDUtils.generateStaticUUID(Instant.EPOCH, identity);
  }

  private static OpenLineage.@Nullable ParentRunFacet buildParentRunFacet(
      OpenLineage openLineage, OpenLineagePipelineOptions options) {
    String parentRunId = options.getOpenLineageParentRunId();
    String parentJobName = options.getOpenLineageParentJobName();
    String parentJobNamespace = options.getOpenLineageParentJobNamespace();
    if (parentRunId == null || parentJobName == null || parentJobNamespace == null) {
      return null;
    }
    return openLineage
        .newParentRunFacetBuilder()
        .run(openLineage.newParentRunFacetRun(UUID.fromString(parentRunId)))
        .job(openLineage.newParentRunFacetJob(parentJobNamespace, parentJobName))
        .root(
            openLineage.newParentRunFacetRoot(
                openLineage.newRootRun(UUID.fromString(parentRunId)),
                openLineage.newRootJob(parentJobNamespace, parentJobName)))
        .build();
  }

  BeamOpenLineageConfig getConfig() {
    return config;
  }

  UUID getRunId() {
    return runId;
  }

  boolean isDisabled() {
    return disabled;
  }

  void setStreaming(boolean streaming) {
    this.streaming = this.streaming || streaming;
  }

  /** Emits START once per run in this JVM. */
  void onJobSubmitted() {
    if (disabled) {
      return;
    }
    if (started.compareAndSet(false, true)) {
      emit(OpenLineage.RunEvent.EventType.START, null);
    }
  }

  /** Registers a dataset without emitting; returns true when it is new to this run. */
  boolean registerDataset(LineageDirection direction, DatasetIdentifier dataset) {
    if (disabled) {
      return false;
    }
    Map<String, DatasetIdentifier> target = direction == LineageDirection.INPUT ? inputs : outputs;
    boolean added = target.putIfAbsent(dataset.getNamespace() + dataset.getName(), dataset) == null;
    if (added) {
      LOG.info(
          "OpenLineage discovered {} dataset {} {}",
          direction,
          dataset.getNamespace(),
          dataset.getName());
    }
    return added;
  }

  /**
   * Registers a dataset discovered at runtime (worker-side plugin path) and emits a RUNNING event
   * when it is new, so long-running streaming jobs surface datasets as they appear.
   */
  void onDatasetDiscovered(LineageDirection direction, DatasetIdentifier dataset) {
    if (registerDataset(direction, dataset) && started.get() && !finished.get()) {
      emit(OpenLineage.RunEvent.EventType.RUNNING, null);
    }
  }

  /** Emits the periodic RUNNING event, driven by {@link OpenLineageJobTracker}. */
  void onTrackingTick() {
    if (disabled || finished.get()) {
      return;
    }
    onJobSubmitted();
    emit(OpenLineage.RunEvent.EventType.RUNNING, null);
  }

  /** Emits the terminal event exactly once. */
  void onJobFinished(OpenLineage.RunEvent.EventType eventType, @Nullable Throwable error) {
    if (disabled) {
      return;
    }
    if (finished.compareAndSet(false, true)) {
      started.set(true);
      emit(eventType, error);
    }
  }

  private void emit(OpenLineage.RunEvent.EventType eventType, @Nullable Throwable error) {
    try {
      emitter.emit(buildEvent(eventType, error));
    } catch (RuntimeException e) {
      LOG.warn("Failed to build OpenLineage {} event", eventType, e);
    }
  }

  private OpenLineage.RunEvent buildEvent(
      OpenLineage.RunEvent.EventType eventType, @Nullable Throwable error) {
    OpenLineage.RunFacetsBuilder runFacets =
        openLineage
            .newRunFacetsBuilder()
            .processing_engine(
                openLineage
                    .newProcessingEngineRunFacetBuilder()
                    .name("beam")
                    .version(Versions.getVersion())
                    .openlineageAdapterVersion(Versions.getVersion())
                    .build());
    if (parentRunFacet != null) {
      runFacets.parent(parentRunFacet);
    }
    if (error != null) {
      String message = error.getMessage() == null ? error.toString() : error.getMessage();
      runFacets.errorMessage(
          openLineage.newErrorMessageRunFacet(message, "JAVA", stackTraceOf(error)));
    }
    return openLineage
        .newRunEventBuilder()
        .eventType(eventType)
        .eventTime(ZonedDateTime.now(ZoneOffset.UTC))
        .run(openLineage.newRunBuilder().runId(runId).facets(runFacets.build()).build())
        .job(
            openLineage
                .newJobBuilder()
                .namespace(jobNamespace)
                .name(jobName)
                .facets(
                    openLineage
                        .newJobFacetsBuilder()
                        .jobType(
                            openLineage
                                .newJobTypeJobFacetBuilder()
                                .processingType(streaming ? "STREAMING" : "BATCH")
                                .integration("BEAM")
                                .jobType("JOB")
                                .build())
                        .build())
                .build())
        .inputs(buildInputs())
        .outputs(buildOutputs())
        .build();
  }

  private List<OpenLineage.InputDataset> buildInputs() {
    List<OpenLineage.InputDataset> result = new ArrayList<>();
    for (DatasetIdentifier di : inputs.values()) {
      OpenLineage.InputDatasetBuilder builder =
          openLineage.newInputDatasetBuilder().namespace(di.getNamespace()).name(di.getName());
      OpenLineage.DatasetFacets facets = datasetFacets(di);
      if (facets != null) {
        builder.facets(facets);
      }
      result.add(builder.build());
    }
    return result;
  }

  private List<OpenLineage.OutputDataset> buildOutputs() {
    List<OpenLineage.OutputDataset> result = new ArrayList<>();
    for (DatasetIdentifier di : outputs.values()) {
      OpenLineage.OutputDatasetBuilder builder =
          openLineage.newOutputDatasetBuilder().namespace(di.getNamespace()).name(di.getName());
      OpenLineage.DatasetFacets facets = datasetFacets(di);
      if (facets != null) {
        builder.facets(facets);
      }
      result.add(builder.build());
    }
    return result;
  }

  private OpenLineage.@Nullable DatasetFacets datasetFacets(DatasetIdentifier di) {
    if (di.getSymlinks().isEmpty()) {
      return null;
    }
    List<OpenLineage.SymlinksDatasetFacetIdentifiers> identifiers = new ArrayList<>();
    for (DatasetIdentifier.Symlink symlink : di.getSymlinks()) {
      identifiers.add(
          openLineage.newSymlinksDatasetFacetIdentifiers(
              symlink.getNamespace(), symlink.getName(), symlink.getType().toString()));
    }
    return openLineage
        .newDatasetFacetsBuilder()
        .symlinks(openLineage.newSymlinksDatasetFacetBuilder().identifiers(identifiers).build())
        .build();
  }

  private static String stackTraceOf(Throwable error) {
    java.io.StringWriter writer = new java.io.StringWriter();
    error.printStackTrace(new java.io.PrintWriter(writer));
    return writer.toString();
  }

  /** Direction of a dataset relative to the pipeline. */
  enum LineageDirection {
    INPUT,
    OUTPUT
  }
}
