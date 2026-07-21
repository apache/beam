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
import io.openlineage.client.utils.UUIDUtils;
import java.util.ServiceLoader;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.PipelineRunnerRegistrar;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link PipelineRunner} that adds OpenLineage emission to any Beam runner.
 *
 * <p>Usage is configuration-only:
 *
 * <pre>{@code
 * --runner=OpenLineageRunner
 * --openLineageDelegateRunner=FlinkRunner
 * }</pre>
 *
 * <p>At submission it mints a UUIDv7 run id (like the Spark integration's driver-minted application
 * run id) and stores it in the serialized options so worker-side emission shares the run; extracts
 * datasets from the pipeline graph; emits START; delegates to the real runner; and starts a {@link
 * OpenLineageJobTracker} that emits periodic RUNNING events and the terminal COMPLETE/ABORT/FAIL
 * event. Transport configuration is read the standard OpenLineage way (see {@link
 * OpenLineagePipelineOptions}).
 */
public class OpenLineageRunner extends PipelineRunner<PipelineResult> {

  private static final Logger LOG = LoggerFactory.getLogger(OpenLineageRunner.class);

  private final PipelineOptions options;

  private OpenLineageRunner(PipelineOptions options) {
    this.options = options;
  }

  /** Constructs the runner from options; called by Beam's runner factory. */
  public static OpenLineageRunner fromOptions(PipelineOptions options) {
    return new OpenLineageRunner(options);
  }

  @Override
  public PipelineResult run(Pipeline pipeline) {
    OpenLineagePipelineOptions olOptions = options.as(OpenLineagePipelineOptions.class);
    if (olOptions.getOpenLineageRunId() == null) {
      olOptions.setOpenLineageRunId(UUIDUtils.generateNewUUID().toString());
    }

    OpenLineageContext context = OpenLineageContext.getOrCreate(options);
    try {
      PipelineGraphExtractor.extract(pipeline, context);
    } catch (RuntimeException e) {
      LOG.warn("OpenLineage graph extraction failed; continuing without static datasets", e);
    }
    context.onJobSubmitted();

    PipelineRunner<? extends PipelineResult> delegate = resolveDelegate(olOptions);
    LOG.info("OpenLineageRunner delegating to {}", delegate.getClass().getName());
    PipelineResult result;
    try {
      result = delegate.run(pipeline);
    } catch (RuntimeException e) {
      context.onJobFinished(OpenLineage.RunEvent.EventType.FAIL, e);
      throw e;
    }

    if (olOptions.isOpenLineageDisableTracking()) {
      LOG.info("OpenLineage tracking disabled; no RUNNING or terminal events will be emitted");
    } else {
      Integer configured = context.getConfig().getTrackingIntervalInSeconds();
      int intervalSeconds =
          configured != null ? configured : BeamOpenLineageConfig.DEFAULT_TRACKING_INTERVAL_SECONDS;
      new OpenLineageJobTracker(context, result, intervalSeconds).startTracking();
    }
    return result;
  }

  private PipelineRunner<? extends PipelineResult> resolveDelegate(
      OpenLineagePipelineOptions olOptions) {
    String name = olOptions.getOpenLineageDelegateRunner();
    Class<? extends PipelineRunner<?>> runnerClass = resolveRunnerClass(name);
    if (runnerClass == null || runnerClass == OpenLineageRunner.class) {
      throw new IllegalArgumentException(
          "Cannot resolve --openLineageDelegateRunner=" + name + " to a pipeline runner");
    }
    // Point the options at the real runner and let Beam's own factory construct it. Workers
    // see the delegate runner in their serialized options, exactly as if it had been used
    // directly.
    options.setRunner(runnerClass);
    return PipelineRunner.fromOptions(options);
  }

  private static @Nullable Class<? extends PipelineRunner<?>> resolveRunnerClass(String name) {
    try {
      Class<?> cls = Class.forName(name);
      if (PipelineRunner.class.isAssignableFrom(cls)) {
        @SuppressWarnings("unchecked")
        Class<? extends PipelineRunner<?>> runnerClass = (Class<? extends PipelineRunner<?>>) cls;
        return runnerClass;
      }
      return null;
    } catch (ClassNotFoundException e) {
      for (PipelineRunnerRegistrar registrar : ServiceLoader.load(PipelineRunnerRegistrar.class)) {
        for (Class<? extends PipelineRunner<?>> cls : registrar.getPipelineRunners()) {
          if (cls.getSimpleName().equals(name)) {
            return cls;
          }
        }
      }
      return null;
    }
  }
}
