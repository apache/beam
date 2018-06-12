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

import java.util.Map;
import org.apache.beam.runners.samza.metrics.SamzaMetricsContainer;
import org.apache.beam.runners.samza.translation.ConfigBuilder;
import org.apache.beam.runners.samza.translation.PViewToIdMapper;
import org.apache.beam.runners.samza.translation.SamzaPipelineTranslator;
import org.apache.beam.runners.samza.translation.SamzaTransformOverrides;
import org.apache.beam.runners.samza.util.PipelineDotRenderer;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PValue;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.operators.ContextManager;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.runtime.ApplicationRunner;
import org.apache.samza.task.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link PipelineRunner} that executes the operations in the {@link Pipeline} into an equivalent
 * Samza plan.
 */
public class SamzaRunner extends PipelineRunner<SamzaPipelineResult> {
  private static final Logger LOG = LoggerFactory.getLogger(SamzaRunner.class);

  public static SamzaRunner fromOptions(PipelineOptions opts) {
    final SamzaPipelineOptions samzaOptions = SamzaPipelineOptionsValidator.validate(opts);
    return new SamzaRunner(samzaOptions);
  }

  private final SamzaPipelineOptions options;

  public SamzaRunner(SamzaPipelineOptions options) {
    this.options = options;
  }

  @Override
  public SamzaPipelineResult run(Pipeline pipeline) {
    MetricsEnvironment.setMetricsSupported(true);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Pre-processed Beam pipeline:\n{}", PipelineDotRenderer.toDotString(pipeline));
    }

    pipeline.replaceAll(SamzaTransformOverrides.getDefaultOverrides());

    if (LOG.isDebugEnabled()) {
      LOG.debug("Post-processed Beam pipeline:\n{}", PipelineDotRenderer.toDotString(pipeline));
    }

    // Add a dummy source for use in special cases (TestStream, empty flatten)
    final PValue dummySource = pipeline.apply("Dummy Input Source", Create.of("dummy"));

    final Map<PValue, String> idMap = PViewToIdMapper.buildIdMap(pipeline);
    final Map<String, String> config = ConfigBuilder.buildConfig(pipeline, options, idMap);

    final SamzaExecutionContext executionContext = new SamzaExecutionContext();

    final ApplicationRunner runner = ApplicationRunner.fromConfig(new MapConfig(config));

    final StreamApplication app = new StreamApplication() {
      @Override
      public void init(StreamGraph streamGraph, Config config) {
        // TODO: we should probably not be creating the execution context this early since it needs
        // to be shipped off to various tasks.
        streamGraph.withContextManager(new ContextManager() {
          @Override
          public void init(Config config, TaskContext context) {
            if (executionContext.getMetricsContainer() == null) {
              final MetricsRegistryMap metricsRegistry = (MetricsRegistryMap)
                  context.getSamzaContainerContext().metricsRegistry;
              executionContext.setMetricsContainer(new SamzaMetricsContainer(metricsRegistry));
            }

            context.setUserContext(executionContext);
          }

          @Override
          public void close() {
          }
        });

        SamzaPipelineTranslator.translate(pipeline, options, streamGraph, idMap, dummySource);
      }
    };

    final SamzaPipelineResult result = new SamzaPipelineResult(app, runner, executionContext);
    runner.run(app);
    return result;
  }
}
