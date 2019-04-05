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
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.samza.translation.ConfigBuilder;
import org.apache.beam.runners.samza.translation.PViewToIdMapper;
import org.apache.beam.runners.samza.translation.PortableTranslationContext;
import org.apache.beam.runners.samza.translation.SamzaPipelineTranslator;
import org.apache.beam.runners.samza.translation.SamzaPortablePipelineTranslator;
import org.apache.beam.runners.samza.translation.SamzaTransformOverrides;
import org.apache.beam.runners.samza.translation.TranslationContext;
import org.apache.beam.runners.samza.util.PipelineDotRenderer;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.values.PValue;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.runtime.ApplicationRunner;
import org.apache.samza.runtime.ApplicationRunners;
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

  private SamzaRunner(SamzaPipelineOptions options) {
    this.options = options;
  }

  SamzaPipelineResult runPortablePipeline(RunnerApi.Pipeline pipeline) {
    ConfigBuilder configBuilder = new ConfigBuilder(options);
    SamzaPortablePipelineTranslator.createConfig(pipeline, configBuilder);
    final SamzaExecutionContext executionContext = new SamzaExecutionContext(options);
    final StreamApplication app =
        appDescriptor -> {
          appDescriptor.withApplicationContainerContextFactory(executionContext.new Factory());
          SamzaPortablePipelineTranslator.translate(
              pipeline, new PortableTranslationContext(appDescriptor, options));
        };
    final ApplicationRunner runner =
        ApplicationRunners.getApplicationRunner(app, configBuilder.build());
    final SamzaPipelineResult result = new SamzaPipelineResult(app, runner, executionContext);
    runner.run();
    return result;
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

    final Map<PValue, String> idMap = PViewToIdMapper.buildIdMap(pipeline);

    final ConfigBuilder configBuilder = new ConfigBuilder(options);
    SamzaPipelineTranslator.createConfig(pipeline, options, idMap, configBuilder);
    final SamzaExecutionContext executionContext = new SamzaExecutionContext(options);

    final StreamApplication app =
        appDescriptor -> {
          appDescriptor.withApplicationContainerContextFactory(executionContext.new Factory());
          SamzaPipelineTranslator.translate(
              pipeline, new TranslationContext(appDescriptor, idMap, options));
        };

    final ApplicationRunner runner =
        ApplicationRunners.getApplicationRunner(app, configBuilder.build());
    final SamzaPipelineResult result = new SamzaPipelineResult(app, runner, executionContext);
    runner.run();
    return result;
  }
}
