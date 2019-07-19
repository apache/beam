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

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.renderer.PipelineDotRenderer;
import org.apache.beam.runners.samza.translation.ConfigBuilder;
import org.apache.beam.runners.samza.translation.PViewToIdMapper;
import org.apache.beam.runners.samza.translation.PortableTranslationContext;
import org.apache.beam.runners.samza.translation.SamzaPipelineTranslator;
import org.apache.beam.runners.samza.translation.SamzaPortablePipelineTranslator;
import org.apache.beam.runners.samza.translation.SamzaTransformOverrides;
import org.apache.beam.runners.samza.translation.TranslationContext;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterators;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.context.ExternalContext;
import org.apache.samza.metrics.MetricsReporter;
import org.apache.samza.metrics.MetricsReporterFactory;
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
  private final SamzaPipelineLifeCycleListener listener;

  private SamzaRunner(SamzaPipelineOptions options) {
    this.options = options;
    final Iterator<SamzaPipelineLifeCycleListener.Registrar> listenerReg =
        ServiceLoader.load(SamzaPipelineLifeCycleListener.Registrar.class).iterator();
    this.listener =
        listenerReg.hasNext() ? Iterators.getOnlyElement(listenerReg).getLifeCycleListener() : null;
  }

  public SamzaPipelineResult runPortablePipeline(RunnerApi.Pipeline pipeline) {
    final ConfigBuilder configBuilder = new ConfigBuilder(options);
    SamzaPortablePipelineTranslator.createConfig(pipeline, configBuilder, options);

    final Config config = configBuilder.build();
    options.setConfigOverride(config);

    if (listener != null) {
      listener.onInit(config, options);
    }

    final SamzaExecutionContext executionContext = new SamzaExecutionContext(options);
    final Map<String, MetricsReporterFactory> reporterFactories = getMetricsReporters();
    final StreamApplication app =
        appDescriptor -> {
          appDescriptor
              .withApplicationContainerContextFactory(executionContext.new Factory())
              .withMetricsReporterFactories(reporterFactories);
          SamzaPortablePipelineTranslator.translate(
              pipeline, new PortableTranslationContext(appDescriptor, options));
        };

    return runSamzaApp(app, config, executionContext);
  }

  @Override
  public SamzaPipelineResult run(Pipeline pipeline) {
    MetricsEnvironment.setMetricsSupported(true);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Pre-processed Beam pipeline:\n{}", PipelineDotRenderer.toDotString(pipeline));
    }

    pipeline.replaceAll(SamzaTransformOverrides.getDefaultOverrides());

    LOG.info("Beam pipeline DOT graph:\n{}", PipelineDotRenderer.toDotString(pipeline));

    final Map<PValue, String> idMap = PViewToIdMapper.buildIdMap(pipeline);

    final ConfigBuilder configBuilder = new ConfigBuilder(options);
    SamzaPipelineTranslator.createConfig(pipeline, options, idMap, configBuilder);

    final Config config = configBuilder.build();
    options.setConfigOverride(config);

    if (listener != null) {
      listener.onInit(config, options);
    }

    final SamzaExecutionContext executionContext = new SamzaExecutionContext(options);
    final Map<String, MetricsReporterFactory> reporterFactories = getMetricsReporters();

    final StreamApplication app =
        appDescriptor -> {
          appDescriptor.withApplicationContainerContextFactory(executionContext.new Factory());
          appDescriptor.withMetricsReporterFactories(reporterFactories);

          SamzaPipelineTranslator.translate(
              pipeline, new TranslationContext(appDescriptor, idMap, options));
        };

    return runSamzaApp(app, config, executionContext);
  }

  private Map<String, MetricsReporterFactory> getMetricsReporters() {
    if (options.getMetricsReporters() != null) {
      final Map<String, MetricsReporterFactory> reporters = new HashMap<>();
      for (int i = 0; i < options.getMetricsReporters().size(); i++) {
        final String name = "beam-metrics-reporter-" + i;
        final MetricsReporter reporter = options.getMetricsReporters().get(i);

        reporters.put(name, (MetricsReporterFactory) (nm, processorId, config) -> reporter);
        LOG.info(name + ": " + reporter.getClass().getName());
      }
      return reporters;
    } else {
      return Collections.emptyMap();
    }
  }

  private SamzaPipelineResult runSamzaApp(
      StreamApplication app, Config config, SamzaExecutionContext executionContext) {

    final ApplicationRunner runner = ApplicationRunners.getApplicationRunner(app, config);
    final SamzaPipelineResult result =
        new SamzaPipelineResult(app, runner, executionContext, listener, config);

    ExternalContext externalContext = null;
    if (listener != null) {
      externalContext = listener.onStart();
    }

    runner.run(externalContext);

    if (listener != null
        && options.getSamzaExecutionEnvironment() == SamzaExecutionEnvironment.YARN) {
      listener.onSubmit();
    }

    return result;
  }
}
