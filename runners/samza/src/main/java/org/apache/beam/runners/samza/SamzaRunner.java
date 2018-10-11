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

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.ServerFactory;
import org.apache.beam.runners.fnexecution.control.ControlClientPool;
import org.apache.beam.runners.fnexecution.control.FnApiControlClientPoolService;
import org.apache.beam.runners.fnexecution.control.InstructionRequestHandler;
import org.apache.beam.runners.fnexecution.control.JobBundleFactory;
import org.apache.beam.runners.fnexecution.control.MapControlClientPool;
import org.apache.beam.runners.fnexecution.control.SingleEnvironmentInstanceJobBundleFactory;
import org.apache.beam.runners.fnexecution.data.GrpcDataService;
import org.apache.beam.runners.fnexecution.environment.EnvironmentFactory;
import org.apache.beam.runners.fnexecution.environment.RemoteEnvironment;
import org.apache.beam.runners.fnexecution.state.GrpcStateService;
import org.apache.beam.runners.samza.metrics.SamzaMetricsContainer;
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
import org.apache.beam.sdk.fn.stream.OutboundObserverFactory;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PValue;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
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
  private static final String SAMZA_WORKER_ID = "samza_py_worker_id";

  private GrpcFnServer<FnApiControlClientPoolService> fnControlServer;
  private GrpcFnServer<GrpcDataService> fnDataServer;
  private GrpcFnServer<GrpcStateService> fnStateServer;
  private ControlClientPool controlClientPool;
  private JobBundleFactory jobBundleFactory;

  public static SamzaRunner fromOptions(PipelineOptions opts) {
    final SamzaPipelineOptions samzaOptions = SamzaPipelineOptionsValidator.validate(opts);
    return new SamzaRunner(samzaOptions);
  }

  private final SamzaPipelineOptions options;

  public SamzaRunner(SamzaPipelineOptions options) {
    this.options = options;
  }

  private void closeFnServer(GrpcFnServer<?> fnServer) {
    try (AutoCloseable closer = fnServer) {
      // do nothing
    } catch (Exception e) {
      LOG.error("Failed to close fn api servers. Ignore since this is shutdown process...", e);
    }
  }

  private void setUpContextManager(
      StreamGraph streamGraph, SamzaExecutionContext executionContext) {
    streamGraph.withContextManager(
        new ContextManager() {
          @Override
          public void init(Config config, TaskContext context) {
            if (executionContext.getMetricsContainer() == null) {
              final MetricsRegistryMap metricsRegistry =
                  (MetricsRegistryMap) context.getSamzaContainerContext().metricsRegistry;
              executionContext.setMetricsContainer(new SamzaMetricsContainer(metricsRegistry));
            }

            if (SamzaRunnerOverrideConfigs.isPortableMode(options)) {
              if (jobBundleFactory == null) {
                try {
                  final long waitTimeoutMs =
                      SamzaRunnerOverrideConfigs.getControlClientWaitTimeoutMs(options);
                  final InstructionRequestHandler instructionHandler =
                      controlClientPool
                          .getSource()
                          .take(SAMZA_WORKER_ID, Duration.ofMillis(waitTimeoutMs));
                  final EnvironmentFactory environmentFactory =
                      environment -> RemoteEnvironment.forHandler(environment, instructionHandler);
                  // TODO: use JobBundleFactoryBase.WrappedSdkHarnessClient.wrapping
                  jobBundleFactory =
                      SingleEnvironmentInstanceJobBundleFactory.create(
                          environmentFactory, fnDataServer, fnStateServer);
                } catch (Exception e) {
                  throw new RuntimeException(
                      "Running samza in Beam portable mode but failed to create job bundle factory",
                      e);
                }
                executionContext.setJobBundleFactory(jobBundleFactory);
              }
            }

            context.setUserContext(executionContext);
          }

          @Override
          public void close() {
            closeFnServer(fnControlServer);
            fnControlServer = null;
            closeFnServer(fnDataServer);
            fnDataServer = null;
            closeFnServer(fnStateServer);
            fnStateServer = null;
          }
        });
  }

  private void setUpFnApiServer() {
    controlClientPool = MapControlClientPool.create();
    ExecutorService dataExecutor = Executors.newCachedThreadPool();
    try {
      fnControlServer =
          GrpcFnServer.allocatePortAndCreateFor(
              FnApiControlClientPoolService.offeringClientsToPool(
                  controlClientPool.getSink(), () -> SAMZA_WORKER_ID),
              ServerFactory.createWithPortSupplier(
                  () -> SamzaRunnerOverrideConfigs.getFnControlPort(options)));

      fnDataServer =
          GrpcFnServer.allocatePortAndCreateFor(
              GrpcDataService.create(dataExecutor, OutboundObserverFactory.serverDirect()),
              ServerFactory.createDefault());

      fnStateServer =
          GrpcFnServer.allocatePortAndCreateFor(
              GrpcStateService.create(), ServerFactory.createDefault());
    } catch (Exception e) {
      LOG.error("Failed to set up fn api servers", e);
      throw new RuntimeException(e);
    }
  }

  SamzaPipelineResult runPortablePipeline(RunnerApi.Pipeline pipeline) {
    final SamzaExecutionContext executionContext = new SamzaExecutionContext();
    setUpFnApiServer();
    ConfigBuilder configBuilder = new ConfigBuilder(options);
    SamzaPortablePipelineTranslator.createConfig(pipeline, configBuilder);
    final ApplicationRunner runner = ApplicationRunner.fromConfig(configBuilder.build());
    final StreamApplication app =
        (streamGraph, config) -> {
          setUpContextManager(streamGraph, executionContext);
          SamzaPortablePipelineTranslator.translate(
              pipeline, new PortableTranslationContext(streamGraph, options));
        };
    final SamzaPipelineResult result = new SamzaPipelineResult(app, runner, executionContext);
    runner.run(app);
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

    // Add a dummy source for use in special cases (TestStream, empty flatten)
    final PValue dummySource = pipeline.apply("Dummy Input Source", Create.of("dummy"));
    final Map<PValue, String> idMap = PViewToIdMapper.buildIdMap(pipeline);

    final ConfigBuilder configBuilder = new ConfigBuilder(options);
    SamzaPipelineTranslator.createConfig(pipeline, options, idMap, configBuilder);
    final ApplicationRunner runner = ApplicationRunner.fromConfig(configBuilder.build());

    final SamzaExecutionContext executionContext = new SamzaExecutionContext();

    final StreamApplication app =
        new StreamApplication() {
          @Override
          public void init(StreamGraph streamGraph, Config config) {
            setUpContextManager(streamGraph, executionContext);
            SamzaPipelineTranslator.translate(
                pipeline, new TranslationContext(streamGraph, idMap, options, dummySource));
          }
        };

    final SamzaPipelineResult result = new SamzaPipelineResult(app, runner, executionContext);
    runner.run(app);
    return result;
  }
}
