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
package org.apache.beam.fn.harness;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;
import org.apache.beam.fn.harness.control.BeamFnControlClient;
import org.apache.beam.fn.harness.control.FinalizeBundleHandler;
import org.apache.beam.fn.harness.control.HarnessMonitoringInfosInstructionHandler;
import org.apache.beam.fn.harness.control.ProcessBundleHandler;
import org.apache.beam.fn.harness.data.BeamFnDataGrpcClient;
import org.apache.beam.fn.harness.logging.BeamFnLoggingClient;
import org.apache.beam.fn.harness.state.BeamFnStateGrpcClientCache;
import org.apache.beam.fn.harness.status.BeamFnStatusClient;
import org.apache.beam.fn.harness.stream.HarnessStreamObserverFactories;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleDescriptor;
import org.apache.beam.model.fnexecution.v1.BeamFnControlGrpc;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.runners.core.construction.PipelineOptionsTranslation;
import org.apache.beam.runners.core.metrics.ExecutionStateSampler;
import org.apache.beam.runners.core.metrics.MetricsContainerImpl;
import org.apache.beam.runners.core.metrics.ShortIdMap;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.fn.IdGenerator;
import org.apache.beam.sdk.fn.IdGenerators;
import org.apache.beam.sdk.fn.JvmInitializers;
import org.apache.beam.sdk.fn.channel.AddHarnessIdInterceptor;
import org.apache.beam.sdk.fn.channel.ManagedChannelFactory;
import org.apache.beam.sdk.fn.stream.OutboundObserverFactory;
import org.apache.beam.sdk.function.ThrowingFunction;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.vendor.grpc.v1p43p2.com.google.protobuf.TextFormat;
import org.apache.beam.vendor.grpc.v1p43p2.io.grpc.ManagedChannel;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main entry point into the Beam SDK Fn Harness for Java.
 *
 * <p>This entry point expects the following environment variables:
 *
 * <ul>
 *   <li>HARNESS_ID: A String representing the ID of this FnHarness. This will be added to the
 *       headers of calls to the Beam Control Service
 *   <li>LOGGING_API_SERVICE_DESCRIPTOR: A {@link
 *       org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor} encoded as text
 *       representing the endpoint that is to be connected to for the Beam Fn Logging service.
 *   <li>CONTROL_API_SERVICE_DESCRIPTOR: A {@link Endpoints.ApiServiceDescriptor} encoded as text
 *       representing the endpoint that is to be connected to for the Beam Fn Control service.
 *   <li>PIPELINE_OPTIONS: A serialized form of {@link PipelineOptions}. See {@link PipelineOptions}
 *       for further details.
 * </ul>
 */
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class FnHarness {
  private static final String HARNESS_ID = "HARNESS_ID";
  private static final String CONTROL_API_SERVICE_DESCRIPTOR = "CONTROL_API_SERVICE_DESCRIPTOR";
  private static final String LOGGING_API_SERVICE_DESCRIPTOR = "LOGGING_API_SERVICE_DESCRIPTOR";
  private static final String STATUS_API_SERVICE_DESCRIPTOR = "STATUS_API_SERVICE_DESCRIPTOR";
  private static final String PIPELINE_OPTIONS = "PIPELINE_OPTIONS";
  private static final String RUNNER_CAPABILITIES = "RUNNER_CAPABILITIES";
  private static final Logger LOG = LoggerFactory.getLogger(FnHarness.class);

  private static Endpoints.ApiServiceDescriptor getApiServiceDescriptor(String descriptor)
      throws TextFormat.ParseException {
    Endpoints.ApiServiceDescriptor.Builder apiServiceDescriptorBuilder =
        Endpoints.ApiServiceDescriptor.newBuilder();
    TextFormat.merge(descriptor, apiServiceDescriptorBuilder);
    return apiServiceDescriptorBuilder.build();
  }

  public static void main(String[] args) throws Exception {
    main(System::getenv);
  }

  @VisibleForTesting
  public static void main(Function<String, String> environmentVarGetter) throws Exception {
    JvmInitializers.runOnStartup();
    System.out.format("SDK Fn Harness started%n");
    System.out.format("Harness ID %s%n", environmentVarGetter.apply(HARNESS_ID));
    System.out.format(
        "Logging location %s%n", environmentVarGetter.apply(LOGGING_API_SERVICE_DESCRIPTOR));
    System.out.format(
        "Control location %s%n", environmentVarGetter.apply(CONTROL_API_SERVICE_DESCRIPTOR));
    System.out.format(
        "Status location %s%n", environmentVarGetter.apply(STATUS_API_SERVICE_DESCRIPTOR));
    System.out.format("Pipeline options %s%n", environmentVarGetter.apply(PIPELINE_OPTIONS));

    String id = environmentVarGetter.apply(HARNESS_ID);
    PipelineOptions options =
        PipelineOptionsTranslation.fromJson(environmentVarGetter.apply(PIPELINE_OPTIONS));

    Endpoints.ApiServiceDescriptor loggingApiServiceDescriptor =
        getApiServiceDescriptor(environmentVarGetter.apply(LOGGING_API_SERVICE_DESCRIPTOR));

    Endpoints.ApiServiceDescriptor controlApiServiceDescriptor =
        getApiServiceDescriptor(environmentVarGetter.apply(CONTROL_API_SERVICE_DESCRIPTOR));

    Endpoints.ApiServiceDescriptor statusApiServiceDescriptor =
        environmentVarGetter.apply(STATUS_API_SERVICE_DESCRIPTOR) == null
            ? null
            : getApiServiceDescriptor(environmentVarGetter.apply(STATUS_API_SERVICE_DESCRIPTOR));
    String runnerCapabilitesOrNull = environmentVarGetter.apply(RUNNER_CAPABILITIES);
    Set<String> runnerCapabilites =
        runnerCapabilitesOrNull == null
            ? Collections.emptySet()
            : ImmutableSet.copyOf(runnerCapabilitesOrNull.split("\\s+"));

    main(
        id,
        options,
        runnerCapabilites,
        loggingApiServiceDescriptor,
        controlApiServiceDescriptor,
        statusApiServiceDescriptor);
  }

  /**
   * Run a FnHarness with the given id and options that attaches to the specified logging and
   * control API service descriptors.
   *
   * @param id Harness ID
   * @param options The options for this pipeline
   * @param runnerCapabilites
   * @param loggingApiServiceDescriptor
   * @param controlApiServiceDescriptor
   * @param statusApiServiceDescriptor
   * @throws Exception
   */
  public static void main(
      String id,
      PipelineOptions options,
      Set<String> runnerCapabilites,
      Endpoints.ApiServiceDescriptor loggingApiServiceDescriptor,
      Endpoints.ApiServiceDescriptor controlApiServiceDescriptor,
      @Nullable Endpoints.ApiServiceDescriptor statusApiServiceDescriptor)
      throws Exception {
    ManagedChannelFactory channelFactory;
    if (ExperimentalOptions.hasExperiment(options, "beam_fn_api_epoll")) {
      channelFactory = ManagedChannelFactory.createEpoll();
    } else {
      channelFactory = ManagedChannelFactory.createDefault();
    }
    OutboundObserverFactory outboundObserverFactory =
        HarnessStreamObserverFactories.fromOptions(options);

    main(
        id,
        options,
        runnerCapabilites,
        loggingApiServiceDescriptor,
        controlApiServiceDescriptor,
        statusApiServiceDescriptor,
        channelFactory,
        outboundObserverFactory,
        Caches.fromOptions(options));
  }

  /**
   * Run a FnHarness with the given id and options that attaches to the specified logging and
   * control API service descriptors using the given channel factory and outbound observer factory.
   *
   * @param id Harness ID
   * @param options The options for this pipeline
   * @param runnerCapabilites
   * @param loggingApiServiceDescriptor
   * @param controlApiServiceDescriptor
   * @param statusApiServiceDescriptor
   * @param channelFactory
   * @param outboundObserverFactory
   * @param processWideCache
   * @throws Exception
   */
  public static void main(
      String id,
      PipelineOptions options,
      Set<String> runnerCapabilites,
      Endpoints.ApiServiceDescriptor loggingApiServiceDescriptor,
      Endpoints.ApiServiceDescriptor controlApiServiceDescriptor,
      Endpoints.ApiServiceDescriptor statusApiServiceDescriptor,
      ManagedChannelFactory channelFactory,
      OutboundObserverFactory outboundObserverFactory,
      Cache<Object, Object> processWideCache)
      throws Exception {
    channelFactory =
        channelFactory.withInterceptors(ImmutableList.of(AddHarnessIdInterceptor.create(id)));

    IdGenerator idGenerator = IdGenerators.decrementingLongs();
    ShortIdMap metricsShortIds = new ShortIdMap();
    ExecutorService executorService = options.as(GcsOptions.class).getExecutorService();
    // The logging client variable is not used per se, but during its lifetime (until close()) it
    // intercepts logging and sends it to the logging service.
    try (BeamFnLoggingClient logging =
        new BeamFnLoggingClient(
            options, loggingApiServiceDescriptor, channelFactory::forDescriptor)) {
      LOG.info("Fn Harness started");
      // Register standard file systems.
      FileSystems.setDefaultPipelineOptions(options);
      EnumMap<
              BeamFnApi.InstructionRequest.RequestCase,
              ThrowingFunction<InstructionRequest, BeamFnApi.InstructionResponse.Builder>>
          handlers = new EnumMap<>(BeamFnApi.InstructionRequest.RequestCase.class);

      ManagedChannel channel = channelFactory.forDescriptor(controlApiServiceDescriptor);
      BeamFnControlGrpc.BeamFnControlStub controlStub = BeamFnControlGrpc.newStub(channel);
      BeamFnControlGrpc.BeamFnControlBlockingStub blockingControlStub =
          BeamFnControlGrpc.newBlockingStub(channel);

      BeamFnDataGrpcClient beamFnDataMultiplexer =
          new BeamFnDataGrpcClient(options, channelFactory::forDescriptor, outboundObserverFactory);

      BeamFnStateGrpcClientCache beamFnStateGrpcClientCache =
          new BeamFnStateGrpcClientCache(idGenerator, channelFactory, outboundObserverFactory);

      FinalizeBundleHandler finalizeBundleHandler =
          new FinalizeBundleHandler(options.as(GcsOptions.class).getExecutorService());

      Function<String, BeamFnApi.ProcessBundleDescriptor> getProcessBundleDescriptor =
          new Function<String, ProcessBundleDescriptor>() {
            private static final String PROCESS_BUNDLE_DESCRIPTORS = "ProcessBundleDescriptors";
            private final Cache<String, BeamFnApi.ProcessBundleDescriptor> cache =
                Caches.subCache(processWideCache, PROCESS_BUNDLE_DESCRIPTORS);

            @Override
            public BeamFnApi.ProcessBundleDescriptor apply(String id) {
              return cache.computeIfAbsent(id, this::loadDescriptor);
            }

            private BeamFnApi.ProcessBundleDescriptor loadDescriptor(String id) {
              return blockingControlStub.getProcessBundleDescriptor(
                  BeamFnApi.GetProcessBundleDescriptorRequest.newBuilder()
                      .setProcessBundleDescriptorId(id)
                      .build());
            }
          };

      MetricsEnvironment.setProcessWideContainer(MetricsContainerImpl.createProcessWideContainer());

      ProcessBundleHandler processBundleHandler =
          new ProcessBundleHandler(
              options,
              runnerCapabilites,
              getProcessBundleDescriptor,
              beamFnDataMultiplexer,
              beamFnStateGrpcClientCache,
              finalizeBundleHandler,
              metricsShortIds,
              processWideCache);

      BeamFnStatusClient beamFnStatusClient = null;
      if (statusApiServiceDescriptor != null) {
        beamFnStatusClient =
            new BeamFnStatusClient(
                statusApiServiceDescriptor,
                channelFactory::forDescriptor,
                processBundleHandler.getBundleProcessorCache(),
                options,
                processWideCache);
      }

      // TODO(BEAM-9729): Remove once runners no longer send this instruction.
      handlers.put(
          BeamFnApi.InstructionRequest.RequestCase.REGISTER,
          request ->
              BeamFnApi.InstructionResponse.newBuilder()
                  .setRegister(BeamFnApi.RegisterResponse.getDefaultInstance()));
      handlers.put(
          BeamFnApi.InstructionRequest.RequestCase.FINALIZE_BUNDLE,
          finalizeBundleHandler::finalizeBundle);
      handlers.put(
          BeamFnApi.InstructionRequest.RequestCase.PROCESS_BUNDLE,
          processBundleHandler::processBundle);
      handlers.put(
          BeamFnApi.InstructionRequest.RequestCase.PROCESS_BUNDLE_PROGRESS,
          processBundleHandler::progress);
      handlers.put(
          BeamFnApi.InstructionRequest.RequestCase.PROCESS_BUNDLE_SPLIT,
          processBundleHandler::trySplit);
      handlers.put(
          InstructionRequest.RequestCase.MONITORING_INFOS,
          request ->
              BeamFnApi.InstructionResponse.newBuilder()
                  .setMonitoringInfos(
                      BeamFnApi.MonitoringInfosMetadataResponse.newBuilder()
                          .putAllMonitoringInfo(
                              StreamSupport.stream(
                                      request
                                          .getMonitoringInfos()
                                          .getMonitoringInfoIdList()
                                          .spliterator(),
                                      false)
                                  .collect(
                                      Collectors.toMap(
                                          Function.identity(), metricsShortIds::get)))));

      HarnessMonitoringInfosInstructionHandler processWideHandler =
          new HarnessMonitoringInfosInstructionHandler(metricsShortIds);
      handlers.put(
          InstructionRequest.RequestCase.HARNESS_MONITORING_INFOS,
          processWideHandler::harnessMonitoringInfos);

      JvmInitializers.runBeforeProcessing(options);

      String samplingPeriodMills =
          ExperimentalOptions.getExperimentValue(
              options, ExperimentalOptions.STATE_SAMPLING_PERIOD_MILLIS);
      if (samplingPeriodMills != null) {
        ExecutionStateSampler.setSamplingPeriod(Integer.parseInt(samplingPeriodMills));
      }
      ExecutionStateSampler.instance().start();

      LOG.info("Entering instruction processing loop");

      // The control client immediately dispatches requests to an executor so we execute on the
      // direct executor. If we created separate channels for different stubs we could use
      // directExecutor() when building the channel.
      BeamFnControlClient control =
          new BeamFnControlClient(
              controlStub.withExecutor(MoreExecutors.directExecutor()),
              outboundObserverFactory,
              executorService,
              handlers);
      control.waitForTermination();
      if (beamFnStatusClient != null) {
        beamFnStatusClient.close();
      }
      processBundleHandler.shutdown();
    } finally {
      System.out.println("Shutting SDK harness down.");
      ExecutionStateSampler.instance().stop();
      executorService.shutdown();
    }
  }
}
