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

import java.util.EnumMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.beam.fn.harness.control.AddHarnessIdInterceptor;
import org.apache.beam.fn.harness.control.BeamFnControlClient;
import org.apache.beam.fn.harness.control.FinalizeBundleHandler;
import org.apache.beam.fn.harness.control.ProcessBundleHandler;
import org.apache.beam.fn.harness.data.BeamFnDataGrpcClient;
import org.apache.beam.fn.harness.logging.BeamFnLoggingClient;
import org.apache.beam.fn.harness.state.BeamFnStateGrpcClientCache;
import org.apache.beam.fn.harness.stream.HarnessStreamObserverFactories;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionResponse.Builder;
import org.apache.beam.model.fnexecution.v1.BeamFnControlGrpc;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.runners.core.construction.PipelineOptionsTranslation;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.fn.IdGenerator;
import org.apache.beam.sdk.fn.IdGenerators;
import org.apache.beam.sdk.fn.JvmInitializers;
import org.apache.beam.sdk.fn.channel.ManagedChannelFactory;
import org.apache.beam.sdk.fn.stream.OutboundObserverFactory;
import org.apache.beam.sdk.function.ThrowingFunction;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.TextFormat;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.ManagedChannel;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.CacheBuilder;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.CacheLoader;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.LoadingCache;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
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
public class FnHarness {
  private static final String HARNESS_ID = "HARNESS_ID";
  private static final String CONTROL_API_SERVICE_DESCRIPTOR = "CONTROL_API_SERVICE_DESCRIPTOR";
  private static final String LOGGING_API_SERVICE_DESCRIPTOR = "LOGGING_API_SERVICE_DESCRIPTOR";
  private static final String PIPELINE_OPTIONS = "PIPELINE_OPTIONS";
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
    System.out.format("Pipeline options %s%n", environmentVarGetter.apply(PIPELINE_OPTIONS));

    String id = environmentVarGetter.apply(HARNESS_ID);
    PipelineOptions options =
        PipelineOptionsTranslation.fromJson(environmentVarGetter.apply(PIPELINE_OPTIONS));

    Endpoints.ApiServiceDescriptor loggingApiServiceDescriptor =
        getApiServiceDescriptor(environmentVarGetter.apply(LOGGING_API_SERVICE_DESCRIPTOR));

    Endpoints.ApiServiceDescriptor controlApiServiceDescriptor =
        getApiServiceDescriptor(environmentVarGetter.apply(CONTROL_API_SERVICE_DESCRIPTOR));

    main(id, options, loggingApiServiceDescriptor, controlApiServiceDescriptor);
  }

  /**
   * Run a FnHarness with the given id and options that attaches to the specified logging and
   * control API service descriptors.
   *
   * @param id Harness ID
   * @param options The options for this pipeline
   * @param loggingApiServiceDescriptor
   * @param controlApiServiceDescriptor
   * @throws Exception
   */
  public static void main(
      String id,
      PipelineOptions options,
      Endpoints.ApiServiceDescriptor loggingApiServiceDescriptor,
      Endpoints.ApiServiceDescriptor controlApiServiceDescriptor)
      throws Exception {
    ManagedChannelFactory channelFactory;
    List<String> experiments = options.as(ExperimentalOptions.class).getExperiments();
    if (experiments != null && experiments.contains("beam_fn_api_epoll")) {
      channelFactory = ManagedChannelFactory.createEpoll();
    } else {
      channelFactory = ManagedChannelFactory.createDefault();
    }
    OutboundObserverFactory outboundObserverFactory =
        HarnessStreamObserverFactories.fromOptions(options);
    channelFactory =
        channelFactory.withInterceptors(ImmutableList.of(AddHarnessIdInterceptor.create(id)));
    main(
        id,
        options,
        loggingApiServiceDescriptor,
        controlApiServiceDescriptor,
        channelFactory,
        outboundObserverFactory);
  }

  /**
   * Run a FnHarness with the given id and options that attaches to the specified logging and
   * control API service descriptors using the given channel factory and outbound observer factory.
   *
   * @param id Harness ID
   * @param options The options for this pipeline
   * @param loggingApiServiceDescriptor
   * @param controlApiServiceDescriptor
   * @param channelFactory
   * @param outboundObserverFactory
   * @throws Exception
   */
  public static void main(
      String id,
      PipelineOptions options,
      Endpoints.ApiServiceDescriptor loggingApiServiceDescriptor,
      Endpoints.ApiServiceDescriptor controlApiServiceDescriptor,
      ManagedChannelFactory channelFactory,
      OutboundObserverFactory outboundObserverFactory)
      throws Exception {
    IdGenerator idGenerator = IdGenerators.decrementingLongs();
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
              ThrowingFunction<InstructionRequest, Builder>>
          handlers = new EnumMap<>(BeamFnApi.InstructionRequest.RequestCase.class);

      ManagedChannel channel = channelFactory.forDescriptor(controlApiServiceDescriptor);
      BeamFnControlGrpc.BeamFnControlStub controlStub = BeamFnControlGrpc.newStub(channel);
      BeamFnControlGrpc.BeamFnControlBlockingStub blockingControlStub =
          BeamFnControlGrpc.newBlockingStub(channel);

      BeamFnDataGrpcClient beamFnDataMultiplexer =
          new BeamFnDataGrpcClient(options, channelFactory::forDescriptor, outboundObserverFactory);

      BeamFnStateGrpcClientCache beamFnStateGrpcClientCache =
          new BeamFnStateGrpcClientCache(
              idGenerator, channelFactory::forDescriptor, outboundObserverFactory);

      FinalizeBundleHandler finalizeBundleHandler =
          new FinalizeBundleHandler(options.as(GcsOptions.class).getExecutorService());

      LoadingCache<String, BeamFnApi.ProcessBundleDescriptor> processBundleDescriptors =
          CacheBuilder.newBuilder()
              .maximumSize(1000)
              .expireAfterAccess(10, TimeUnit.MINUTES)
              .build(
                  new CacheLoader<String, BeamFnApi.ProcessBundleDescriptor>() {
                    @Override
                    public BeamFnApi.ProcessBundleDescriptor load(String id) {
                      return blockingControlStub.getProcessBundleDescriptor(
                          BeamFnApi.GetProcessBundleDescriptorRequest.newBuilder()
                              .setProcessBundleDescriptorId(id)
                              .build());
                    }
                  });

      ProcessBundleHandler processBundleHandler =
          new ProcessBundleHandler(
              options,
              processBundleDescriptors::getUnchecked,
              beamFnDataMultiplexer,
              beamFnStateGrpcClientCache,
              finalizeBundleHandler);
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
      BeamFnControlClient control =
          new BeamFnControlClient(id, controlStub, outboundObserverFactory, handlers);

      JvmInitializers.runBeforeProcessing(options);

      LOG.info("Entering instruction processing loop");
      control.processInstructionRequests(executorService);
      processBundleHandler.shutdown();
    } finally {
      System.out.println("Shutting SDK harness down.");
      executorService.shutdown();
    }
  }
}
