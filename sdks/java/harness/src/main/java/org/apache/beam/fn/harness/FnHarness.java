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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.beam.fn.harness.control.BeamFnControlClient;
import org.apache.beam.fn.harness.control.ExecutionStateSampler;
import org.apache.beam.fn.harness.control.FinalizeBundleHandler;
import org.apache.beam.fn.harness.control.HarnessMonitoringInfosInstructionHandler;
import org.apache.beam.fn.harness.control.ProcessBundleHandler;
import org.apache.beam.fn.harness.data.BeamFnDataGrpcClient;
import org.apache.beam.fn.harness.debug.DataSampler;
import org.apache.beam.fn.harness.logging.LoggingClient;
import org.apache.beam.fn.harness.logging.LoggingClientFactory;
import org.apache.beam.fn.harness.state.BeamFnStateGrpcClientCache;
import org.apache.beam.fn.harness.status.BeamFnStatusClient;
import org.apache.beam.fn.harness.stream.HarnessStreamObserverFactories;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleDescriptor;
import org.apache.beam.model.fnexecution.v1.BeamFnControlGrpc;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi;
import org.apache.beam.model.jobmanagement.v1.ArtifactRetrievalServiceGrpc;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.metrics.MetricsContainerImpl;
import org.apache.beam.runners.core.metrics.ShortIdMap;
import org.apache.beam.sdk.fn.IdGenerator;
import org.apache.beam.sdk.fn.IdGenerators;
import org.apache.beam.sdk.fn.JvmInitializers;
import org.apache.beam.sdk.fn.channel.AddHarnessIdInterceptor;
import org.apache.beam.sdk.fn.channel.ManagedChannelFactory;
import org.apache.beam.sdk.fn.stream.OutboundObserverFactory;
import org.apache.beam.sdk.function.ThrowingFunction;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.options.ExecutorOptions;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.SdkHarnessOptions;
import org.apache.beam.sdk.util.construction.ArtifactResolver;
import org.apache.beam.sdk.util.construction.BeamUrns;
import org.apache.beam.sdk.util.construction.CoderTranslation;
import org.apache.beam.sdk.util.construction.DefaultArtifactResolver;
import org.apache.beam.sdk.util.construction.PipelineOptionsTranslation;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.TextFormat;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.ManagedChannel;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.MoreExecutors;
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
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class FnHarness {
  private static final String HARNESS_ID = "HARNESS_ID";
  private static final String CONTROL_API_SERVICE_DESCRIPTOR = "CONTROL_API_SERVICE_DESCRIPTOR";
  private static final String LOGGING_API_SERVICE_DESCRIPTOR = "LOGGING_API_SERVICE_DESCRIPTOR";
  private static final String STATUS_API_SERVICE_DESCRIPTOR = "STATUS_API_SERVICE_DESCRIPTOR";

  private static final String PIPELINE_OPTIONS_FILE = "PIPELINE_OPTIONS_FILE";
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

  public static String removeNestedKey(String jsonString, String keyToRemove) throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    JsonNode rootNode = mapper.readTree(jsonString);

    removeKeyRecursively(rootNode, keyToRemove);

    return mapper.writeValueAsString(rootNode);
  }

  private static void removeKeyRecursively(JsonNode node, String keyToRemove) {
    if (node.isObject()) {
      Iterator<Map.Entry<String, JsonNode>> iterator = node.fields();
      while (iterator.hasNext()) {
        Map.Entry<String, JsonNode> field = iterator.next();
        if (field.getKey().equals(keyToRemove)) {
          iterator.remove(); // Safe removal using Iterator
        } else {
          removeKeyRecursively(field.getValue(), keyToRemove);
        }
      }
    }
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
    String id = environmentVarGetter.apply(HARNESS_ID);

    String pipelineOptionsJson = environmentVarGetter.apply(PIPELINE_OPTIONS);
    // Try looking for a file first. If that exists it should override PIPELINE_OPTIONS to avoid
    // maxing out the kernel's environment space
    try {
      String pipelineOptionsPath = environmentVarGetter.apply(PIPELINE_OPTIONS_FILE);
      System.out.format("Pipeline Options File %s%n", pipelineOptionsPath);
      if (pipelineOptionsPath != null) {
        Path filePath = Paths.get(pipelineOptionsPath);
        if (Files.exists(filePath)) {
          System.out.format(
              "Pipeline Options File %s exists. Overriding existing options.%n",
              pipelineOptionsPath);
          pipelineOptionsJson = new String(Files.readAllBytes(filePath), StandardCharsets.UTF_8);
        }
      }
    } catch (Exception e) {
      System.out.format("Problem loading pipeline options from file: %s%n", e.getMessage());
    }

    System.out.format("Pipeline options %s%n", pipelineOptionsJson);
    // TODO: https://github.com/apache/beam/issues/30301
    pipelineOptionsJson = removeNestedKey(pipelineOptionsJson, "impersonateServiceAccount");

    PipelineOptions options = PipelineOptionsTranslation.fromJson(pipelineOptionsJson);

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

    // Note: This main method overload doesn't receive the artifact endpoint.
    // It likely relies on the other overload being called by ExternalWorkerService.
    main(
        id,
        options,
        runnerCapabilites,
        loggingApiServiceDescriptor,
        controlApiServiceDescriptor,
        statusApiServiceDescriptor,
        null); // Pass null for artifact endpoint here
  }

  /**
   * Run a FnHarness with the given id and options that attaches to the specified logging and
   * control API service descriptors.
   *
   * @param id Harness ID
   * @param options The options for this pipeline
   * @param runnerCapabilities
   * @param loggingApiServiceDescriptor
   * @param controlApiServiceDescriptor
   * @param statusApiServiceDescriptor
   * @param artifactApiServiceDescriptor The endpoint for the ArtifactRetrievalService
   * @throws Exception
   */
  public static void main(
      String id,
      PipelineOptions options,
      Set<String> runnerCapabilities,
      Endpoints.ApiServiceDescriptor loggingApiServiceDescriptor,
      Endpoints.ApiServiceDescriptor controlApiServiceDescriptor,
      @Nullable Endpoints.ApiServiceDescriptor statusApiServiceDescriptor,
      @Nullable
          Endpoints.ApiServiceDescriptor artifactApiServiceDescriptor) // Added artifact endpoint
      throws Exception {
    ManagedChannelFactory channelFactory;
    if (ExperimentalOptions.hasExperiment(options, "beam_fn_api_epoll")) {
      channelFactory = ManagedChannelFactory.createEpoll();
    } else {
      channelFactory = ManagedChannelFactory.createDefault();
    }
    OutboundObserverFactory outboundObserverFactory =
        HarnessStreamObserverFactories.fromOptions(options);

    // Call the main overload that includes the artifact endpoint
    main(
        id,
        options,
        runnerCapabilities,
        loggingApiServiceDescriptor,
        controlApiServiceDescriptor,
        statusApiServiceDescriptor,
        artifactApiServiceDescriptor, // Pass it along
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
   * @param runnerCapabilities
   * @param loggingApiServiceDescriptor
   * @param controlApiServiceDescriptor
   * @param statusApiServiceDescriptor
   * @param artifactApiServiceDescriptor The endpoint for the ArtifactRetrievalService
   * @param channelFactory
   * @param outboundObserverFactory
   * @param processWideCache
   * @throws Exception
   */
  public static void main(
      String id,
      PipelineOptions options,
      Set<String> runnerCapabilities,
      Endpoints.ApiServiceDescriptor loggingApiServiceDescriptor,
      Endpoints.ApiServiceDescriptor controlApiServiceDescriptor,
      @Nullable Endpoints.ApiServiceDescriptor statusApiServiceDescriptor,
      @Nullable
          Endpoints.ApiServiceDescriptor artifactApiServiceDescriptor, // Added artifact endpoint
      ManagedChannelFactory channelFactory,
      OutboundObserverFactory outboundObserverFactory,
      Cache<Object, Object> processWideCache)
      throws Exception {
    channelFactory =
        channelFactory.withInterceptors(ImmutableList.of(AddHarnessIdInterceptor.create(id)));

    // Initialize network-based artifact resolution if endpoint is provided
    if (artifactApiServiceDescriptor != null) {
      LOG.info(
          "Initializing network artifact retrieval from {}", artifactApiServiceDescriptor.getUrl());
      try {
        ManagedChannel artifactChannel = channelFactory.forDescriptor(artifactApiServiceDescriptor);
        ArtifactRetrievalServiceGrpc.ArtifactRetrievalServiceBlockingStub artifactClient =
            ArtifactRetrievalServiceGrpc.newBlockingStub(artifactChannel);

        // Define the ResolutionFn
        ArtifactResolver.ResolutionFn networkResolver =
            (info) -> {
              // Only handle types that typically require remote fetching (FILE, URL, etc.)
              // Let the default resolver handle EMBEDDED or unknown types.
              String typeUrn = info.getTypeUrn();
              if (BeamUrns.getUrn(RunnerApi.StandardArtifacts.Types.FILE).equals(typeUrn)
                  || BeamUrns.getUrn(RunnerApi.StandardArtifacts.Types.URL).equals(typeUrn)) {
                try {
                  LOG.debug("Attempting to resolve artifact via network: {}", info.getTypeUrn());
                  // Create a temporary local file path
                  // TODO: Use a more robust temp file creation mechanism if available
                  Path tempPath = Files.createTempFile("beam_artifact_", info.getTypeUrn());
                  tempPath.toFile().deleteOnExit(); // Ensure cleanup

                  // Fetch artifact chunks using the gRPC client
                  Iterator<ArtifactApi.GetArtifactResponse> responses =
                      artifactClient.getArtifact(
                          ArtifactApi.GetArtifactRequest.newBuilder().setArtifact(info).build());

                  // Write chunks to the temporary file
                  try (OutputStream outputStream = new FileOutputStream(tempPath.toFile())) {
                    while (responses.hasNext()) {
                      responses.next().getData().writeTo(outputStream);
                    }
                  }

                  LOG.info(
                      "Successfully resolved artifact {} to local file {}",
                      info.getTypeUrn(),
                      tempPath);

                  // Return new ArtifactInformation pointing to the local file
                  RunnerApi.ArtifactInformation resolvedInfo =
                      info.toBuilder()
                          .setTypeUrn(BeamUrns.getUrn(RunnerApi.StandardArtifacts.Types.FILE))
                          .setTypePayload(
                              RunnerApi.ArtifactFilePayload.newBuilder()
                                  .setPath(tempPath.toString())
                                  .build()
                                  .toByteString())
                          .build();
                  return Optional.of(ImmutableList.of(resolvedInfo));

                } catch (Exception e) {
                  LOG.error("Failed to resolve artifact {} via network", info.getTypeUrn(), e);
                  // Fallback or rethrow? For now, let default handle it.
                  return Optional.empty();
                }
              } // Close the 'if' statement for FILE/URL types
              // Let the default resolver handle other types (like EMBEDDED)
              return Optional.empty();
            };

        // Register the network resolver. It will be checked before the default file resolver.
        DefaultArtifactResolver.INSTANCE.register(networkResolver);
        LOG.info("Registered network artifact resolver.");

      } catch (Exception e) {
        LOG.error("Failed to initialize network artifact retrieval client", e);
        // Proceed without network retrieval? Or fail hard?
        // For now, log and continue, relying on default resolution.
      }
    } else {
      LOG.warn("No artifact API service descriptor provided. Using default artifact resolution.");
    }

    IdGenerator idGenerator = IdGenerators.decrementingLongs();
    ShortIdMap metricsShortIds = new ShortIdMap();
    ExecutorService executorService =
        options.as(ExecutorOptions.class).getScheduledExecutorService();
    ExecutionStateSampler executionStateSampler =
        new ExecutionStateSampler(options, System::currentTimeMillis);

    final @Nullable DataSampler dataSampler = DataSampler.create(options);

    // The logging client variable is not used per se, but during its lifetime (until close()) it
    // intercepts logging and sends it to the logging service.
    try (LoggingClient logging =
        LoggingClientFactory.createAndStart(
            options, loggingApiServiceDescriptor, channelFactory::forDescriptor)) {
      LOG.info("Fn Harness started");
      // Register standard file systems.
      FileSystems.setDefaultPipelineOptions(options);
      CoderTranslation.verifyModelCodersRegistered();
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

      FinalizeBundleHandler finalizeBundleHandler = new FinalizeBundleHandler(executorService);

      // Retrieves the ProcessBundleDescriptor from cache. Requests the PBD from the Runner if it
      // doesn't exist. Additionally, runs any graph modifications.
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
              runnerCapabilities, // Use the corrected variable name
              getProcessBundleDescriptor,
              beamFnDataMultiplexer,
              beamFnStateGrpcClientCache,
              finalizeBundleHandler,
              metricsShortIds,
              executionStateSampler,
              processWideCache,
              dataSampler);

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

      // TODO(https://github.com/apache/beam/issues/20270): Remove once runners no longer send this
      // instruction.
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
                              metricsShortIds.get(
                                  request.getMonitoringInfos().getMonitoringInfoIdList()))));

      HarnessMonitoringInfosInstructionHandler processWideHandler =
          new HarnessMonitoringInfosInstructionHandler(metricsShortIds);
      handlers.put(
          InstructionRequest.RequestCase.HARNESS_MONITORING_INFOS,
          processWideHandler::harnessMonitoringInfos);
      handlers.put(
          InstructionRequest.RequestCase.SAMPLE_DATA,
          request ->
              dataSampler == null
                  ? BeamFnApi.InstructionResponse.newBuilder()
                      .setSampleData(BeamFnApi.SampleDataResponse.newBuilder())
                  : dataSampler.handleDataSampleRequest(request));

      JvmInitializers.runBeforeProcessing(options);

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
      if (options.as(SdkHarnessOptions.class).getEnableLogViaFnApi()) {
        CompletableFuture.anyOf(control.terminationFuture(), logging.terminationFuture()).get();
      } else {
        control.terminationFuture().get();
      }
      if (beamFnStatusClient != null) {
        beamFnStatusClient.close();
      }
      processBundleHandler.shutdown();
    } catch (Exception e) {
      LOG.error("Shutting down harness due to exception", e);
      e.printStackTrace();
    } finally {
      LOG.info("Shutting SDK harness down.");
      executionStateSampler.stop();
      executorService.shutdown();
    }
  }
}
