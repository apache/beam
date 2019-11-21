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
package org.apache.beam.runners.dataflow.worker;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleDescriptor;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.RegisterRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.RemoteGrpcPort;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.runners.core.construction.CoderTranslation;
import org.apache.beam.runners.core.construction.RehydratedComponents;
import org.apache.beam.runners.core.construction.SdkComponents;
import org.apache.beam.runners.fnexecution.control.InstructionRequestHandler;
import org.apache.beam.runners.fnexecution.data.FnDataService;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.fn.IdGenerator;
import org.apache.beam.sdk.fn.data.CloseableFnDataReceiver;
import org.apache.beam.sdk.fn.data.InboundDataClient;
import org.apache.beam.sdk.fn.data.LogicalEndpoint;
import org.apache.beam.sdk.fn.data.RemoteGrpcPortRead;
import org.apache.beam.sdk.fn.data.RemoteGrpcPortWrite;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.WindowMappingFn;
import org.apache.beam.sdk.util.MoreFutures;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.Cache;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.CacheBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maps main input windows onto side input windows.
 *
 * <p>Note that this {@link WindowMappingFn} performs blocking calls over the data plane. The thread
 * which maps side input windows should not be on the same thread that is reading from the same data
 * plane channel.
 *
 * <p>TODO: Swap this with an implementation which streams all required window mappings per bundle
 * instead of per mapping request. This requires rewriting the {@link StreamingSideInputFetcher} to
 * not be inline calls and process elements over a stream.
 */
class FnApiWindowMappingFn<TargetWindowT extends BoundedWindow>
    extends WindowMappingFn<TargetWindowT> {
  private static final Logger LOG = LoggerFactory.getLogger(FnApiWindowMappingFn.class);
  private static final byte[] EMPTY_ARRAY = new byte[0];

  @AutoValue
  public abstract static class CacheKey {
    public static CacheKey create(FunctionSpec windowMappingFn, BoundedWindow mainWindow) {
      return new AutoValue_FnApiWindowMappingFn_CacheKey(windowMappingFn, mainWindow);
    }

    public abstract FunctionSpec getWindowMappingFn();

    public abstract BoundedWindow getMainWindow();
  }

  private static final Cache<CacheKey, BoundedWindow> sideInputMappingCache =
      CacheBuilder.newBuilder().maximumSize(1000).build();

  private final IdGenerator idGenerator;
  private final FnDataService beamFnDataService;
  private final InstructionRequestHandler instructionRequestHandler;
  private final FunctionSpec windowMappingFn;
  private final Coder<WindowedValue<KV<byte[], BoundedWindow>>> outboundCoder;
  private final Coder<WindowedValue<KV<byte[], TargetWindowT>>> inboundCoder;
  private final ProcessBundleDescriptor processBundleDescriptor;

  FnApiWindowMappingFn(
      IdGenerator idGenerator,
      InstructionRequestHandler instructionRequestHandler,
      ApiServiceDescriptor dataServiceApiServiceDescriptor,
      FnDataService beamFnDataService,
      FunctionSpec windowMappingFn,
      Coder<BoundedWindow> mainInputWindowCoder,
      Coder<TargetWindowT> sideInputWindowCoder) {
    this.idGenerator = idGenerator;
    this.instructionRequestHandler = instructionRequestHandler;
    this.beamFnDataService = beamFnDataService;
    this.windowMappingFn = windowMappingFn;

    SdkComponents sdkComponents = SdkComponents.create();
    sdkComponents.registerEnvironment(RunnerApi.Environment.getDefaultInstance());
    String mainInputWindowCoderId;
    String sideInputWindowCoderId;
    String windowingStrategyId;
    RunnerApi.Components components;
    try {
      mainInputWindowCoderId =
          sdkComponents.registerCoder(
              WindowedValue.getFullCoder(
                  KvCoder.of(ByteArrayCoder.of(), mainInputWindowCoder),
                  GlobalWindow.Coder.INSTANCE));
      sideInputWindowCoderId =
          sdkComponents.registerCoder(
              WindowedValue.getFullCoder(
                  KvCoder.of(ByteArrayCoder.of(), sideInputWindowCoder),
                  GlobalWindow.Coder.INSTANCE));
      windowingStrategyId =
          sdkComponents.registerWindowingStrategy(WindowingStrategy.globalDefault());
      RehydratedComponents rehydratedComponents =
          RehydratedComponents.forComponents(sdkComponents.toComponents());
      components = sdkComponents.toComponents();
      outboundCoder =
          (Coder)
              CoderTranslation.fromProto(
                  components.getCodersOrThrow(mainInputWindowCoderId), rehydratedComponents);
      inboundCoder =
          (Coder)
              CoderTranslation.fromProto(
                  components.getCodersOrThrow(sideInputWindowCoderId), rehydratedComponents);
    } catch (IOException e) {
      throw new IllegalStateException(
          "Unable to create side input window mapping process bundle specification.", e);
    }

    processBundleDescriptor =
        ProcessBundleDescriptor.newBuilder()
            .putAllCoders(components.getCodersMap())
            .putPcollections(
                "inPC",
                PCollection.newBuilder()
                    .setCoderId(mainInputWindowCoderId)
                    .setWindowingStrategyId(windowingStrategyId)
                    .build())
            .putPcollections(
                "outPC",
                PCollection.newBuilder()
                    .setCoderId(sideInputWindowCoderId)
                    .setWindowingStrategyId(windowingStrategyId)
                    .build())
            .putTransforms(
                "read",
                RunnerApi.PTransform.newBuilder()
                    .setSpec(
                        FunctionSpec.newBuilder()
                            .setUrn(RemoteGrpcPortRead.URN)
                            .setPayload(
                                RemoteGrpcPort.newBuilder()
                                    .setApiServiceDescriptor(dataServiceApiServiceDescriptor)
                                    .setCoderId(mainInputWindowCoderId)
                                    .build()
                                    .toByteString()))
                    .putOutputs("in", "inPC")
                    .build())
            .putTransforms(
                "map",
                RunnerApi.PTransform.newBuilder()
                    .setSpec(
                        FunctionSpec.newBuilder()
                            .setUrn("beam:transform:map_windows:v1")
                            .setPayload(windowMappingFn.toByteString()))
                    .putInputs("in", "inPC")
                    .putOutputs("out", "outPC")
                    .build())
            .putTransforms(
                "write",
                RunnerApi.PTransform.newBuilder()
                    .setSpec(
                        FunctionSpec.newBuilder()
                            .setUrn(RemoteGrpcPortWrite.URN)
                            .setPayload(
                                RemoteGrpcPort.newBuilder()
                                    .setApiServiceDescriptor(dataServiceApiServiceDescriptor)
                                    .setCoderId(sideInputWindowCoderId)
                                    .build()
                                    .toByteString()))
                    .putInputs("out", "outPC")
                    .build())
            .putAllWindowingStrategies(components.getWindowingStrategiesMap())
            .build();
  }

  @Override
  public TargetWindowT getSideInputWindow(BoundedWindow mainWindow) {
    try {
      TargetWindowT rval =
          (TargetWindowT)
              sideInputMappingCache.get(
                  CacheKey.create(windowMappingFn, mainWindow),
                  () -> loadIfNeeded(windowMappingFn, mainWindow));
      return rval;
    } catch (ExecutionException e) {
      throw new IllegalStateException(
          String.format("Unable to load side input window for %s", mainWindow), e);
    }
  }

  private TargetWindowT loadIfNeeded(FunctionSpec windowMappingFn, BoundedWindow mainWindow) {
    try {
      String processRequestInstructionId = idGenerator.getId();
      InstructionRequest processRequest =
          InstructionRequest.newBuilder()
              .setInstructionId(processRequestInstructionId)
              .setProcessBundle(
                  ProcessBundleRequest.newBuilder()
                      .setProcessBundleDescriptorId(registerIfRequired()))
              .build();

      ConcurrentLinkedQueue<WindowedValue<KV<byte[], TargetWindowT>>> outputValue =
          new ConcurrentLinkedQueue<>();

      // Open the inbound consumer
      InboundDataClient waitForInboundTermination =
          beamFnDataService.receive(
              LogicalEndpoint.of(processRequestInstructionId, "write"),
              inboundCoder,
              outputValue::add);

      CompletionStage<InstructionResponse> processResponse =
          instructionRequestHandler.handle(processRequest);

      // Open the outbound consumer
      try (CloseableFnDataReceiver<WindowedValue<KV<byte[], BoundedWindow>>> outboundConsumer =
          beamFnDataService.send(
              LogicalEndpoint.of(processRequestInstructionId, "read"), outboundCoder)) {

        outboundConsumer.accept(WindowedValue.valueInGlobalWindow(KV.of(EMPTY_ARRAY, mainWindow)));
      }

      // Check to see if processing the request failed.
      MoreFutures.get(processResponse);

      waitForInboundTermination.awaitCompletion();
      WindowedValue<KV<byte[], TargetWindowT>> sideInputWindow = outputValue.poll();
      checkState(
          sideInputWindow != null,
          "Expected side input window to have been emitted by SDK harness.");
      checkState(
          sideInputWindow.getValue() != null,
          "Side input window emitted by SDK harness was a WindowedValue with no value in it.");
      checkState(
          sideInputWindow.getValue().getValue() != null,
          "Side input window emitted by SDK harness was a WindowedValue<KV<...>> with a null V.");
      checkState(
          outputValue.isEmpty(),
          "Expected only a single side input window to have been emitted by "
              + "the SDK harness but also received %s",
          outputValue);
      return sideInputWindow.getValue().getValue();
    } catch (Throwable e) {
      LOG.error("Unable to map main input window {} to side input window.", mainWindow, e);
      throw new IllegalStateException(e);
    }
  }

  /** Should only be accessed from within {@link #registerIfRequired}. */
  private String processBundleDescriptorId;

  /**
   * Register a process bundle descriptor for this remote window mapping fn.
   *
   * <p>Caches the result after the first registration.
   */
  private synchronized String registerIfRequired() throws ExecutionException, InterruptedException {
    if (processBundleDescriptorId == null) {
      String descriptorId = idGenerator.getId();

      CompletionStage<InstructionResponse> response =
          instructionRequestHandler.handle(
              InstructionRequest.newBuilder()
                  .setInstructionId(idGenerator.getId())
                  .setRegister(
                      RegisterRequest.newBuilder()
                          .addProcessBundleDescriptor(
                              processBundleDescriptor.toBuilder().setId(descriptorId).build())
                          .build())
                  .build());
      // Check if the bundle descriptor is registered successfully.
      MoreFutures.get(response);
      processBundleDescriptorId = descriptorId;
    }
    return processBundleDescriptorId;
  }
}
