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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionRequest.RequestCase;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleDescriptor;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.RegisterResponse;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.runners.core.construction.ParDoTranslation;
import org.apache.beam.runners.core.construction.SdkComponents;
import org.apache.beam.runners.fnexecution.control.InstructionRequestHandler;
import org.apache.beam.runners.fnexecution.data.FnDataService;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.IdGenerators;
import org.apache.beam.sdk.fn.data.CloseableFnDataReceiver;
import org.apache.beam.sdk.fn.data.CompletableFutureInboundDataClient;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.fn.data.InboundDataClient;
import org.apache.beam.sdk.fn.data.LogicalEndpoint;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow.IntervalWindowCoder;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link FnApiWindowMappingFn}. */
@RunWith(JUnit4.class)
public class FnApiWindowMappingFnTest {
  private static final ApiServiceDescriptor DATA_SERVICE =
      ApiServiceDescriptor.newBuilder().setUrl("test://data").build();
  private static final FunctionSpec WINDOW_MAPPING_SPEC =
      ParDoTranslation.translateWindowMappingFn(
          new GlobalWindows().getDefaultWindowMappingFn(),
          SdkComponents.create(PipelineOptionsFactory.create()));

  @Test
  public void testWindowMapping() throws Exception {
    TestSdkHarness testSdkHarness = new TestSdkHarness(GlobalWindow.INSTANCE);

    FnApiWindowMappingFn windowMappingFn =
        new FnApiWindowMappingFn(
            IdGenerators.decrementingLongs(),
            testSdkHarness,
            DATA_SERVICE,
            testSdkHarness,
            WINDOW_MAPPING_SPEC,
            IntervalWindowCoder.of(),
            GlobalWindow.Coder.INSTANCE);

    // Check mapping an element returns the expected result.
    BoundedWindow inputWindow = new IntervalWindow(Instant.now(), Duration.standardMinutes(1));
    assertEquals(GlobalWindow.INSTANCE, windowMappingFn.getSideInputWindow(inputWindow));
    assertEquals(inputWindow, ((KV) testSdkHarness.getInputValues().get(0).getValue()).getValue());

    // Check mapping a different element returns the expected result.
    BoundedWindow inputWindow2 = new IntervalWindow(Instant.now(), Duration.standardMinutes(2));
    assertEquals(GlobalWindow.INSTANCE, windowMappingFn.getSideInputWindow(inputWindow2));
    assertEquals(inputWindow2, ((KV) testSdkHarness.getInputValues().get(1).getValue()).getValue());

    // Check that mapping the same element returns a cached result.
    assertEquals(GlobalWindow.INSTANCE, windowMappingFn.getSideInputWindow(inputWindow));
    assertEquals(2, testSdkHarness.getInputValues().size());
  }

  /** A fake SDK harness which always returns the expected output value. */
  private static class TestSdkHarness implements FnDataService, InstructionRequestHandler {
    private boolean registered;
    private String processBundleDescriptorId;
    private BoundedWindow outputValue;
    private List<WindowedValue<?>> inputValues;
    private FnDataReceiver<WindowedValue<?>> inboundReceiver;
    private InboundDataClient inboundDataClient;

    TestSdkHarness(BoundedWindow outputValue) {
      this.outputValue = outputValue;
      this.inputValues = new ArrayList<>();
      this.registered = false;
    }

    public List<WindowedValue<?>> getInputValues() {
      return inputValues;
    }

    @Override
    public <T> InboundDataClient receive(
        LogicalEndpoint inputLocation, Coder<T> coder, FnDataReceiver<T> consumer) {
      this.inboundReceiver = (FnDataReceiver) consumer;
      this.inboundDataClient = CompletableFutureInboundDataClient.create();
      return inboundDataClient;
    }

    @Override
    public <T> CloseableFnDataReceiver<T> send(LogicalEndpoint outputLocation, Coder<T> coder) {
      return new CloseableFnDataReceiver<T>() {
        @Override
        public void accept(T value) throws Exception {
          WindowedValue<KV<Object, Object>> windowedValue =
              (WindowedValue<KV<Object, Object>>) value;
          inputValues.add(windowedValue);
          KV<Object, Object> kv = windowedValue.getValue();
          inboundReceiver.accept(windowedValue.withValue(KV.of(kv.getKey(), outputValue)));
          inboundDataClient.complete();
        }

        @Override
        public void flush() throws Exception {}

        @Override
        public void close() throws Exception {}
      };
    }

    @Override
    public CompletionStage<InstructionResponse> handle(InstructionRequest request) {
      if (RequestCase.REGISTER.equals(request.getRequestCase())) {
        assertFalse("Expected only a single registration request", registered);
        registered = true;
        processBundleDescriptorId =
            Iterables.getOnlyElement(request.getRegister().getProcessBundleDescriptorList())
                .getId();
        return CompletableFuture.completedFuture(
            InstructionResponse.newBuilder()
                .setInstructionId(request.getInstructionId())
                .setRegister(RegisterResponse.getDefaultInstance())
                .build());
      } else if (RequestCase.PROCESS_BUNDLE.equals(request.getRequestCase())) {
        assertEquals(
            processBundleDescriptorId, request.getProcessBundle().getProcessBundleDescriptorId());
        return CompletableFuture.completedFuture(
            InstructionResponse.newBuilder()
                .setInstructionId(request.getInstructionId())
                .setProcessBundle(ProcessBundleResponse.getDefaultInstance())
                .build());
      }
      throw new AssertionError(String.format("Unexpected request %s", request));
    }

    @Override
    public void registerProcessBundleDescriptor(ProcessBundleDescriptor descriptor) {}

    @Override
    public void close() {}
  }
}
