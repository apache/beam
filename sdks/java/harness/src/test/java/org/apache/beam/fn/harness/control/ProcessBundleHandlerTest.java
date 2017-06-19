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

package org.apache.beam.fn.harness.control;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.google.protobuf.Message;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.beam.fn.harness.data.BeamFnDataClient;
import org.apache.beam.fn.harness.fn.ThrowingConsumer;
import org.apache.beam.fn.harness.fn.ThrowingRunnable;
import org.apache.beam.fn.v1.BeamFnApi;
import org.apache.beam.runners.core.PTransformRunnerFactory;
import org.apache.beam.sdk.common.runner.v1.RunnerApi;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.util.WindowedValue;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Tests for {@link ProcessBundleHandler}. */
@RunWith(JUnit4.class)
public class ProcessBundleHandlerTest {
  private static final String DATA_INPUT_URN = "urn:org.apache.beam:source:runner:0.1";
  private static final String DATA_OUTPUT_URN = "urn:org.apache.beam:sink:runner:0.1";

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Mock private BeamFnDataClient beamFnDataClient;
  @Captor private ArgumentCaptor<ThrowingConsumer<WindowedValue<String>>> consumerCaptor;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testOrderOfStartAndFinishCalls() throws Exception {
    BeamFnApi.ProcessBundleDescriptor processBundleDescriptor =
        BeamFnApi.ProcessBundleDescriptor.newBuilder()
            .putTransforms("2L", RunnerApi.PTransform.newBuilder()
                .setSpec(RunnerApi.FunctionSpec.newBuilder().setUrn(DATA_INPUT_URN).build())
                .putOutputs("2L-output", "2L-output-pc")
                .build())
            .putTransforms("3L", RunnerApi.PTransform.newBuilder()
                .setSpec(RunnerApi.FunctionSpec.newBuilder().setUrn(DATA_OUTPUT_URN).build())
                .putInputs("3L-input", "2L-output-pc")
                .build())
            .putPcollections("2L-output-pc", RunnerApi.PCollection.getDefaultInstance())
            .build();
    Map<String, Message> fnApiRegistry = ImmutableMap.of("1L", processBundleDescriptor);

    List<RunnerApi.PTransform> transformsProcessed = new ArrayList<>();
    List<String> orderOfOperations = new ArrayList<>();

    PTransformRunnerFactory<Object> startFinishRecorder = new PTransformRunnerFactory<Object>() {
      @Override
      public Object createRunnerForPTransform(
          PipelineOptions pipelineOptions,
          BeamFnDataClient beamFnDataClient,
          String pTransformId,
          RunnerApi.PTransform pTransform,
          Supplier<String> processBundleInstructionId,
          Map<String, RunnerApi.PCollection> pCollections,
          Map<String, RunnerApi.Coder> coders,
          Multimap<String, ThrowingConsumer<WindowedValue<?>>> pCollectionIdsToConsumers,
          Consumer<ThrowingRunnable> addStartFunction,
          Consumer<ThrowingRunnable> addFinishFunction) throws IOException {

        assertThat(processBundleInstructionId.get(), equalTo("999L"));

        transformsProcessed.add(pTransform);
        addStartFunction.accept(
            () -> orderOfOperations.add("Start" + pTransformId));
        addFinishFunction.accept(
            () -> orderOfOperations.add("Finish" + pTransformId));
        return null;
      }
    };

    ProcessBundleHandler handler = new ProcessBundleHandler(
        PipelineOptionsFactory.create(),
        fnApiRegistry::get,
        beamFnDataClient,
        ImmutableMap.of(
            DATA_INPUT_URN, startFinishRecorder,
            DATA_OUTPUT_URN, startFinishRecorder));

    handler.processBundle(BeamFnApi.InstructionRequest.newBuilder()
        .setInstructionId("999L")
        .setProcessBundle(
            BeamFnApi.ProcessBundleRequest.newBuilder().setProcessBundleDescriptorReference("1L"))
        .build());

    // Processing of transforms is performed in reverse order.
    assertThat(transformsProcessed, contains(
        processBundleDescriptor.getTransformsMap().get("3L"),
        processBundleDescriptor.getTransformsMap().get("2L")));
    // Start should occur in reverse order while finish calls should occur in forward order
    assertThat(orderOfOperations, contains("Start3L", "Start2L", "Finish2L", "Finish3L"));
  }

  @Test
  public void testCreatingPTransformExceptionsArePropagated() throws Exception {
    BeamFnApi.ProcessBundleDescriptor processBundleDescriptor =
        BeamFnApi.ProcessBundleDescriptor.newBuilder()
            .putTransforms("2L", RunnerApi.PTransform.newBuilder()
                .setSpec(RunnerApi.FunctionSpec.newBuilder().setUrn(DATA_INPUT_URN).build())
                .build())
            .build();
    Map<String, Message> fnApiRegistry = ImmutableMap.of("1L", processBundleDescriptor);

    ProcessBundleHandler handler = new ProcessBundleHandler(
        PipelineOptionsFactory.create(),
        fnApiRegistry::get,
        beamFnDataClient,
        ImmutableMap.of(DATA_INPUT_URN, new PTransformRunnerFactory<Object>() {
          @Override
          public Object createRunnerForPTransform(
              PipelineOptions pipelineOptions,
              BeamFnDataClient beamFnDataClient,
              String pTransformId,
              RunnerApi.PTransform pTransform,
              Supplier<String> processBundleInstructionId,
              Map<String, RunnerApi.PCollection> pCollections,
              Map<String, RunnerApi.Coder> coders,
              Multimap<String, ThrowingConsumer<WindowedValue<?>>> pCollectionIdsToConsumers,
              Consumer<ThrowingRunnable> addStartFunction,
              Consumer<ThrowingRunnable> addFinishFunction) throws IOException {
            thrown.expect(IllegalStateException.class);
            thrown.expectMessage("TestException");
            throw new IllegalStateException("TestException");
          }
        }));
    handler.processBundle(
        BeamFnApi.InstructionRequest.newBuilder().setProcessBundle(
            BeamFnApi.ProcessBundleRequest.newBuilder().setProcessBundleDescriptorReference("1L"))
            .build());
  }

  @Test
  public void testPTransformStartExceptionsArePropagated() throws Exception {
    BeamFnApi.ProcessBundleDescriptor processBundleDescriptor =
        BeamFnApi.ProcessBundleDescriptor.newBuilder()
            .putTransforms("2L", RunnerApi.PTransform.newBuilder()
                .setSpec(RunnerApi.FunctionSpec.newBuilder().setUrn(DATA_INPUT_URN).build())
                .build())
            .build();
    Map<String, Message> fnApiRegistry = ImmutableMap.of("1L", processBundleDescriptor);

    ProcessBundleHandler handler = new ProcessBundleHandler(
        PipelineOptionsFactory.create(),
        fnApiRegistry::get,
        beamFnDataClient,
        ImmutableMap.of(DATA_INPUT_URN, new PTransformRunnerFactory<Object>() {
          @Override
          public Object createRunnerForPTransform(
              PipelineOptions pipelineOptions,
              BeamFnDataClient beamFnDataClient,
              String pTransformId,
              RunnerApi.PTransform pTransform,
              Supplier<String> processBundleInstructionId,
              Map<String, RunnerApi.PCollection> pCollections,
              Map<String, RunnerApi.Coder> coders,
              Multimap<String, ThrowingConsumer<WindowedValue<?>>> pCollectionIdsToConsumers,
              Consumer<ThrowingRunnable> addStartFunction,
              Consumer<ThrowingRunnable> addFinishFunction) throws IOException {
            thrown.expect(IllegalStateException.class);
            thrown.expectMessage("TestException");
            addStartFunction.accept(ProcessBundleHandlerTest::throwException);
            return null;
          }
        }));
    handler.processBundle(
        BeamFnApi.InstructionRequest.newBuilder().setProcessBundle(
            BeamFnApi.ProcessBundleRequest.newBuilder().setProcessBundleDescriptorReference("1L"))
            .build());
  }

  @Test
  public void testPTransformFinishExceptionsArePropagated() throws Exception {
    BeamFnApi.ProcessBundleDescriptor processBundleDescriptor =
        BeamFnApi.ProcessBundleDescriptor.newBuilder()
            .putTransforms("2L", RunnerApi.PTransform.newBuilder()
                .setSpec(RunnerApi.FunctionSpec.newBuilder().setUrn(DATA_INPUT_URN).build())
                .build())
            .build();
    Map<String, Message> fnApiRegistry = ImmutableMap.of("1L", processBundleDescriptor);

    ProcessBundleHandler handler = new ProcessBundleHandler(
        PipelineOptionsFactory.create(),
        fnApiRegistry::get,
        beamFnDataClient,
        ImmutableMap.of(DATA_INPUT_URN, new PTransformRunnerFactory<Object>() {
          @Override
          public Object createRunnerForPTransform(
              PipelineOptions pipelineOptions,
              BeamFnDataClient beamFnDataClient,
              String pTransformId,
              RunnerApi.PTransform pTransform,
              Supplier<String> processBundleInstructionId,
              Map<String, RunnerApi.PCollection> pCollections,
              Map<String, RunnerApi.Coder> coders,
              Multimap<String, ThrowingConsumer<WindowedValue<?>>> pCollectionIdsToConsumers,
              Consumer<ThrowingRunnable> addStartFunction,
              Consumer<ThrowingRunnable> addFinishFunction) throws IOException {
            thrown.expect(IllegalStateException.class);
            thrown.expectMessage("TestException");
            addFinishFunction.accept(ProcessBundleHandlerTest::throwException);
            return null;
          }
        }));
    handler.processBundle(
        BeamFnApi.InstructionRequest.newBuilder().setProcessBundle(
            BeamFnApi.ProcessBundleRequest.newBuilder().setProcessBundleDescriptorReference("1L"))
            .build());
  }

  private static void throwException() {
    throw new IllegalStateException("TestException");
  }
}
