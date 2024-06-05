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

import static org.apache.beam.sdk.util.WindowedValue.valueInGlobalWindow;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.beam.fn.harness.PTransformRunnerFactory.Registrar;
import org.apache.beam.fn.harness.data.BeamFnDataClient;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.Elements;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.Elements.Data;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.MessageWithComponents;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.fn.data.BeamFnDataOutboundAggregator;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.fn.data.RemoteGrpcPortWrite;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.construction.CoderTranslation;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.hamcrest.collection.IsMapContaining;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Tests for {@link BeamFnDataWriteRunner}. */
@RunWith(JUnit4.class)
public class BeamFnDataWriteRunnerTest {

  private static final String ELEM_CODER_ID = "string-coder-id";
  private static final Coder<String> ELEM_CODER = StringUtf8Coder.of();
  private static final String WIRE_CODER_ID = "windowed-string-coder-id";
  private static final Coder<WindowedValue<String>> WIRE_CODER =
      WindowedValue.getFullCoder(ELEM_CODER, GlobalWindow.Coder.INSTANCE);
  private static final RunnerApi.Coder WIRE_CODER_SPEC;
  private static final RunnerApi.Components COMPONENTS;

  private static final BeamFnApi.RemoteGrpcPort PORT_SPEC =
      BeamFnApi.RemoteGrpcPort.newBuilder()
          .setApiServiceDescriptor(Endpoints.ApiServiceDescriptor.getDefaultInstance())
          .setCoderId(WIRE_CODER_ID)
          .build();

  static {
    try {
      MessageWithComponents coderAndComponents = CoderTranslation.toProto(WIRE_CODER);
      WIRE_CODER_SPEC = coderAndComponents.getCoder();
      COMPONENTS =
          coderAndComponents
              .getComponents()
              .toBuilder()
              .putCoders(WIRE_CODER_ID, WIRE_CODER_SPEC)
              .putCoders(ELEM_CODER_ID, CoderTranslation.toProto(ELEM_CODER).getCoder())
              .build();
    } catch (IOException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private static final String TRANSFORM_ID = "1";

  @Mock private BeamFnDataClient mockBeamFnDataClient;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  private BeamFnDataOutboundAggregator createRecordingAggregator(
      Map<String, List<WindowedValue<String>>> output, Supplier<String> bundleId) {
    PipelineOptions options = PipelineOptionsFactory.create();
    options.as(ExperimentalOptions.class).setExperiments(Arrays.asList("data_buffer_size_limit=0"));
    return new BeamFnDataOutboundAggregator(
        options,
        bundleId,
        new StreamObserver<Elements>() {
          @Override
          public void onNext(Elements elements) {
            for (Data data : elements.getDataList()) {
              try {
                output.get(bundleId.get()).add(WIRE_CODER.decode(data.getData().newInput()));
              } catch (IOException e) {
                throw new RuntimeException("Failed to decode output.");
              }
            }
          }

          @Override
          public void onError(Throwable throwable) {}

          @Override
          public void onCompleted() {}
        },
        false);
  }

  @Test
  public void testReuseForMultipleBundles() throws Exception {
    AtomicReference<String> bundleId = new AtomicReference<>("0");
    String localInputId = "inputPC";
    RunnerApi.PTransform pTransform =
        RemoteGrpcPortWrite.writeToPort(localInputId, PORT_SPEC).toPTransform();

    List<WindowedValue<String>> output0 = new ArrayList<>();
    List<WindowedValue<String>> output1 = new ArrayList<>();
    Map<ApiServiceDescriptor, BeamFnDataOutboundAggregator> aggregators = new HashMap<>();
    BeamFnDataOutboundAggregator aggregator =
        createRecordingAggregator(ImmutableMap.of("0", output0, "1", output1), bundleId::get);
    aggregators.put(PORT_SPEC.getApiServiceDescriptor(), aggregator);

    PTransformRunnerFactoryTestContext context =
        PTransformRunnerFactoryTestContext.builder(TRANSFORM_ID, pTransform)
            .beamFnDataClient(mockBeamFnDataClient)
            .processBundleInstructionIdSupplier(bundleId::get)
            .outboundAggregators(aggregators)
            .pCollections(
                ImmutableMap.of(
                    localInputId,
                    RunnerApi.PCollection.newBuilder().setCoderId(ELEM_CODER_ID).build()))
            .coders(COMPONENTS.getCodersMap())
            .windowingStrategies(COMPONENTS.getWindowingStrategiesMap())
            .build();

    new BeamFnDataWriteRunner.Factory<String>().createRunnerForPTransform(context);

    assertThat(context.getPCollectionConsumers().keySet(), containsInAnyOrder(localInputId));

    FnDataReceiver<Object> pCollectionConsumer = context.getPCollectionConsumer(localInputId);
    pCollectionConsumer.accept(valueInGlobalWindow("ABC"));
    pCollectionConsumer.accept(valueInGlobalWindow("DEF"));

    assertThat(output0, contains(valueInGlobalWindow("ABC"), valueInGlobalWindow("DEF")));

    output0.clear();

    // Process for bundle id 1
    bundleId.set("1");

    pCollectionConsumer.accept(valueInGlobalWindow("GHI"));
    pCollectionConsumer.accept(valueInGlobalWindow("JKL"));

    assertThat(output1, contains(valueInGlobalWindow("GHI"), valueInGlobalWindow("JKL")));
    verifyNoMoreInteractions(mockBeamFnDataClient);
  }

  @Test
  public void testRegistration() {
    for (Registrar registrar : ServiceLoader.load(Registrar.class)) {
      if (registrar instanceof BeamFnDataWriteRunner.Registrar) {
        assertThat(
            registrar.getPTransformRunnerFactories(),
            IsMapContaining.hasKey(RemoteGrpcPortWrite.URN));
        return;
      }
    }
    fail("Expected registrar not found.");
  }
}
