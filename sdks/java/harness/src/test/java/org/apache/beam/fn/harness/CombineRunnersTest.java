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
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.core.IsEqual.equalTo;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.construction.ModelCoders;
import org.apache.beam.sdk.util.construction.PipelineTranslation;
import org.apache.beam.sdk.util.construction.SdkComponents;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link CombineRunners}. */
@RunWith(JUnit4.class)
public class CombineRunnersTest {
  // CombineFn that converts strings to ints and sums them up to an accumulator, and negates the
  // value of the accumulator when extracting outputs. These operations are chosen to avoid
  // autoboxing and provide a way to easily check that certain combine steps actually executed.
  private static class TestCombineFn extends CombineFn<String, Integer, Integer> {

    @Override
    public Integer createAccumulator() {
      return 0;
    }

    @Override
    public Integer addInput(Integer accum, String input) {
      accum += Integer.parseInt(input);
      return accum;
    }

    @Override
    public Integer mergeAccumulators(Iterable<Integer> accums) {
      Integer merged = 0;
      for (Integer accum : accums) {
        merged += accum;
      }
      return merged;
    }

    @Override
    public Integer extractOutput(Integer accum) {
      return -accum;
    }
  }

  private static final String TEST_COMBINE_ID = "combineId";

  private RunnerApi.PTransform pTransform;
  private String inputPCollectionId;
  private String outputPCollectionId;
  private RunnerApi.Pipeline pProto;

  @Before
  public void createPipeline() throws Exception {
    // Create pipeline with an input pCollection, combine, and output pCollection.
    TestCombineFn combineFn = new TestCombineFn();
    Combine.PerKey<String, String, Integer> combine = Combine.perKey(combineFn);

    Pipeline p = Pipeline.create();
    PCollection<KV<String, String>> inputPCollection = p.apply(Create.of(KV.of("unused", "0")));
    inputPCollection.setCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));
    PCollection<KV<String, Integer>> outputPCollection =
        inputPCollection.apply(TEST_COMBINE_ID, combine);
    outputPCollection.setCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of()));

    // Create FnApi protos needed for the runner.
    SdkComponents sdkComponents = SdkComponents.create(p.getOptions());
    pProto = PipelineTranslation.toProto(p, sdkComponents);
    inputPCollectionId = sdkComponents.registerPCollection(inputPCollection);
    outputPCollectionId = sdkComponents.registerPCollection(outputPCollection);
    pTransform = pProto.getComponents().getTransformsOrThrow(TEST_COMBINE_ID);
  }

  /**
   * Create a Precombine that is given keyed elements and validates that the outputted elements
   * values' are accumulators that were correctly derived from the input.
   */
  @Test
  public void testPrecombine() throws Exception {
    PTransformRunnerFactoryTestContext context =
        PTransformRunnerFactoryTestContext.builder(TEST_COMBINE_ID, pTransform)
            .pCollections(pProto.getComponents().getPcollectionsMap())
            .coders(pProto.getComponents().getCodersMap())
            .windowingStrategies(pProto.getComponents().getWindowingStrategiesMap())
            .build();
    // Add a consumer and output target to check output values.
    Deque<WindowedValue<KV<String, Integer>>> mainOutputValues = new ArrayDeque<>();
    context.addPCollectionConsumer(
        Iterables.getOnlyElement(pTransform.getOutputsMap().values()),
        (FnDataReceiver)
            (FnDataReceiver<WindowedValue<KV<String, Integer>>>) mainOutputValues::add);

    // Create runner.
    new CombineRunners.PrecombineFactory<>().createRunnerForPTransform(context);

    Iterables.getOnlyElement(context.getStartBundleFunctions()).run();

    // Send elements to runner and check outputs.
    mainOutputValues.clear();
    assertThat(
        context.getPCollectionConsumers().keySet(),
        containsInAnyOrder(inputPCollectionId, outputPCollectionId));

    FnDataReceiver<WindowedValue<?>> input = context.getPCollectionConsumer(inputPCollectionId);
    input.accept(valueInGlobalWindow(KV.of("A", "1")));
    input.accept(valueInGlobalWindow(KV.of("A", "2")));
    input.accept(valueInGlobalWindow(KV.of("A", "6")));
    input.accept(valueInGlobalWindow(KV.of("B", "2")));
    input.accept(valueInGlobalWindow(KV.of("C", "3")));

    Iterables.getOnlyElement(context.getFinishBundleFunctions()).run();

    // Check that all values for "A" were converted to accumulators regardless of how they were
    // combined by the Precombine optimization.
    Integer sum = 0;
    for (WindowedValue<KV<String, Integer>> outputValue : mainOutputValues) {
      if ("A".equals(outputValue.getValue().getKey())) {
        sum += outputValue.getValue().getValue();
      }
    }
    assertThat(sum, equalTo(9));

    // Check that elements for "B" and "C" are present as well.
    mainOutputValues.removeIf(elem -> "A".equals(elem.getValue().getKey()));
    assertThat(
        mainOutputValues,
        containsInAnyOrder(valueInGlobalWindow(KV.of("B", 2)), valueInGlobalWindow(KV.of("C", 3))));
  }

  /**
   * Create a Merge Accumulators function that is given keyed lists of accumulators and validates
   * that the accumulators of each list were merged.
   */
  @Test
  public void testMergeAccumulators() throws Exception {
    // Create coder map for size estimation
    RunnerApi.PCollection pCollection =
        RunnerApi.PCollection.newBuilder()
            .setUniqueName(inputPCollectionId)
            .setCoderId("coder-id")
            .build();
    Map<String, RunnerApi.PCollection> pCollectionMap =
        new HashMap<>(pProto.getComponents().getPcollectionsMap());
    pCollectionMap.put(inputPCollectionId, pCollection);
    Map<String, RunnerApi.Coder> coderMap = new HashMap<>(pProto.getComponents().getCodersMap());
    coderMap.put(
        "coder-id",
        RunnerApi.Coder.newBuilder()
            .setSpec(RunnerApi.FunctionSpec.newBuilder().setUrn(ModelCoders.KV_CODER_URN).build())
            .addComponentCoderIds("StringUtf8Coder")
            .addComponentCoderIds("coder-id-iterable")
            .build());
    coderMap.put(
        "coder-id-iterable",
        RunnerApi.Coder.newBuilder()
            .setSpec(
                RunnerApi.FunctionSpec.newBuilder().setUrn(ModelCoders.ITERABLE_CODER_URN).build())
            .addComponentCoderIds("BigEndianIntegerCoder")
            .build());

    PTransformRunnerFactoryTestContext context =
        PTransformRunnerFactoryTestContext.builder(TEST_COMBINE_ID, pTransform)
            .pCollections(pCollectionMap)
            .coders(coderMap)
            .build();
    // Add a consumer and output target to check output values.
    Deque<WindowedValue<KV<String, Integer>>> mainOutputValues = new ArrayDeque<>();
    context.addPCollectionConsumer(
        Iterables.getOnlyElement(pTransform.getOutputsMap().values()),
        (FnDataReceiver)
            (FnDataReceiver<WindowedValue<KV<String, Integer>>>) mainOutputValues::add);

    // Create runner.
    MapFnRunners.forValueMapFnFactory(CombineRunners::createMergeAccumulatorsMapFunction)
        .createRunnerForPTransform(context);

    assertThat(context.getStartBundleFunctions(), empty());
    assertThat(context.getFinishBundleFunctions(), empty());

    // Send elements to runner and check outputs.
    mainOutputValues.clear();
    assertThat(
        context.getPCollectionConsumers().keySet(),
        containsInAnyOrder(inputPCollectionId, outputPCollectionId));

    FnDataReceiver<WindowedValue<?>> input = context.getPCollectionConsumer(inputPCollectionId);
    input.accept(valueInGlobalWindow(KV.of("A", Arrays.asList(1, 2, 6))));
    input.accept(valueInGlobalWindow(KV.of("B", Arrays.asList(2, 3))));
    input.accept(valueInGlobalWindow(KV.of("C", Arrays.asList(5, 2))));

    assertThat(
        mainOutputValues,
        contains(
            valueInGlobalWindow(KV.of("A", 9)),
            valueInGlobalWindow(KV.of("B", 5)),
            valueInGlobalWindow(KV.of("C", 7))));
  }

  /**
   * Create an Extract Outputs function that is given keyed accumulators and validates that the
   * accumulators were turned into the output type.
   */
  @Test
  public void testExtractOutputs() throws Exception {
    // Create coder map for size estimation
    RunnerApi.PCollection pCollection =
        RunnerApi.PCollection.newBuilder()
            .setUniqueName(inputPCollectionId)
            .setCoderId("coder-id")
            .build();
    Map<String, RunnerApi.PCollection> pCollectionMap =
        new HashMap<>(pProto.getComponents().getPcollectionsMap());
    pCollectionMap.put(inputPCollectionId, pCollection);
    Map<String, RunnerApi.Coder> coderMap = new HashMap<>(pProto.getComponents().getCodersMap());
    coderMap.put(
        "coder-id",
        RunnerApi.Coder.newBuilder()
            .setSpec(RunnerApi.FunctionSpec.newBuilder().setUrn(ModelCoders.KV_CODER_URN).build())
            .addComponentCoderIds("StringUtf8Coder")
            .addComponentCoderIds("BigEndianIntegerCoder")
            .build());
    PTransformRunnerFactoryTestContext context =
        PTransformRunnerFactoryTestContext.builder(TEST_COMBINE_ID, pTransform)
            .pCollections(pCollectionMap)
            .coders(coderMap)
            .build();
    // Add a consumer and output target to check output values.
    Deque<WindowedValue<KV<String, Integer>>> mainOutputValues = new ArrayDeque<>();
    context.addPCollectionConsumer(
        Iterables.getOnlyElement(pTransform.getOutputsMap().values()),
        (FnDataReceiver)
            (FnDataReceiver<WindowedValue<KV<String, Integer>>>) mainOutputValues::add);

    // Create runner.
    MapFnRunners.forValueMapFnFactory(CombineRunners::createExtractOutputsMapFunction)
        .createRunnerForPTransform(context);

    assertThat(context.getStartBundleFunctions(), empty());
    assertThat(context.getFinishBundleFunctions(), empty());

    // Send elements to runner and check outputs.
    mainOutputValues.clear();
    assertThat(
        context.getPCollectionConsumers().keySet(),
        containsInAnyOrder(inputPCollectionId, outputPCollectionId));

    FnDataReceiver<WindowedValue<?>> input = context.getPCollectionConsumer(inputPCollectionId);
    input.accept(valueInGlobalWindow(KV.of("A", 9)));
    input.accept(valueInGlobalWindow(KV.of("B", 5)));
    input.accept(valueInGlobalWindow(KV.of("C", 7)));

    assertThat(
        mainOutputValues,
        contains(
            valueInGlobalWindow(KV.of("A", -9)),
            valueInGlobalWindow(KV.of("B", -5)),
            valueInGlobalWindow(KV.of("C", -7))));
  }

  /**
   * Create a Convert To Accumulators function that is given keyed accumulators and validates that
   * the input values were turned into the accumulator type.
   */
  @Test
  public void testConvertToAccumulators() throws Exception {
    PTransformRunnerFactoryTestContext context =
        PTransformRunnerFactoryTestContext.builder(TEST_COMBINE_ID, pTransform)
            .pCollections(pProto.getComponents().getPcollectionsMap())
            .coders(pProto.getComponents().getCodersMap())
            .build();
    // Add a consumer and output target to check output values.
    Deque<WindowedValue<KV<String, Integer>>> mainOutputValues = new ArrayDeque<>();
    context.addPCollectionConsumer(
        Iterables.getOnlyElement(pTransform.getOutputsMap().values()),
        (FnDataReceiver)
            (FnDataReceiver<WindowedValue<KV<String, Integer>>>) mainOutputValues::add);

    // Create runner.
    MapFnRunners.forValueMapFnFactory(CombineRunners::createConvertToAccumulatorsMapFunction)
        .createRunnerForPTransform(context);

    assertThat(context.getStartBundleFunctions(), empty());
    assertThat(context.getFinishBundleFunctions(), empty());

    // Send elements to runner and check outputs.
    mainOutputValues.clear();
    assertThat(
        context.getPCollectionConsumers().keySet(),
        containsInAnyOrder(inputPCollectionId, outputPCollectionId));

    FnDataReceiver<WindowedValue<?>> input = context.getPCollectionConsumer(inputPCollectionId);
    input.accept(valueInGlobalWindow(KV.of("A", "9")));
    input.accept(valueInGlobalWindow(KV.of("B", "5")));
    input.accept(valueInGlobalWindow(KV.of("C", "7")));

    assertThat(
        mainOutputValues,
        contains(
            valueInGlobalWindow(KV.of("A", 9)),
            valueInGlobalWindow(KV.of("B", 5)),
            valueInGlobalWindow(KV.of("C", 7))));
  }
  /**
   * Create a Combine Grouped Values function that is given lists of values that are grouped by key
   * and validates that the lists are properly combined.
   */
  @Test
  public void testCombineGroupedValues() throws Exception {
    // Create coder map for size estimation
    RunnerApi.PCollection pCollection =
        RunnerApi.PCollection.newBuilder()
            .setUniqueName(inputPCollectionId)
            .setCoderId("coder-id")
            .build();
    Map<String, RunnerApi.PCollection> pCollectionMap =
        new HashMap<>(pProto.getComponents().getPcollectionsMap());
    pCollectionMap.put(inputPCollectionId, pCollection);
    Map<String, RunnerApi.Coder> coderMap = new HashMap<>(pProto.getComponents().getCodersMap());
    coderMap.put(
        "coder-id",
        RunnerApi.Coder.newBuilder()
            .setSpec(RunnerApi.FunctionSpec.newBuilder().setUrn(ModelCoders.KV_CODER_URN).build())
            .addComponentCoderIds("StringUtf8Coder")
            .addComponentCoderIds("coder-id-iterable")
            .build());
    coderMap.put(
        "coder-id-iterable",
        RunnerApi.Coder.newBuilder()
            .setSpec(
                RunnerApi.FunctionSpec.newBuilder().setUrn(ModelCoders.ITERABLE_CODER_URN).build())
            .addComponentCoderIds("StringUtf8Coder")
            .build());
    PTransformRunnerFactoryTestContext context =
        PTransformRunnerFactoryTestContext.builder(TEST_COMBINE_ID, pTransform)
            .pCollections(pCollectionMap)
            .coders(coderMap)
            .build();
    // Add a consumer and output target to check output values.
    Deque<WindowedValue<KV<String, Integer>>> mainOutputValues = new ArrayDeque<>();
    context.addPCollectionConsumer(
        Iterables.getOnlyElement(pTransform.getOutputsMap().values()),
        (FnDataReceiver)
            (FnDataReceiver<WindowedValue<KV<String, Integer>>>) mainOutputValues::add);

    // Create runner.
    MapFnRunners.forValueMapFnFactory(CombineRunners::createCombineGroupedValuesMapFunction)
        .createRunnerForPTransform(context);

    assertThat(context.getStartBundleFunctions(), empty());
    assertThat(context.getFinishBundleFunctions(), empty());

    // Send elements to runner and check outputs.
    mainOutputValues.clear();
    assertThat(
        context.getPCollectionConsumers().keySet(),
        containsInAnyOrder(inputPCollectionId, outputPCollectionId));

    FnDataReceiver<WindowedValue<?>> input = context.getPCollectionConsumer(inputPCollectionId);
    input.accept(valueInGlobalWindow(KV.of("A", Arrays.asList("1", "2", "6"))));
    input.accept(valueInGlobalWindow(KV.of("B", Arrays.asList("2", "3"))));
    input.accept(valueInGlobalWindow(KV.of("C", Arrays.asList("5", "2"))));

    assertThat(
        mainOutputValues,
        contains(
            valueInGlobalWindow(KV.of("A", -9)),
            valueInGlobalWindow(KV.of("B", -5)),
            valueInGlobalWindow(KV.of("C", -7))));
  }
}
