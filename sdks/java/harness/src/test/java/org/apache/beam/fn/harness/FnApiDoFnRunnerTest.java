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

import static java.util.Arrays.asList;
import static org.apache.beam.sdk.options.ExperimentalOptions.addExperiment;
import static org.apache.beam.sdk.util.WindowedValue.timestampedValueInGlobalWindow;
import static org.apache.beam.sdk.util.WindowedValue.valueInGlobalWindow;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.apache.beam.fn.harness.FnApiDoFnRunner.SplitResultsWithStopIndex;
import org.apache.beam.fn.harness.FnApiDoFnRunner.WindowedSplitResult;
import org.apache.beam.fn.harness.HandlesSplits.SplitResult;
import org.apache.beam.fn.harness.control.BundleProgressReporter;
import org.apache.beam.fn.harness.control.BundleSplitListener;
import org.apache.beam.fn.harness.state.FakeBeamFnStateClient;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.BundleApplication;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.DelayedBundleApplication;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfo;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.runners.core.metrics.DistributionData;
import org.apache.beam.runners.core.metrics.MetricUpdates.MetricUpdate;
import org.apache.beam.runners.core.metrics.MetricsContainerImpl;
import org.apache.beam.runners.core.metrics.MetricsContainerStepMap;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants.Urns;
import org.apache.beam.runners.core.metrics.ShortIdMap;
import org.apache.beam.runners.core.metrics.SimpleMonitoringInfoBuilder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.DoubleCoder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.fn.data.BeamFnDataOutboundAggregator;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.fn.data.LogicalEndpoint;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.MetricKey;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.CombiningState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerMap;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.testing.ResetDateTimeProvider;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker.Progress;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker.TruncateResult;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimators;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.construction.CoderTranslation;
import org.apache.beam.sdk.util.construction.CoderTranslation.TranslationContext;
import org.apache.beam.sdk.util.construction.PTransformTranslation;
import org.apache.beam.sdk.util.construction.ParDoTranslation;
import org.apache.beam.sdk.util.construction.PipelineTranslation;
import org.apache.beam.sdk.util.construction.RehydratedComponents;
import org.apache.beam.sdk.util.construction.SdkComponents;
import org.apache.beam.sdk.util.construction.graph.ProtoOverrides;
import org.apache.beam.sdk.util.construction.graph.SplittableParDoExpander;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.util.Durations;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.hamcrest.collection.IsMapContaining;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.PeriodFormat;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link FnApiDoFnRunner}. */
@RunWith(Enclosed.class)
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  // TODO(https://github.com/apache/beam/issues/21230): Remove when new version of
  // errorprone is released (2.11.0)
  "unused"
})
public class FnApiDoFnRunnerTest implements Serializable {

  @RunWith(JUnit4.class)
  public static class ExecutionTest implements Serializable {
    @Rule public transient ResetDateTimeProvider dateTimeProvider = new ResetDateTimeProvider();

    public static final String TEST_TRANSFORM_ID = "pTransformId";

    private static class ConcatCombineFn extends CombineFn<String, String, String> {
      @Override
      public String createAccumulator() {
        return "";
      }

      @Override
      public String addInput(String accumulator, String input) {
        return accumulator.concat(input);
      }

      @Override
      public String mergeAccumulators(Iterable<String> accumulators) {
        StringBuilder builder = new StringBuilder();
        for (String value : accumulators) {
          builder.append(value);
        }
        return builder.toString();
      }

      @Override
      public String extractOutput(String accumulator) {
        return accumulator;
      }
    }

    private static class TestStatefulDoFn extends DoFn<KV<String, String>, String> {

      @StateId("value")
      private final StateSpec<ValueState<String>> valueStateSpec =
          StateSpecs.value(StringUtf8Coder.of());

      @StateId("bag")
      private final StateSpec<BagState<String>> bagStateSpec = StateSpecs.bag(StringUtf8Coder.of());

      @StateId("combine")
      private final StateSpec<CombiningState<String, String, String>> combiningStateSpec =
          StateSpecs.combining(StringUtf8Coder.of(), new ConcatCombineFn());

      @ProcessElement
      public void processElement(
          ProcessContext context,
          @StateId("value") ValueState<String> valueState,
          @StateId("bag") BagState<String> bagState,
          @StateId("combine") CombiningState<String, String, String> combiningState) {
        context.output("value:" + valueState.read());
        valueState.write(context.element().getValue());

        context.output("bag:" + Iterables.toString(bagState.read()));
        bagState.add(context.element().getValue());

        context.output("combine:" + combiningState.read());
        combiningState.add(context.element().getValue());
      }
    }

    @Test
    public void testUsingUserState() throws Exception {
      Pipeline p = Pipeline.create();
      PCollection<KV<String, String>> valuePCollection =
          p.apply(Create.of(KV.of("unused", "unused")));
      PCollection<String> outputPCollection =
          valuePCollection.apply(TEST_TRANSFORM_ID, ParDo.of(new TestStatefulDoFn()));

      SdkComponents sdkComponents = SdkComponents.create(p.getOptions());
      RunnerApi.Pipeline pProto = PipelineTranslation.toProto(p, sdkComponents);
      String inputPCollectionId = sdkComponents.registerPCollection(valuePCollection);
      String outputPCollectionId = sdkComponents.registerPCollection(outputPCollection);
      RunnerApi.PTransform pTransform =
          pProto
              .getComponents()
              .getTransformsOrThrow(
                  pProto
                      .getComponents()
                      .getTransformsOrThrow(TEST_TRANSFORM_ID)
                      .getSubtransforms(0));

      FakeBeamFnStateClient fakeClient =
          new FakeBeamFnStateClient(
              StringUtf8Coder.of(),
              ImmutableMap.of(
                  bagUserStateKey("value", "X"), asList("X0"),
                  bagUserStateKey("bag", "X"), asList("X0"),
                  bagUserStateKey("combine", "X"), asList("X0")));

      PTransformRunnerFactoryTestContext context =
          PTransformRunnerFactoryTestContext.builder(TEST_TRANSFORM_ID, pTransform)
              .beamFnStateClient(fakeClient)
              .processBundleInstructionId("57")
              .pCollections(pProto.getComponents().getPcollectionsMap())
              .coders(pProto.getComponents().getCodersMap())
              .windowingStrategies(pProto.getComponents().getWindowingStrategiesMap())
              .build();
      List<WindowedValue<String>> mainOutputValues = new ArrayList<>();
      context.addPCollectionConsumer(
          outputPCollectionId,
          (FnDataReceiver) (FnDataReceiver<WindowedValue<String>>) mainOutputValues::add);

      new FnApiDoFnRunner.Factory<>().createRunnerForPTransform(context);

      Iterables.getOnlyElement(context.getStartBundleFunctions()).run();
      mainOutputValues.clear();

      assertThat(
          context.getPCollectionConsumers().keySet(),
          containsInAnyOrder(inputPCollectionId, outputPCollectionId));

      // Ensure that bag user state that is initially empty or populated works.
      // Ensure that the key order does not matter when we traverse over KV pairs.
      FnDataReceiver<WindowedValue<?>> mainInput =
          context.getPCollectionConsumer(inputPCollectionId);
      mainInput.accept(valueInGlobalWindow(KV.of("X", "X1")));
      mainInput.accept(valueInGlobalWindow(KV.of("Y", "Y1")));
      mainInput.accept(valueInGlobalWindow(KV.of("X", "X2")));
      mainInput.accept(valueInGlobalWindow(KV.of("Y", "Y2")));
      assertThat(
          mainOutputValues,
          contains(
              valueInGlobalWindow("value:X0"),
              valueInGlobalWindow("bag:[X0]"),
              valueInGlobalWindow("combine:X0"),
              valueInGlobalWindow("value:null"),
              valueInGlobalWindow("bag:[]"),
              valueInGlobalWindow("combine:"),
              valueInGlobalWindow("value:X1"),
              valueInGlobalWindow("bag:[X0, X1]"),
              valueInGlobalWindow("combine:X0X1"),
              valueInGlobalWindow("value:Y1"),
              valueInGlobalWindow("bag:[Y1]"),
              valueInGlobalWindow("combine:Y1")));
      mainOutputValues.clear();

      Iterables.getOnlyElement(context.getFinishBundleFunctions()).run();
      assertThat(mainOutputValues, empty());

      Iterables.getOnlyElement(context.getTearDownFunctions()).run();
      assertThat(mainOutputValues, empty());

      assertEquals(
          new FakeBeamFnStateClient(
                  StringUtf8Coder.of(),
                  ImmutableMap.<StateKey, List<String>>builder()
                      .put(bagUserStateKey("value", "X"), asList("X2"))
                      .put(bagUserStateKey("bag", "X"), asList("X0", "X1", "X2"))
                      .put(bagUserStateKey("combine", "X"), asList("X0X1X2"))
                      .put(bagUserStateKey("value", "Y"), asList("Y2"))
                      .put(bagUserStateKey("bag", "Y"), asList("Y1", "Y2"))
                      .put(bagUserStateKey("combine", "Y"), asList("Y1Y2"))
                      .build())
              .getData(),
          fakeClient.getData());
    }

    /** Produces a bag user {@link StateKey} for the test PTransform id in the global window. */
    private StateKey bagUserStateKey(String userStateId, String key) throws IOException {
      return StateKey.newBuilder()
          .setBagUserState(
              StateKey.BagUserState.newBuilder()
                  .setTransformId(TEST_TRANSFORM_ID)
                  .setUserStateId(userStateId)
                  .setKey(encode(key))
                  .setWindow(
                      ByteString.copyFrom(
                          CoderUtils.encodeToByteArray(
                              GlobalWindow.Coder.INSTANCE, GlobalWindow.INSTANCE))))
          .build();
    }

    private static class TestSideInputDoFn extends DoFn<String, String> {
      private final PCollectionView<String> defaultSingletonSideInput;
      private final PCollectionView<String> singletonSideInput;
      private final PCollectionView<Iterable<String>> iterableSideInput;
      private final TupleTag<String> additionalOutput;

      private TestSideInputDoFn(
          PCollectionView<String> defaultSingletonSideInput,
          PCollectionView<String> singletonSideInput,
          PCollectionView<Iterable<String>> iterableSideInput,
          TupleTag<String> additionalOutput) {
        this.defaultSingletonSideInput = defaultSingletonSideInput;
        this.singletonSideInput = singletonSideInput;
        this.iterableSideInput = iterableSideInput;
        this.additionalOutput = additionalOutput;
      }

      @ProcessElement
      public void processElement(ProcessContext context) {
        context.output(context.element() + ":" + context.sideInput(defaultSingletonSideInput));
        context.output(context.element() + ":" + context.sideInput(singletonSideInput));
        for (String sideInputValue : context.sideInput(iterableSideInput)) {
          context.output(context.element() + ":" + sideInputValue);
        }
        context.output(additionalOutput, context.element() + ":additional");
      }
    }

    @Test
    public void testProcessElementWithSideInputsAndOutputs() throws Exception {
      Pipeline p = Pipeline.create();
      addExperiment(p.getOptions().as(ExperimentalOptions.class), "beam_fn_api");
      // TODO(BEAM-10097): Remove experiment once all portable runners support this view type
      addExperiment(p.getOptions().as(ExperimentalOptions.class), "use_runner_v2");
      PCollection<String> valuePCollection = p.apply(Create.of("unused"));
      PCollectionView<String> defaultSingletonSideInputView =
          valuePCollection.apply(
              View.<String>asSingleton().withDefaultValue("defaultSingletonValue"));
      PCollectionView<String> singletonSideInputView = valuePCollection.apply(View.asSingleton());
      PCollectionView<Iterable<String>> iterableSideInputView =
          valuePCollection.apply(View.asIterable());
      TupleTag<String> mainOutput = new TupleTag<String>("main") {};
      TupleTag<String> additionalOutput = new TupleTag<String>("additional") {};
      PCollectionTuple outputPCollection =
          valuePCollection.apply(
              TEST_TRANSFORM_ID,
              ParDo.of(
                      new TestSideInputDoFn(
                          defaultSingletonSideInputView,
                          singletonSideInputView,
                          iterableSideInputView,
                          additionalOutput))
                  .withSideInputs(
                      defaultSingletonSideInputView, singletonSideInputView, iterableSideInputView)
                  .withOutputTags(mainOutput, TupleTagList.of(additionalOutput)));

      SdkComponents sdkComponents = SdkComponents.create(p.getOptions());
      RunnerApi.Pipeline pProto = PipelineTranslation.toProto(p, sdkComponents, true);
      String inputPCollectionId = sdkComponents.registerPCollection(valuePCollection);
      String outputPCollectionId =
          sdkComponents.registerPCollection(outputPCollection.get(mainOutput));
      String additionalPCollectionId =
          sdkComponents.registerPCollection(outputPCollection.get(additionalOutput));

      RunnerApi.PTransform pTransform =
          pProto.getComponents().getTransformsOrThrow(TEST_TRANSFORM_ID);

      ImmutableMap<StateKey, List<String>> stateData =
          ImmutableMap.of(
              iterableSideInputKey(singletonSideInputView.getTagInternal().getId()),
              asList("singletonValue"),
              iterableSideInputKey(iterableSideInputView.getTagInternal().getId()),
              asList("iterableValue1", "iterableValue2", "iterableValue3"));

      FakeBeamFnStateClient fakeClient =
          new FakeBeamFnStateClient(StringUtf8Coder.of(), stateData, 1000);

      PTransformRunnerFactoryTestContext context =
          PTransformRunnerFactoryTestContext.builder(TEST_TRANSFORM_ID, pTransform)
              .beamFnStateClient(fakeClient)
              .processBundleInstructionId("57")
              .pCollections(pProto.getComponents().getPcollectionsMap())
              .coders(pProto.getComponents().getCodersMap())
              .windowingStrategies(pProto.getComponents().getWindowingStrategiesMap())
              .build();
      List<WindowedValue<String>> mainOutputValues = new ArrayList<>();
      List<WindowedValue<String>> additionalOutputValues = new ArrayList<>();
      context.addPCollectionConsumer(
          outputPCollectionId,
          (FnDataReceiver) (FnDataReceiver<WindowedValue<String>>) mainOutputValues::add);
      context.addPCollectionConsumer(
          additionalPCollectionId,
          (FnDataReceiver) (FnDataReceiver<WindowedValue<String>>) additionalOutputValues::add);

      new FnApiDoFnRunner.Factory<>().createRunnerForPTransform(context);

      Iterables.getOnlyElement(context.getStartBundleFunctions()).run();
      mainOutputValues.clear();

      assertThat(
          context.getPCollectionConsumers().keySet(),
          containsInAnyOrder(inputPCollectionId, outputPCollectionId, additionalPCollectionId));

      // Ensure that bag user state that is initially empty or populated works.
      // Ensure that the bagUserStateKey order does not matter when we traverse over KV pairs.
      FnDataReceiver<WindowedValue<?>> mainInput =
          context.getPCollectionConsumer(inputPCollectionId);
      mainInput.accept(valueInGlobalWindow("X"));
      mainInput.accept(valueInGlobalWindow("Y"));
      assertThat(
          mainOutputValues,
          contains(
              valueInGlobalWindow("X:defaultSingletonValue"),
              valueInGlobalWindow("X:singletonValue"),
              valueInGlobalWindow("X:iterableValue1"),
              valueInGlobalWindow("X:iterableValue2"),
              valueInGlobalWindow("X:iterableValue3"),
              valueInGlobalWindow("Y:defaultSingletonValue"),
              valueInGlobalWindow("Y:singletonValue"),
              valueInGlobalWindow("Y:iterableValue1"),
              valueInGlobalWindow("Y:iterableValue2"),
              valueInGlobalWindow("Y:iterableValue3")));
      assertThat(
          additionalOutputValues,
          contains(valueInGlobalWindow("X:additional"), valueInGlobalWindow("Y:additional")));
      mainOutputValues.clear();

      Iterables.getOnlyElement(context.getFinishBundleFunctions()).run();
      assertThat(mainOutputValues, empty());

      Iterables.getOnlyElement(context.getTearDownFunctions()).run();
      assertThat(mainOutputValues, empty());

      // Assert that state data did not change
      assertEquals(
          new FakeBeamFnStateClient(StringUtf8Coder.of(), stateData).getData(),
          fakeClient.getData());
    }

    private static class TestNonWindowObservingDoFn extends DoFn<String, String> {
      private final TupleTag<String> additionalOutput;

      private TestNonWindowObservingDoFn(TupleTag<String> additionalOutput) {
        this.additionalOutput = additionalOutput;
      }

      @ProcessElement
      public void processElement(ProcessContext context) {
        context.output(context.element() + ":main");
        context.output(additionalOutput, context.element() + ":additional");
      }
    }

    @Test
    public void testProcessElementWithNonWindowObservingOptimization() throws Exception {
      Pipeline p = Pipeline.create();
      addExperiment(p.getOptions().as(ExperimentalOptions.class), "beam_fn_api");
      // TODO(BEAM-10097): Remove experiment once all portable runners support this view type
      addExperiment(p.getOptions().as(ExperimentalOptions.class), "use_runner_v2");
      PCollection<String> valuePCollection =
          p.apply(Create.of("unused"))
              .apply(Window.into(FixedWindows.of(Duration.standardMinutes(1))));
      TupleTag<String> mainOutput = new TupleTag<String>("main") {};
      TupleTag<String> additionalOutput = new TupleTag<String>("additional") {};
      PCollectionTuple outputPCollection =
          valuePCollection.apply(
              TEST_TRANSFORM_ID,
              ParDo.of(new TestNonWindowObservingDoFn(additionalOutput))
                  .withOutputTags(mainOutput, TupleTagList.of(additionalOutput)));

      SdkComponents sdkComponents = SdkComponents.create(p.getOptions());
      RunnerApi.Pipeline pProto = PipelineTranslation.toProto(p, sdkComponents, true);
      String inputPCollectionId = sdkComponents.registerPCollection(valuePCollection);
      String outputPCollectionId =
          sdkComponents.registerPCollection(outputPCollection.get(mainOutput));
      String additionalPCollectionId =
          sdkComponents.registerPCollection(outputPCollection.get(additionalOutput));

      RunnerApi.PTransform pTransform =
          pProto.getComponents().getTransformsOrThrow(TEST_TRANSFORM_ID);

      PTransformRunnerFactoryTestContext context =
          PTransformRunnerFactoryTestContext.builder(TEST_TRANSFORM_ID, pTransform)
              .processBundleInstructionId("57")
              .pCollections(pProto.getComponents().getPcollectionsMap())
              .coders(pProto.getComponents().getCodersMap())
              .windowingStrategies(pProto.getComponents().getWindowingStrategiesMap())
              .build();
      List<WindowedValue<String>> mainOutputValues = new ArrayList<>();
      List<WindowedValue<String>> additionalOutputValues = new ArrayList<>();
      context.addPCollectionConsumer(
          outputPCollectionId,
          (FnDataReceiver) (FnDataReceiver<WindowedValue<String>>) mainOutputValues::add);
      context.addPCollectionConsumer(
          additionalPCollectionId,
          (FnDataReceiver) (FnDataReceiver<WindowedValue<String>>) additionalOutputValues::add);

      new FnApiDoFnRunner.Factory<>().createRunnerForPTransform(context);

      Iterables.getOnlyElement(context.getStartBundleFunctions()).run();
      mainOutputValues.clear();

      assertThat(
          context.getPCollectionConsumers().keySet(),
          containsInAnyOrder(inputPCollectionId, outputPCollectionId, additionalPCollectionId));

      FnDataReceiver<WindowedValue<?>> mainInput =
          context.getPCollectionConsumer(inputPCollectionId);
      mainInput.accept(
          valueInWindows(
              "X",
              new IntervalWindow(new Instant(0L), Duration.standardMinutes(1)),
              new IntervalWindow(new Instant(10L), Duration.standardMinutes(1))));
      mainInput.accept(
          valueInWindows(
              "Y",
              new IntervalWindow(new Instant(1000L), Duration.standardMinutes(1)),
              new IntervalWindow(new Instant(1010L), Duration.standardMinutes(1))));
      // Ensure that each output element is in all the windows and not one per window.
      assertThat(
          mainOutputValues,
          contains(
              valueInWindows(
                  "X:main",
                  new IntervalWindow(new Instant(0L), Duration.standardMinutes(1)),
                  new IntervalWindow(new Instant(10L), Duration.standardMinutes(1))),
              valueInWindows(
                  "Y:main",
                  new IntervalWindow(new Instant(1000L), Duration.standardMinutes(1)),
                  new IntervalWindow(new Instant(1010L), Duration.standardMinutes(1)))));
      assertThat(
          additionalOutputValues,
          contains(
              valueInWindows(
                  "X:additional",
                  new IntervalWindow(new Instant(0L), Duration.standardMinutes(1)),
                  new IntervalWindow(new Instant(10L), Duration.standardMinutes(1))),
              valueInWindows(
                  "Y:additional",
                  new IntervalWindow(new Instant(1000L), Duration.standardMinutes(1)),
                  new IntervalWindow(new Instant(1010L), Duration.standardMinutes(1)))));
      mainOutputValues.clear();

      Iterables.getOnlyElement(context.getFinishBundleFunctions()).run();
      assertThat(mainOutputValues, empty());

      Iterables.getOnlyElement(context.getTearDownFunctions()).run();
      assertThat(mainOutputValues, empty());
    }

    private static class TestSideInputIsAccessibleForDownstreamCallersDoFn
        extends DoFn<String, Iterable<String>> {
      public static final String USER_COUNTER_NAME = "userCountedElems";
      private final Counter countedElements =
          Metrics.counter(
              TestSideInputIsAccessibleForDownstreamCallersDoFn.class, USER_COUNTER_NAME);

      private final PCollectionView<Iterable<String>> iterableSideInput;

      private TestSideInputIsAccessibleForDownstreamCallersDoFn(
          PCollectionView<Iterable<String>> iterableSideInput) {
        this.iterableSideInput = iterableSideInput;
      }

      @ProcessElement
      public void processElement(ProcessContext context) {
        countedElements.inc();
        context.output(context.sideInput(iterableSideInput));
      }
    }

    @Test
    public void testSideInputIsAccessibleForDownstreamCallers() throws Exception {
      FixedWindows windowFn = FixedWindows.of(Duration.millis(1L));
      IntervalWindow windowA = windowFn.assignWindow(new Instant(1L));
      IntervalWindow windowB = windowFn.assignWindow(new Instant(2L));
      ByteString encodedWindowA =
          ByteString.copyFrom(CoderUtils.encodeToByteArray(windowFn.windowCoder(), windowA));
      ByteString encodedWindowB =
          ByteString.copyFrom(CoderUtils.encodeToByteArray(windowFn.windowCoder(), windowB));

      Pipeline p = Pipeline.create();
      addExperiment(p.getOptions().as(ExperimentalOptions.class), "beam_fn_api");
      // TODO(BEAM-10097): Remove experiment once all portable runners support this view type
      addExperiment(p.getOptions().as(ExperimentalOptions.class), "use_runner_v2");
      PCollection<String> valuePCollection =
          p.apply(Create.of("unused")).apply(Window.into(windowFn));
      PCollectionView<Iterable<String>> iterableSideInputView =
          valuePCollection.apply(View.asIterable());
      PCollection<Iterable<String>> outputPCollection =
          valuePCollection.apply(
              TEST_TRANSFORM_ID,
              ParDo.of(new TestSideInputIsAccessibleForDownstreamCallersDoFn(iterableSideInputView))
                  .withSideInputs(iterableSideInputView));

      SdkComponents sdkComponents = SdkComponents.create(p.getOptions());
      RunnerApi.Pipeline pProto = PipelineTranslation.toProto(p, sdkComponents, true);
      String inputPCollectionId = sdkComponents.registerPCollection(valuePCollection);
      String outputPCollectionId = sdkComponents.registerPCollection(outputPCollection);

      RunnerApi.PTransform pTransform =
          pProto
              .getComponents()
              .getTransformsOrThrow(
                  pProto
                      .getComponents()
                      .getTransformsOrThrow(TEST_TRANSFORM_ID)
                      .getSubtransforms(0));

      ImmutableMap<StateKey, List<String>> stateData =
          ImmutableMap.of(
              iterableSideInputKey(iterableSideInputView.getTagInternal().getId(), encodedWindowA),
              asList("iterableValue1A", "iterableValue2A", "iterableValue3A"),
              iterableSideInputKey(iterableSideInputView.getTagInternal().getId(), encodedWindowB),
              asList("iterableValue1B", "iterableValue2B", "iterableValue3B"));

      FakeBeamFnStateClient fakeClient =
          new FakeBeamFnStateClient(StringUtf8Coder.of(), stateData, 1000);

      PTransformRunnerFactoryTestContext context =
          PTransformRunnerFactoryTestContext.builder(TEST_TRANSFORM_ID, pTransform)
              .beamFnStateClient(fakeClient)
              .processBundleInstructionId("57")
              .pCollections(pProto.getComponents().getPcollectionsMap())
              .coders(pProto.getComponents().getCodersMap())
              .windowingStrategies(pProto.getComponents().getWindowingStrategiesMap())
              .build();
      List<WindowedValue<Iterable<String>>> mainOutputValues = new ArrayList<>();
      context.addPCollectionConsumer(
          Iterables.getOnlyElement(pTransform.getOutputsMap().values()),
          (FnDataReceiver) (FnDataReceiver<WindowedValue<Iterable<String>>>) mainOutputValues::add);

      new FnApiDoFnRunner.Factory<>().createRunnerForPTransform(context);

      Iterables.getOnlyElement(context.getStartBundleFunctions()).run();
      mainOutputValues.clear();

      assertThat(
          context.getPCollectionConsumers().keySet(),
          containsInAnyOrder(inputPCollectionId, outputPCollectionId));

      // Ensure that bag user state that is initially empty or populated works.
      // Ensure that the bagUserStateKey order does not matter when we traverse over KV pairs.
      FnDataReceiver<WindowedValue<?>> mainInput =
          context.getPCollectionConsumer(inputPCollectionId);
      mainInput.accept(valueInWindows("X", windowA));
      mainInput.accept(valueInWindows("Y", windowB));
      assertThat(mainOutputValues, hasSize(2));
      assertThat(
          mainOutputValues.get(0).getValue(),
          contains("iterableValue1A", "iterableValue2A", "iterableValue3A"));
      assertThat(
          mainOutputValues.get(1).getValue(),
          contains("iterableValue1B", "iterableValue2B", "iterableValue3B"));
      mainOutputValues.clear();

      Iterables.getOnlyElement(context.getFinishBundleFunctions()).run();
      assertThat(mainOutputValues, empty());

      Iterables.getOnlyElement(context.getTearDownFunctions()).run();
      assertThat(mainOutputValues, empty());

      // Assert that state data did not change
      assertEquals(
          new FakeBeamFnStateClient(StringUtf8Coder.of(), stateData).getData(),
          fakeClient.getData());
    }

    /** @return a test MetricUpdate for expected metrics to compare against */
    public MetricUpdate create(String stepName, MetricName name, long value) {
      return MetricUpdate.create(MetricKey.create(stepName, name), value);
    }

    @Test
    @Ignore("https://github.com/apache/beam/issues/20872")
    public void testUsingMetrics() throws Exception {
      MetricsContainerStepMap metricsContainerRegistry = new MetricsContainerStepMap();
      MetricsContainerImpl metricsContainer = metricsContainerRegistry.getUnboundContainer();
      Closeable closeable = MetricsEnvironment.scopedMetricsContainer(metricsContainer);
      FixedWindows windowFn = FixedWindows.of(Duration.millis(1L));
      IntervalWindow windowA = windowFn.assignWindow(new Instant(1L));
      IntervalWindow windowB = windowFn.assignWindow(new Instant(2L));
      ByteString encodedWindowA =
          ByteString.copyFrom(CoderUtils.encodeToByteArray(windowFn.windowCoder(), windowA));
      ByteString encodedWindowB =
          ByteString.copyFrom(CoderUtils.encodeToByteArray(windowFn.windowCoder(), windowB));

      Pipeline p = Pipeline.create();
      PCollection<String> valuePCollection =
          p.apply(Create.of("unused")).apply(Window.into(windowFn));
      PCollectionView<Iterable<String>> iterableSideInputView =
          valuePCollection.apply(View.asIterable());
      PCollection<Iterable<String>> outputPCollection =
          valuePCollection.apply(
              TEST_TRANSFORM_ID,
              ParDo.of(new TestSideInputIsAccessibleForDownstreamCallersDoFn(iterableSideInputView))
                  .withSideInputs(iterableSideInputView));

      SdkComponents sdkComponents = SdkComponents.create(p.getOptions());
      RunnerApi.Pipeline pProto = PipelineTranslation.toProto(p, sdkComponents, true);
      String inputPCollectionId = sdkComponents.registerPCollection(valuePCollection);
      String outputPCollectionId = sdkComponents.registerPCollection(outputPCollection);

      RunnerApi.PTransform pTransform =
          pProto
              .getComponents()
              .getTransformsOrThrow(
                  pProto
                      .getComponents()
                      .getTransformsOrThrow(TEST_TRANSFORM_ID)
                      .getSubtransforms(0));

      ImmutableMap<StateKey, List<String>> stateData =
          ImmutableMap.of(
              iterableSideInputKey(iterableSideInputView.getTagInternal().getId(), encodedWindowA),
              asList("iterableValue1A", "iterableValue2A", "iterableValue3A"),
              iterableSideInputKey(iterableSideInputView.getTagInternal().getId(), encodedWindowB),
              asList("iterableValue1B", "iterableValue2B", "iterableValue3B"));

      FakeBeamFnStateClient fakeClient = new FakeBeamFnStateClient(StringUtf8Coder.of(), stateData);

      PTransformRunnerFactoryTestContext context =
          PTransformRunnerFactoryTestContext.builder(TEST_TRANSFORM_ID, pTransform)
              .beamFnStateClient(fakeClient)
              .processBundleInstructionId("57")
              .pCollections(pProto.getComponents().getPcollectionsMap())
              .coders(pProto.getComponents().getCodersMap())
              .windowingStrategies(pProto.getComponents().getWindowingStrategiesMap())
              .build();
      List<WindowedValue<Iterable<String>>> mainOutputValues = new ArrayList<>();
      context.addPCollectionConsumer(
          Iterables.getOnlyElement(pTransform.getOutputsMap().values()),
          (FnDataReceiver) (FnDataReceiver<WindowedValue<Iterable<String>>>) mainOutputValues::add);

      new FnApiDoFnRunner.Factory<>().createRunnerForPTransform(context);

      Iterables.getOnlyElement(context.getStartBundleFunctions()).run();
      mainOutputValues.clear();

      assertThat(
          context.getPCollectionConsumers().keySet(),
          containsInAnyOrder(inputPCollectionId, outputPCollectionId));

      // Ensure that bag user state that is initially empty or populated works.
      // Ensure that the bagUserStateKey order does not matter when we traverse over KV pairs.
      FnDataReceiver<WindowedValue<?>> mainInput =
          context.getPCollectionConsumer(inputPCollectionId);
      mainInput.accept(valueInWindows("X", windowA));
      mainInput.accept(valueInWindows("Y", windowB));
      mainOutputValues.clear();

      Iterables.getOnlyElement(context.getFinishBundleFunctions()).run();
      assertThat(mainOutputValues, empty());

      Iterables.getOnlyElement(context.getTearDownFunctions()).run();
      assertThat(mainOutputValues, empty());

      List<MonitoringInfo> expected = new ArrayList<MonitoringInfo>();
      SimpleMonitoringInfoBuilder builder = new SimpleMonitoringInfoBuilder();
      builder.setUrn(MonitoringInfoConstants.Urns.ELEMENT_COUNT);
      builder.setLabel(
          MonitoringInfoConstants.Labels.PCOLLECTION, "Window.Into()/Window.Assign.out");
      builder.setInt64SumValue(2);
      expected.add(builder.build());

      builder = new SimpleMonitoringInfoBuilder();
      builder.setUrn(MonitoringInfoConstants.Urns.ELEMENT_COUNT);
      builder.setLabel(
          MonitoringInfoConstants.Labels.PCOLLECTION,
          "pTransformId/ParMultiDo(TestSideInputIsAccessibleForDownstreamCallers).output");
      builder.setInt64SumValue(2);
      expected.add(builder.build());

      builder = new SimpleMonitoringInfoBuilder();
      builder
          .setUrn(MonitoringInfoConstants.Urns.USER_SUM_INT64)
          .setLabel(
              MonitoringInfoConstants.Labels.NAMESPACE,
              TestSideInputIsAccessibleForDownstreamCallersDoFn.class.getName())
          .setLabel(
              MonitoringInfoConstants.Labels.NAME,
              TestSideInputIsAccessibleForDownstreamCallersDoFn.USER_COUNTER_NAME);
      builder.setLabel(MonitoringInfoConstants.Labels.PTRANSFORM, TEST_TRANSFORM_ID);
      builder.setInt64SumValue(2);
      expected.add(builder.build());

      builder = new SimpleMonitoringInfoBuilder();
      builder.setUrn(MonitoringInfoConstants.Urns.SAMPLED_BYTE_SIZE);
      builder.setLabel(
          MonitoringInfoConstants.Labels.PCOLLECTION, "Window.Into()/Window.Assign.out");
      builder.setInt64DistributionValue(DistributionData.create(4, 2, 2, 2));
      expected.add(builder.build());

      builder = new SimpleMonitoringInfoBuilder();
      builder.setUrn(Urns.SAMPLED_BYTE_SIZE);
      builder.setLabel(
          MonitoringInfoConstants.Labels.PCOLLECTION,
          "pTransformId/ParMultiDo(TestSideInputIsAccessibleForDownstreamCallers).output");
      builder.setInt64DistributionValue(DistributionData.create(10, 2, 5, 5));
      expected.add(builder.build());

      closeable.close();
      List<MonitoringInfo> result = new ArrayList<MonitoringInfo>();
      for (MonitoringInfo mi : metricsContainerRegistry.getMonitoringInfos()) {
        result.add(mi);
      }
      assertThat(result, containsInAnyOrder(expected.toArray()));
    }

    private class TestBeamFnDataOutboundAggregator extends BeamFnDataOutboundAggregator {
      private Map<LogicalEndpoint, List<org.apache.beam.sdk.util.construction.Timer<?>>> timers;
      private Map<LogicalEndpoint, List<WindowedValue<String>>> dataOutput;
      private Supplier<String> processBundleRequestIdSupplier;

      public TestBeamFnDataOutboundAggregator(Supplier<String> bundleIdSupplier) {
        super(PipelineOptionsFactory.create(), bundleIdSupplier, null, false);
        this.timers = new HashMap<>();
        this.dataOutput = new HashMap<>();
        this.processBundleRequestIdSupplier = bundleIdSupplier;
      }

      public Map<LogicalEndpoint, List<org.apache.beam.sdk.util.construction.Timer<?>>>
          getOutputTimers() {
        return timers;
      }

      public Map<LogicalEndpoint, List<WindowedValue<String>>> getOutputData() {
        return dataOutput;
      }

      @Override
      public <T> FnDataReceiver<T> registerOutputDataLocation(String pTransformId, Coder<T> coder) {
        return data ->
            dataOutput
                .computeIfAbsent(
                    LogicalEndpoint.data(processBundleRequestIdSupplier.get(), pTransformId),
                    e -> new ArrayList<>())
                .add((WindowedValue<String>) data);
      }

      @Override
      public <T> FnDataReceiver<T> registerOutputTimersLocation(
          String pTransformId, String timerFamilyId, Coder<T> coder) {
        return data ->
            timers
                .computeIfAbsent(
                    LogicalEndpoint.timer(
                        processBundleRequestIdSupplier.get(), pTransformId, timerFamilyId),
                    e -> new ArrayList<>())
                .add((org.apache.beam.sdk.util.construction.Timer<?>) data);
      }
    }

    @Test
    public void testTimers() throws Exception {
      dateTimeProvider.setDateTimeFixed(10000L);

      Pipeline p = Pipeline.create();
      PCollection<KV<String, String>> valuePCollection =
          p.apply(Create.of(KV.of("unused", "unused")));
      PCollection<String> outputPCollection =
          valuePCollection.apply(TEST_TRANSFORM_ID, ParDo.of(new TestTimerfulDoFn()));

      SdkComponents sdkComponents = SdkComponents.create();
      sdkComponents.registerEnvironment(Environment.getDefaultInstance());
      RunnerApi.Pipeline pProto = PipelineTranslation.toProto(p, sdkComponents);
      String inputPCollectionId = sdkComponents.registerPCollection(valuePCollection);
      String outputPCollectionId = sdkComponents.registerPCollection(outputPCollection);

      RunnerApi.PTransform pTransform =
          pProto
              .getComponents()
              .getTransformsOrThrow(
                  pProto
                      .getComponents()
                      .getTransformsOrThrow(TEST_TRANSFORM_ID)
                      .getSubtransforms(0))
              .toBuilder()
              .build();

      FakeBeamFnStateClient fakeStateClient =
          new FakeBeamFnStateClient(
              StringUtf8Coder.of(),
              ImmutableMap.of(
                  bagUserStateKey("bag", "X"), asList("X0"),
                  bagUserStateKey("bag", "A"), asList("A0"),
                  bagUserStateKey("bag", "C"), asList("C0")));
      List<WindowedValue<String>> mainOutputValues = new ArrayList<>();
      TestBeamFnDataOutboundAggregator aggregator =
          new TestBeamFnDataOutboundAggregator(() -> "57L");

      PTransformRunnerFactoryTestContext context =
          PTransformRunnerFactoryTestContext.builder(TEST_TRANSFORM_ID, pTransform)
              .beamFnStateClient(fakeStateClient)
              .processBundleInstructionId("57L")
              .pCollections(pProto.getComponentsOrBuilder().getPcollectionsMap())
              .coders(pProto.getComponents().getCodersMap())
              .windowingStrategies(pProto.getComponents().getWindowingStrategiesMap())
              .outboundAggregators(
                  ImmutableMap.of(ApiServiceDescriptor.getDefaultInstance(), aggregator))
              .timerApiServiceDescriptor(ApiServiceDescriptor.getDefaultInstance())
              .build();
      context.addPCollectionConsumer(
          outputPCollectionId,
          (FnDataReceiver) (FnDataReceiver<WindowedValue<String>>) mainOutputValues::add);

      new FnApiDoFnRunner.Factory<>().createRunnerForPTransform(context);

      Iterables.getOnlyElement(context.getStartBundleFunctions()).run();
      mainOutputValues.clear();

      assertThat(
          context.getPCollectionConsumers().keySet(),
          containsInAnyOrder(inputPCollectionId, outputPCollectionId));

      LogicalEndpoint eventTimer = LogicalEndpoint.timer("57L", TEST_TRANSFORM_ID, "ts-event");
      LogicalEndpoint processingTimer =
          LogicalEndpoint.timer("57L", TEST_TRANSFORM_ID, "ts-processing");
      LogicalEndpoint eventFamilyTimer =
          LogicalEndpoint.timer("57L", TEST_TRANSFORM_ID, "tfs-event-family");
      LogicalEndpoint processingFamilyTimer =
          LogicalEndpoint.timer("57L", TEST_TRANSFORM_ID, "tfs-processing-family");
      // Ensure that bag user state that is initially empty or populated works.
      // Ensure that the key order does not matter when we traverse over KV pairs.
      FnDataReceiver<WindowedValue<?>> mainInput =
          context.getPCollectionConsumer(inputPCollectionId);
      mainInput.accept(timestampedValueInGlobalWindow(KV.of("X", "X1"), new Instant(1000L)));
      mainInput.accept(timestampedValueInGlobalWindow(KV.of("Y", "Y1"), new Instant(1100L)));
      mainInput.accept(timestampedValueInGlobalWindow(KV.of("X", "X2"), new Instant(1200L)));
      mainInput.accept(timestampedValueInGlobalWindow(KV.of("Y", "Y2"), new Instant(1300L)));

      context
          .getIncomingTimerEndpoint(eventTimer.getTimerFamilyId())
          .getReceiver()
          .accept(timerInGlobalWindow("A", new Instant(1400L), new Instant(2400L)));
      context
          .getIncomingTimerEndpoint(eventTimer.getTimerFamilyId())
          .getReceiver()
          .accept(timerInGlobalWindow("B", new Instant(1500L), new Instant(2500L)));
      // This will be ignored since there are earlier timers, and the earlier timer will eventually
      // push the timer past 1600L.
      context
          .getIncomingTimerEndpoint(eventTimer.getTimerFamilyId())
          .getReceiver()
          .accept(timerInGlobalWindow("A", new Instant(1600L), new Instant(2600L)));
      // This will be ignored since the timer was already cleared in this bundle.
      context
          .getIncomingTimerEndpoint(processingTimer.getTimerFamilyId())
          .getReceiver()
          .accept(timerInGlobalWindow("X", new Instant(1700L), new Instant(2700L)));
      context
          .getIncomingTimerEndpoint(processingTimer.getTimerFamilyId())
          .getReceiver()
          .accept(timerInGlobalWindow("C", new Instant(1800L), new Instant(2800L)));
      context
          .getIncomingTimerEndpoint(processingTimer.getTimerFamilyId())
          .getReceiver()
          .accept(timerInGlobalWindow("B", new Instant(1500), new Instant(10032)));
      context
          .getIncomingTimerEndpoint(eventFamilyTimer.getTimerFamilyId())
          .getReceiver()
          .accept(
              dynamicTimerInGlobalWindow(
                  "B", "event-timer2", new Instant(2000L), new Instant(1650L)));
      context
          .getIncomingTimerEndpoint(processingFamilyTimer.getTimerFamilyId())
          .getReceiver()
          .accept(
              dynamicTimerInGlobalWindow(
                  "Y", "processing-timer2", new Instant(2100L), new Instant(3100L)));

      assertThat(
          mainOutputValues,
          contains(
              timestampedValueInGlobalWindow("key:X mainX[X0]", new Instant(1000L)),
              timestampedValueInGlobalWindow("key:Y mainY[]", new Instant(1100L)),
              timestampedValueInGlobalWindow("key:X mainX[X0, X1]", new Instant(1200L)),
              timestampedValueInGlobalWindow("key:Y mainY[Y1]", new Instant(1300L)),
              timestampedValueInGlobalWindow("key:A event[A0]", new Instant(1400L)),
              timestampedValueInGlobalWindow("key:B event[]", new Instant(1500L)),
              timestampedValueInGlobalWindow("key:A event[A0, event]", new Instant(1400L)),
              timestampedValueInGlobalWindow("key:A event[A0, event, event]", new Instant(1400L)),
              timestampedValueInGlobalWindow(
                  "key:A event[A0, event, event, event]", new Instant(1400L)),
              timestampedValueInGlobalWindow(
                  "key:A event[A0, event, event, event, event]", new Instant(1400L)),
              timestampedValueInGlobalWindow(
                  "key:A event[A0, event, event, event, event, event]", new Instant(1400L)),
              timestampedValueInGlobalWindow(
                  "key:A event[A0, event, event, event, event, event, event]", new Instant(1400L)),
              timestampedValueInGlobalWindow("key:C processing[C0]", new Instant(1800L)),
              timestampedValueInGlobalWindow("key:B processing[event]", new Instant(1500L)),
              timestampedValueInGlobalWindow("key:B event[event, processing]", new Instant(1500)),
              timestampedValueInGlobalWindow(
                  "key:B event[event, processing, event]", new Instant(1500)),
              timestampedValueInGlobalWindow(
                  "key:B event[event, processing, event, event]", new Instant(1500)),
              timestampedValueInGlobalWindow(
                  "key:B event-family[event, processing, event, event, event]", new Instant(2000L)),
              timestampedValueInGlobalWindow(
                  "key:Y processing-family[Y1, Y2]", new Instant(2100L))));

      mainOutputValues.clear();

      // Timers will get delivered to the client when finishBundle is called.
      Iterables.getOnlyElement(context.getFinishBundleFunctions()).run();

      assertThat(
          aggregator.getOutputTimers().get(eventTimer),
          contains(
              clearedTimerInGlobalWindow("X"),
              timerInGlobalWindow("Y", new Instant(2100L), new Instant(2181L)),
              timerInGlobalWindow("A", new Instant(1400L), new Instant(2617L)),
              timerInGlobalWindow("B", new Instant(2000L), new Instant(2071L)),
              timerInGlobalWindow("C", new Instant(1800L), new Instant(1861L))));
      assertThat(
          aggregator.getOutputTimers().get(processingTimer),
          contains(
              clearedTimerInGlobalWindow("X"),
              timerInGlobalWindow("Y", new Instant(2100L), new Instant(10082L)),
              timerInGlobalWindow("A", new Instant(1400L), new Instant(10032L)),
              timerInGlobalWindow("B", new Instant(2000L), new Instant(10072L)),
              timerInGlobalWindow("C", new Instant(1800L), new Instant(10062L))));

      assertThat(
          aggregator.getOutputTimers().get(eventFamilyTimer),
          containsInAnyOrder(
              dynamicTimerInGlobalWindow(
                  "X", "event-timer1", new Instant(1200L), new Instant(1203L)),
              clearedTimerInGlobalWindow("X", "to-delete-event"),
              clearedTimerInGlobalWindow("Y", "to-delete-event"),
              dynamicTimerInGlobalWindow(
                  "Y", "event-timer1", new Instant(2100L), new Instant(2183L)),
              dynamicTimerInGlobalWindow(
                  "A", "event-timer1", new Instant(1400L), new Instant(2619L)),
              dynamicTimerInGlobalWindow(
                  "B", "event-timer1", new Instant(2000L), new Instant(2073L)),
              dynamicTimerInGlobalWindow(
                  "C", "event-timer1", new Instant(1800L), new Instant(1863L))));
      assertThat(
          aggregator.getOutputTimers().get(processingFamilyTimer),
          containsInAnyOrder(
              dynamicTimerInGlobalWindow(
                  "X", "processing-timer1", new Instant(1200L), new Instant(10004L)),
              clearedTimerInGlobalWindow("X", "to-delete-processing"),
              dynamicTimerInGlobalWindow(
                  "Y", "processing-timer1", new Instant(2100L), new Instant(10084L)),
              clearedTimerInGlobalWindow("Y", "to-delete-processing"),
              dynamicTimerInGlobalWindow(
                  "A", "processing-timer1", new Instant(1400L), new Instant(10034L)),
              dynamicTimerInGlobalWindow(
                  "B", "processing-timer1", new Instant(2000L), new Instant(10074L)),
              dynamicTimerInGlobalWindow(
                  "C", "processing-timer1", new Instant(1800L), new Instant(10064L))));

      assertThat(mainOutputValues, empty());

      Iterables.getOnlyElement(context.getTearDownFunctions()).run();
      assertThat(mainOutputValues, empty());

      assertEquals(
          new FakeBeamFnStateClient(
                  StringUtf8Coder.of(),
                  ImmutableMap.<StateKey, List<String>>builder()
                      .put(bagUserStateKey("bag", "X"), asList("X0", "X1", "X2"))
                      .put(bagUserStateKey("bag", "Y"), asList("Y1", "Y2", "processing-family"))
                      .put(
                          bagUserStateKey("bag", "A"),
                          asList(
                              "A0", "event", "event", "event", "event", "event", "event", "event"))
                      .put(
                          bagUserStateKey("bag", "B"),
                          asList("event", "processing", "event", "event", "event", "event-family"))
                      .put(bagUserStateKey("bag", "C"), asList("C0", "processing"))
                      .build())
              .getData(),
          fakeStateClient.getData());
    }

    private <K> org.apache.beam.sdk.util.construction.Timer<K> timerInGlobalWindow(
        K userKey, Instant holdTimestamp, Instant fireTimestamp) {
      return dynamicTimerInGlobalWindow(userKey, "", holdTimestamp, fireTimestamp);
    }

    private <K> org.apache.beam.sdk.util.construction.Timer<K> clearedTimerInGlobalWindow(
        K userKey) {
      return clearedTimerInGlobalWindow(userKey, "");
    }

    private <K> org.apache.beam.sdk.util.construction.Timer<K> clearedTimerInGlobalWindow(
        K userKey, String dynamicTimerTag) {
      return org.apache.beam.sdk.util.construction.Timer.cleared(
          userKey, dynamicTimerTag, Collections.singletonList(GlobalWindow.INSTANCE));
    }

    private <K> org.apache.beam.sdk.util.construction.Timer<K> dynamicTimerInGlobalWindow(
        K userKey, String dynamicTimerTag, Instant holdTimestamp, Instant fireTimestamp) {
      return org.apache.beam.sdk.util.construction.Timer.of(
          userKey,
          dynamicTimerTag,
          Collections.singletonList(GlobalWindow.INSTANCE),
          fireTimestamp,
          holdTimestamp,
          PaneInfo.NO_FIRING);
    }

    private <T> WindowedValue<T> valueInWindows(
        T value, BoundedWindow window, BoundedWindow... windows) {
      return WindowedValue.of(
          value,
          window.maxTimestamp(),
          ImmutableList.<BoundedWindow>builder().add(window).add(windows).build(),
          PaneInfo.NO_FIRING);
    }

    private static class TestTimerfulDoFn extends DoFn<KV<String, String>, String> {

      @StateId("bag")
      private final StateSpec<BagState<String>> bagStateSpec = StateSpecs.bag(StringUtf8Coder.of());

      @TimerId("event")
      private final TimerSpec eventTimerSpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

      @TimerId("processing")
      private final TimerSpec processingTimerSpec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

      @TimerFamily("event-family")
      private final TimerSpec eventTimerFamilySpec = TimerSpecs.timerMap(TimeDomain.EVENT_TIME);

      @TimerFamily("processing-family")
      private final TimerSpec processingTimerFamilySpec =
          TimerSpecs.timerMap(TimeDomain.PROCESSING_TIME);

      @ProcessElement
      public void processElement(
          ProcessContext context,
          @StateId("bag") BagState<String> bagState,
          @TimerId("event") Timer eventTimeTimer,
          @TimerId("processing") Timer processingTimeTimer,
          @TimerFamily("event-family") TimerMap eventTimerFamily,
          @TimerFamily("processing-family") TimerMap processingTimerFamily) {
        context.output(
            "key:"
                + context.element().getKey()
                + " main"
                + context.element().getKey()
                + Iterables.toString(bagState.read()));
        bagState.add(context.element().getValue());

        eventTimeTimer
            .withOutputTimestamp(context.timestamp())
            .set(context.timestamp().plus(Duration.millis(1L)));
        eventTimeTimer.clear();
        processingTimeTimer.offset(Duration.millis(2L));
        processingTimeTimer.setRelative();
        processingTimeTimer.clear();
        eventTimerFamily
            .get("event-timer1")
            .withOutputTimestamp(context.timestamp())
            .set(context.timestamp().plus(Duration.millis(3L)));
        eventTimerFamily.get("to-delete-event").set(context.timestamp().plus(Duration.millis(5L)));
        eventTimerFamily.get("to-delete-event").clear();
        processingTimerFamily.get("processing-timer1").offset(Duration.millis(4L)).setRelative();
        processingTimerFamily.get("to-delete-processing").offset(Duration.millis(4L)).setRelative();
        processingTimerFamily.get("to-delete-processing").clear();
      }

      @OnTimer("event")
      public void eventTimer(
          OnTimerContext context,
          @Key String key,
          @StateId("bag") BagState<String> bagState,
          @TimerId("event") Timer eventTimeTimer,
          @TimerId("processing") Timer processingTimeTimer,
          @TimerFamily("event-family") TimerMap eventTimerFamily,
          @TimerFamily("processing-family") TimerMap processingTimerFamily) {
        context.output("key:" + key + " event" + Iterables.toString(bagState.read()));
        bagState.add("event");
        eventTimeTimer
            .withOutputTimestamp(context.timestamp())
            .set(context.fireTimestamp().plus(Duration.millis(31L)));
        processingTimeTimer.offset(Duration.millis(32L));
        processingTimeTimer.setRelative();
        eventTimerFamily
            .get("event-timer1")
            .withOutputTimestamp(context.timestamp())
            .set(context.fireTimestamp().plus(Duration.millis(33L)));

        processingTimerFamily.get("processing-timer1").offset(Duration.millis(34L)).setRelative();
      }

      @OnTimer("processing")
      public void processingTimer(
          OnTimerContext context,
          @Key String key,
          @StateId("bag") BagState<String> bagState,
          @TimerId("event") Timer eventTimeTimer,
          @TimerId("processing") Timer processingTimeTimer,
          @TimerFamily("event-family") TimerMap eventTimerFamily,
          @TimerFamily("processing-family") TimerMap processingTimerFamily) {
        context.output("key:" + key + " processing" + Iterables.toString(bagState.read()));
        bagState.add("processing");

        eventTimeTimer
            .withOutputTimestamp(context.timestamp())
            .set(context.timestamp().plus(Duration.millis(61L)));
        processingTimeTimer.offset(Duration.millis(62L));
        processingTimeTimer.setRelative();
        eventTimerFamily
            .get("event-timer1")
            .withOutputTimestamp(context.timestamp())
            .set(context.timestamp().plus(Duration.millis(63L)));

        processingTimerFamily.get("processing-timer1").offset(Duration.millis(64L)).setRelative();
      }

      @OnTimerFamily("event-family")
      public void eventFamilyOnTimer(
          OnTimerContext context,
          @Key String key,
          @Timestamp Instant ts,
          @StateId("bag") BagState<String> bagState,
          @TimerId("event") Timer eventTimeTimer,
          @TimerId("processing") Timer processingTimeTimer,
          @TimerFamily("event-family") TimerMap eventTimerFamily,
          @TimerFamily("processing-family") TimerMap processingTimerFamily) {
        context.output("key:" + key + " event-family" + Iterables.toString(bagState.read()));
        bagState.add("event-family");

        eventTimeTimer
            .withOutputTimestamp(context.timestamp())
            .set(context.timestamp().plus(Duration.millis(71L)));
        processingTimeTimer.offset(Duration.millis(72L));
        processingTimeTimer.setRelative();
        eventTimerFamily
            .get("event-timer1")
            .withOutputTimestamp(context.timestamp())
            .set(context.timestamp().plus(Duration.millis(73L)));

        processingTimerFamily.get("processing-timer1").offset(Duration.millis(74L)).setRelative();
      }

      @OnTimerFamily("processing-family")
      public void processingFamilyOnTimer(
          OnTimerContext context,
          @Key String key,
          @StateId("bag") BagState<String> bagState,
          @TimerId("event") Timer eventTimeTimer,
          @TimerId("processing") Timer processingTimeTimer,
          @TimerFamily("event-family") TimerMap eventTimerFamily,
          @TimerFamily("processing-family") TimerMap processingTimerFamily) {
        context.output("key:" + key + " processing-family" + Iterables.toString(bagState.read()));
        bagState.add("processing-family");

        eventTimeTimer
            .withOutputTimestamp(context.timestamp())
            .set(context.timestamp().plus(Duration.millis(81L)));
        processingTimeTimer.offset(Duration.millis(82L));
        processingTimeTimer.setRelative();
        eventTimerFamily
            .get("event-timer1")
            .withOutputTimestamp(context.timestamp())
            .set(context.timestamp().plus(Duration.millis(83L)));

        processingTimerFamily.get("processing-timer1").offset(Duration.millis(84L)).setRelative();
      }
    }

    /**
     * Produces an iterable side input {@link StateKey} for the test PTransform id in the global
     * window.
     */
    private StateKey iterableSideInputKey(String sideInputId) throws IOException {
      return iterableSideInputKey(
          sideInputId,
          ByteString.copyFrom(
              CoderUtils.encodeToByteArray(GlobalWindow.Coder.INSTANCE, GlobalWindow.INSTANCE)));
    }

    /**
     * Produces an iterable side input {@link StateKey} for the test PTransform id in the supplied
     * window.
     */
    private StateKey iterableSideInputKey(String sideInputId, ByteString windowKey) {
      return StateKey.newBuilder()
          .setIterableSideInput(
              StateKey.IterableSideInput.newBuilder()
                  .setTransformId(TEST_TRANSFORM_ID)
                  .setSideInputId(sideInputId)
                  .setWindow(windowKey))
          .build();
    }

    private ByteString encode(String... values) throws IOException {
      ByteStringOutputStream out = new ByteStringOutputStream();
      for (String value : values) {
        StringUtf8Coder.of().encode(value, out);
      }
      return out.toByteString();
    }

    @Test
    public void testRegistration() {
      for (PTransformRunnerFactory.Registrar registrar :
          ServiceLoader.load(PTransformRunnerFactory.Registrar.class)) {
        if (registrar instanceof FnApiDoFnRunner.Registrar) {
          assertThat(
              registrar.getPTransformRunnerFactories(),
              IsMapContaining.hasKey(PTransformTranslation.PAR_DO_TRANSFORM_URN));
          return;
        }
      }
      fail("Expected registrar not found.");
    }

    /**
     * The trySplit testing of this splittable DoFn is done when processing the {@link
     * NonWindowObservingTestSplittableDoFn#SPLIT_ELEMENT}. Always checkpoints at element {@link
     * NonWindowObservingTestSplittableDoFn#CHECKPOINT_UPPER_BOUND}.
     *
     * <p>The expected thread flow is:
     *
     * <ul>
     *   <li>splitting thread: {@link
     *       NonWindowObservingTestSplittableDoFn#waitForSplitElementToBeProcessed()}
     *   <li>process element thread: {@link
     *       NonWindowObservingTestSplittableDoFn#splitElementProcessed()}
     *   <li>splitting thread: perform try split
     *   <li>splitting thread: {@link NonWindowObservingTestSplittableDoFn#trySplitPerformed()} *
     *   <li>process element thread: {@link
     *       NonWindowObservingTestSplittableDoFn#waitForTrySplitPerformed()}
     * </ul>
     */
    static class NonWindowObservingTestSplittableDoFn extends DoFn<String, String> {
      private static final ConcurrentMap<String, Latches> DOFN_INSTANCE_TO_LATCHES =
          new ConcurrentHashMap<>();
      private static final long SPLIT_ELEMENT = 3;
      private static final long CHECKPOINT_UPPER_BOUND = 8;

      static class Latches {
        public Latches() {}

        CountDownLatch blockProcessLatch = new CountDownLatch(0);
        CountDownLatch processEnteredLatch = new CountDownLatch(1);
        CountDownLatch splitElementProcessedLatch = new CountDownLatch(1);
        CountDownLatch trySplitPerformedLatch = new CountDownLatch(1);
        AtomicBoolean abortProcessing = new AtomicBoolean();
      }

      private Latches getLatches() {
        return DOFN_INSTANCE_TO_LATCHES.computeIfAbsent(this.uuid, (uuid) -> new Latches());
      }

      public void splitElementProcessed() {
        getLatches().splitElementProcessedLatch.countDown();
      }

      public void waitForSplitElementToBeProcessed() throws InterruptedException {
        if (!getLatches().splitElementProcessedLatch.await(30, TimeUnit.SECONDS)) {
          fail("Failed to wait for trySplit to occur.");
        }
      }

      public void trySplitPerformed() {
        getLatches().trySplitPerformedLatch.countDown();
      }

      public void waitForTrySplitPerformed() throws InterruptedException {
        if (!getLatches().trySplitPerformedLatch.await(30, TimeUnit.SECONDS)) {
          fail("Failed to wait for trySplit to occur.");
        }
      }

      // Must be called before process is invoked. Will prevent process from doing anything until
      // unblockProcess is
      // called.
      public void setupBlockProcess() {
        getLatches().blockProcessLatch = new CountDownLatch(1);
      }

      public void enterProcessAndBlockIfEnabled() throws InterruptedException {
        getLatches().processEnteredLatch.countDown();
        if (!getLatches().blockProcessLatch.await(30, TimeUnit.SECONDS)) {
          fail("Failed to wait for unblockProcess to occur.");
        }
      }

      public void waitForProcessEntered() throws InterruptedException {
        if (!getLatches().processEnteredLatch.await(5, TimeUnit.SECONDS)) {
          fail("Failed to wait for process to begin.");
        }
      }

      public void unblockProcess() throws InterruptedException {
        getLatches().blockProcessLatch.countDown();
      }

      public void setAbortProcessing() {
        getLatches().abortProcessing.set(true);
      }

      public boolean shouldAbortProcessing() {
        return getLatches().abortProcessing.get();
      }

      private final String uuid;

      private NonWindowObservingTestSplittableDoFn() {
        this.uuid = UUID.randomUUID().toString();
      }

      @ProcessElement
      public ProcessContinuation processElement(
          ProcessContext context,
          RestrictionTracker<OffsetRange, Long> tracker,
          ManualWatermarkEstimator<Instant> watermarkEstimator)
          throws Exception {
        long checkpointUpperBound = CHECKPOINT_UPPER_BOUND;
        long position = tracker.currentRestriction().getFrom();
        boolean claimStatus = true;
        while (!shouldAbortProcessing()) {
          claimStatus = tracker.tryClaim(position);
          if (!claimStatus) {
            break;
          } else if (position == SPLIT_ELEMENT) {
            splitElementProcessed();
            waitForTrySplitPerformed();
          }
          context.outputWithTimestamp(
              context.element() + ":" + position,
              GlobalWindow.TIMESTAMP_MIN_VALUE.plus(Duration.millis(position)));
          watermarkEstimator.setWatermark(
              GlobalWindow.TIMESTAMP_MIN_VALUE.plus(Duration.millis(position)));
          position += 1L;
          if (position == checkpointUpperBound) {
            break;
          }
        }
        if (!claimStatus) {
          return ProcessContinuation.stop();
        } else {
          return ProcessContinuation.resume().withResumeDelay(Duration.millis(54321L));
        }
      }

      @GetInitialRestriction
      public OffsetRange restriction(@Element String element) {
        return new OffsetRange(0, Integer.parseInt(element));
      }

      @NewTracker
      public RestrictionTracker<OffsetRange, Long> newTracker(
          @Restriction OffsetRange restriction) {
        return new OffsetRangeTracker(restriction);
      }

      @SplitRestriction
      public void splitRange(@Restriction OffsetRange range, OutputReceiver<OffsetRange> receiver) {
        receiver.output(new OffsetRange(range.getFrom(), (range.getFrom() + range.getTo()) / 2));
        receiver.output(new OffsetRange((range.getFrom() + range.getTo()) / 2, range.getTo()));
      }

      @TruncateRestriction
      public TruncateResult<OffsetRange> truncateRestriction(@Restriction OffsetRange range)
          throws Exception {
        return TruncateResult.of(new OffsetRange(range.getFrom(), range.getTo() / 2));
      }

      @GetInitialWatermarkEstimatorState
      public Instant getInitialWatermarkEstimatorState() {
        return GlobalWindow.TIMESTAMP_MIN_VALUE.plus(Duration.millis(1));
      }

      @NewWatermarkEstimator
      public WatermarkEstimators.Manual newWatermarkEstimator(
          @WatermarkEstimatorState Instant watermark) {
        return new WatermarkEstimators.Manual(watermark);
      }
    }

    /**
     * A window observing variant of {@link NonWindowObservingTestSplittableDoFn} which uses the
     * side inputs to choose the checkpoint upper bound.
     */
    static class WindowObservingTestSplittableDoFn extends NonWindowObservingTestSplittableDoFn {

      private final PCollectionView<String> singletonSideInput;
      private static final long PROCESSED_WINDOW = 1;
      private boolean splitAtTruncate = false;
      private long processedWindowCount = 0;

      private WindowObservingTestSplittableDoFn(PCollectionView<String> singletonSideInput) {
        this.singletonSideInput = singletonSideInput;
      }

      private static WindowObservingTestSplittableDoFn forSplitAtTruncate(
          PCollectionView<String> singletonSideInput) {
        WindowObservingTestSplittableDoFn doFn =
            new WindowObservingTestSplittableDoFn(singletonSideInput);
        doFn.splitAtTruncate = true;
        return doFn;
      }

      @Override
      @ProcessElement
      public ProcessContinuation processElement(
          ProcessContext context,
          RestrictionTracker<OffsetRange, Long> tracker,
          ManualWatermarkEstimator<Instant> watermarkEstimator)
          throws Exception {
        enterProcessAndBlockIfEnabled();
        long checkpointUpperBound = Long.parseLong(context.sideInput(singletonSideInput));
        long position = tracker.currentRestriction().getFrom();
        boolean claimStatus = true;
        while (!shouldAbortProcessing()) {
          claimStatus = tracker.tryClaim(position);
          if (!claimStatus) {
            break;
          } else if (position == NonWindowObservingTestSplittableDoFn.SPLIT_ELEMENT) {
            splitElementProcessed();
            waitForTrySplitPerformed();
          }
          context.outputWithTimestamp(
              context.element() + ":" + position,
              GlobalWindow.TIMESTAMP_MIN_VALUE.plus(Duration.millis(position)));
          watermarkEstimator.setWatermark(
              GlobalWindow.TIMESTAMP_MIN_VALUE.plus(Duration.millis(position)));
          position += 1L;
          if (position == checkpointUpperBound) {
            break;
          }
        }
        if (!claimStatus) {
          return ProcessContinuation.stop();
        } else {
          return ProcessContinuation.resume().withResumeDelay(Duration.millis(54321L));
        }
      }

      @Override
      public Duration getAllowedTimestampSkew() {
        return Duration.millis(Long.MAX_VALUE);
      }

      @Override
      @TruncateRestriction
      public TruncateResult<OffsetRange> truncateRestriction(@Restriction OffsetRange range)
          throws Exception {
        // Waiting for split when we are on the second window.
        if (splitAtTruncate && processedWindowCount == PROCESSED_WINDOW) {
          splitElementProcessed();
          waitForTrySplitPerformed();
        }
        processedWindowCount += 1;
        return TruncateResult.of(new OffsetRange(range.getFrom(), range.getTo() / 2));
      }
    }

    @Test
    public void testProcessElementForSizedElementAndRestriction() throws Exception {
      Pipeline p = Pipeline.create();
      addExperiment(p.getOptions().as(ExperimentalOptions.class), "beam_fn_api");
      // TODO(BEAM-10097): Remove experiment once all portable runners support this view type
      addExperiment(p.getOptions().as(ExperimentalOptions.class), "use_runner_v2");
      PCollection<String> valuePCollection = p.apply(Create.of("unused"));
      PCollectionView<String> singletonSideInputView = valuePCollection.apply(View.asSingleton());
      WindowObservingTestSplittableDoFn doFn =
          new WindowObservingTestSplittableDoFn(singletonSideInputView);
      valuePCollection.apply(
          TEST_TRANSFORM_ID, ParDo.of(doFn).withSideInputs(singletonSideInputView));

      RunnerApi.Pipeline pProto =
          ProtoOverrides.updateTransform(
              PTransformTranslation.PAR_DO_TRANSFORM_URN,
              PipelineTranslation.toProto(p, SdkComponents.create(p.getOptions()), true),
              SplittableParDoExpander.createSizedReplacement());
      String expandedTransformId =
          Iterables.find(
                  pProto.getComponents().getTransformsMap().entrySet(),
                  entry ->
                      entry
                              .getValue()
                              .getSpec()
                              .getUrn()
                              .equals(
                                  PTransformTranslation
                                      .SPLITTABLE_PROCESS_SIZED_ELEMENTS_AND_RESTRICTIONS_URN)
                          && entry.getValue().getUniqueName().contains(TEST_TRANSFORM_ID))
              .getKey();
      RunnerApi.PTransform pTransform =
          pProto.getComponents().getTransformsOrThrow(expandedTransformId);
      String inputPCollectionId =
          pTransform.getInputsOrThrow(ParDoTranslation.getMainInputName(pTransform));
      RunnerApi.PCollection inputPCollection =
          pProto.getComponents().getPcollectionsOrThrow(inputPCollectionId);
      RehydratedComponents rehydratedComponents =
          RehydratedComponents.forComponents(pProto.getComponents());
      Coder<WindowedValue> inputCoder =
          WindowedValue.getFullCoder(
              CoderTranslation.fromProto(
                  pProto.getComponents().getCodersOrThrow(inputPCollection.getCoderId()),
                  rehydratedComponents,
                  TranslationContext.DEFAULT),
              (Coder)
                  CoderTranslation.fromProto(
                      pProto
                          .getComponents()
                          .getCodersOrThrow(
                              pProto
                                  .getComponents()
                                  .getWindowingStrategiesOrThrow(
                                      inputPCollection.getWindowingStrategyId())
                                  .getWindowCoderId()),
                      rehydratedComponents,
                      TranslationContext.DEFAULT));
      String outputPCollectionId = pTransform.getOutputsOrThrow("output");

      ImmutableMap<StateKey, List<String>> stateData =
          ImmutableMap.of(
              iterableSideInputKey(
                  singletonSideInputView.getTagInternal().getId(), ByteString.EMPTY),
              asList("8"));

      FakeBeamFnStateClient fakeClient = new FakeBeamFnStateClient(StringUtf8Coder.of(), stateData);

      BundleSplitListener.InMemory splitListener = BundleSplitListener.InMemory.create();

      PTransformRunnerFactoryTestContext context =
          PTransformRunnerFactoryTestContext.builder(TEST_TRANSFORM_ID, pTransform)
              .beamFnStateClient(fakeClient)
              .processBundleInstructionId("57")
              .pCollections(pProto.getComponentsOrBuilder().getPcollectionsMap())
              .coders(pProto.getComponents().getCodersMap())
              .windowingStrategies(pProto.getComponents().getWindowingStrategiesMap())
              .splitListener(splitListener)
              .build();
      List<WindowedValue<String>> mainOutputValues = new ArrayList<>();
      context.addPCollectionConsumer(
          outputPCollectionId,
          (FnDataReceiver) (FnDataReceiver<WindowedValue<String>>) mainOutputValues::add);

      new FnApiDoFnRunner.Factory<>().createRunnerForPTransform(context);

      Iterables.getOnlyElement(context.getStartBundleFunctions()).run();
      mainOutputValues.clear();

      assertThat(
          context.getPCollectionConsumers().keySet(),
          containsInAnyOrder(inputPCollectionId, outputPCollectionId));

      FnDataReceiver<WindowedValue<?>> mainInput =
          context.getPCollectionConsumer(inputPCollectionId);
      assertThat(mainInput, instanceOf(HandlesSplits.class));

      {
        // Check that before processing an element we don't report progress
        assertNoReportedProgress(context.getBundleProgressReporters());
        mainInput.accept(
            valueInGlobalWindow(
                KV.of(
                    KV.of("5", KV.of(new OffsetRange(5, 10), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                    5.0)));
        // Check that after processing an element we don't report progress
        assertNoReportedProgress(context.getBundleProgressReporters());

        // Since the side input upperBound is 8 we will process 5, 6, and 7 then checkpoint.
        // We expect that the watermark advances to MIN + 7 and that the primary represents [5, 8)
        // with
        // the original watermark while the residual represents [8, 10) with the new MIN + 7
        // watermark.
        BundleApplication primaryRoot = Iterables.getOnlyElement(splitListener.getPrimaryRoots());
        DelayedBundleApplication residualRoot =
            Iterables.getOnlyElement(splitListener.getResidualRoots());
        assertEquals(ParDoTranslation.getMainInputName(pTransform), primaryRoot.getInputId());
        assertEquals(TEST_TRANSFORM_ID, primaryRoot.getTransformId());
        assertEquals(
            ParDoTranslation.getMainInputName(pTransform),
            residualRoot.getApplication().getInputId());
        assertEquals(TEST_TRANSFORM_ID, residualRoot.getApplication().getTransformId());
        assertEquals(
            valueInGlobalWindow(
                KV.of(
                    KV.of("5", KV.of(new OffsetRange(5, 8), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                    3.0)),
            inputCoder.decode(primaryRoot.getElement().newInput()));
        assertEquals(
            valueInGlobalWindow(
                KV.of(
                    KV.of(
                        "5",
                        KV.of(
                            new OffsetRange(8, 10),
                            GlobalWindow.TIMESTAMP_MIN_VALUE.plus(Duration.millis(7)))),
                    2.0)),
            inputCoder.decode(residualRoot.getApplication().getElement().newInput()));
        Instant expectedOutputWatermark = GlobalWindow.TIMESTAMP_MIN_VALUE.plus(Duration.millis(7));
        assertEquals(
            ImmutableMap.of(
                "output",
                org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.Timestamp.newBuilder()
                    .setSeconds(expectedOutputWatermark.getMillis() / 1000)
                    .setNanos((int) (expectedOutputWatermark.getMillis() % 1000) * 1000000)
                    .build()),
            residualRoot.getApplication().getOutputWatermarksMap());
        assertEquals(
            org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.Duration.newBuilder()
                .setSeconds(54)
                .setNanos(321000000)
                .build(),
            residualRoot.getRequestedTimeDelay());
        splitListener.clear();

        // Check that before processing an element we don't report progress
        assertNoReportedProgress(context.getBundleProgressReporters());
        mainInput.accept(
            valueInGlobalWindow(
                KV.of(
                    KV.of("2", KV.of(new OffsetRange(0, 2), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                    2.0)));
        // Check that after processing an element we don't report progress
        assertNoReportedProgress(context.getBundleProgressReporters());

        assertThat(
            mainOutputValues,
            contains(
                timestampedValueInGlobalWindow(
                    "5:5", GlobalWindow.TIMESTAMP_MIN_VALUE.plus(Duration.millis(5))),
                timestampedValueInGlobalWindow(
                    "5:6", GlobalWindow.TIMESTAMP_MIN_VALUE.plus(Duration.millis(6))),
                timestampedValueInGlobalWindow(
                    "5:7", GlobalWindow.TIMESTAMP_MIN_VALUE.plus(Duration.millis(7))),
                timestampedValueInGlobalWindow(
                    "2:0", GlobalWindow.TIMESTAMP_MIN_VALUE.plus(Duration.millis(0))),
                timestampedValueInGlobalWindow(
                    "2:1", GlobalWindow.TIMESTAMP_MIN_VALUE.plus(Duration.millis(1)))));
        assertTrue(splitListener.getPrimaryRoots().isEmpty());
        assertTrue(splitListener.getResidualRoots().isEmpty());
        mainOutputValues.clear();
      }

      {
        // Setup and launch the trySplit thread.
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        Future<HandlesSplits.SplitResult> trySplitFuture =
            executorService.submit(
                () -> {
                  try {
                    doFn.waitForSplitElementToBeProcessed();
                    // Currently processing "3" out of range [0, 5) elements.
                    assertEquals(0.6, ((HandlesSplits) mainInput).getProgress(), 0.01);

                    // Check that during progressing of an element we report progress
                    assertReportedProgressEquals(
                        context.getShortIdMap(), context.getBundleProgressReporters(), 3.0, 2.0);

                    return ((HandlesSplits) mainInput).trySplit(0);
                  } finally {
                    doFn.trySplitPerformed();
                  }
                });

        // Check that before processing an element we don't report progress
        assertNoReportedProgress(context.getBundleProgressReporters());
        mainInput.accept(
            valueInGlobalWindow(
                KV.of(
                    KV.of("7", KV.of(new OffsetRange(0, 5), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                    2.0)));
        HandlesSplits.SplitResult trySplitResult = trySplitFuture.get();

        // Check that after processing an element we don't report progress
        assertNoReportedProgress(context.getBundleProgressReporters());

        // Since the SPLIT_ELEMENT is 3 we will process 0, 1, 2, 3 then be split.
        // We expect that the watermark advances to MIN + 2 since the manual watermark estimator
        // has yet to be invoked for the split element and that the primary represents [0, 4) with
        // the original watermark while the residual represents [4, 5) with the new MIN + 2
        // watermark.
        assertThat(
            mainOutputValues,
            contains(
                timestampedValueInGlobalWindow(
                    "7:0", GlobalWindow.TIMESTAMP_MIN_VALUE.plus(Duration.millis(0))),
                timestampedValueInGlobalWindow(
                    "7:1", GlobalWindow.TIMESTAMP_MIN_VALUE.plus(Duration.millis(1))),
                timestampedValueInGlobalWindow(
                    "7:2", GlobalWindow.TIMESTAMP_MIN_VALUE.plus(Duration.millis(2))),
                timestampedValueInGlobalWindow(
                    "7:3", GlobalWindow.TIMESTAMP_MIN_VALUE.plus(Duration.millis(3)))));

        BundleApplication primaryRoot = Iterables.getOnlyElement(trySplitResult.getPrimaryRoots());
        DelayedBundleApplication residualRoot =
            Iterables.getOnlyElement(trySplitResult.getResidualRoots());
        assertEquals(ParDoTranslation.getMainInputName(pTransform), primaryRoot.getInputId());
        assertEquals(TEST_TRANSFORM_ID, primaryRoot.getTransformId());
        assertEquals(
            ParDoTranslation.getMainInputName(pTransform),
            residualRoot.getApplication().getInputId());
        assertEquals(TEST_TRANSFORM_ID, residualRoot.getApplication().getTransformId());
        assertEquals(
            valueInGlobalWindow(
                KV.of(
                    KV.of("7", KV.of(new OffsetRange(0, 4), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                    4.0)),
            inputCoder.decode(primaryRoot.getElement().newInput()));
        assertEquals(
            valueInGlobalWindow(
                KV.of(
                    KV.of(
                        "7",
                        KV.of(
                            new OffsetRange(4, 5),
                            GlobalWindow.TIMESTAMP_MIN_VALUE.plus(Duration.millis(2)))),
                    1.0)),
            inputCoder.decode(residualRoot.getApplication().getElement().newInput()));
        Instant expectedOutputWatermark = GlobalWindow.TIMESTAMP_MIN_VALUE.plus(Duration.millis(2));
        assertEquals(
            ImmutableMap.of(
                "output",
                org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.Timestamp.newBuilder()
                    .setSeconds(expectedOutputWatermark.getMillis() / 1000)
                    .setNanos((int) (expectedOutputWatermark.getMillis() % 1000) * 1000000)
                    .build()),
            residualRoot.getApplication().getOutputWatermarksMap());
        // We expect 0 resume delay.
        assertEquals(
            residualRoot.getRequestedTimeDelay().getDefaultInstanceForType(),
            residualRoot.getRequestedTimeDelay());
        // We don't expect the outputs to goto the SDK initiated checkpointing listener.
        assertTrue(splitListener.getPrimaryRoots().isEmpty());
        assertTrue(splitListener.getResidualRoots().isEmpty());
        mainOutputValues.clear();
        executorService.shutdown();
      }

      Iterables.getOnlyElement(context.getFinishBundleFunctions()).run();
      assertThat(mainOutputValues, empty());

      Iterables.getOnlyElement(context.getTearDownFunctions()).run();
      assertThat(mainOutputValues, empty());

      // Assert that state data did not change
      assertEquals(
          new FakeBeamFnStateClient(StringUtf8Coder.of(), stateData).getData(),
          fakeClient.getData());
    }

    @Test
    public void testProcessElementForSizedElementAndRestrictionSplitBeforeTryClaim()
        throws Exception {
      Pipeline p = Pipeline.create();
      addExperiment(p.getOptions().as(ExperimentalOptions.class), "beam_fn_api");
      // TODO(BEAM-10097): Remove experiment once all portable runners support this view type
      addExperiment(p.getOptions().as(ExperimentalOptions.class), "use_runner_v2");
      PCollection<String> valuePCollection = p.apply(Create.of("unused"));
      PCollectionView<String> singletonSideInputView = valuePCollection.apply(View.asSingleton());
      WindowObservingTestSplittableDoFn doFn =
          new WindowObservingTestSplittableDoFn(singletonSideInputView);
      valuePCollection.apply(
          TEST_TRANSFORM_ID, ParDo.of(doFn).withSideInputs(singletonSideInputView));

      RunnerApi.Pipeline pProto =
          ProtoOverrides.updateTransform(
              PTransformTranslation.PAR_DO_TRANSFORM_URN,
              PipelineTranslation.toProto(p, SdkComponents.create(p.getOptions()), true),
              SplittableParDoExpander.createSizedReplacement());
      String expandedTransformId =
          Iterables.find(
                  pProto.getComponents().getTransformsMap().entrySet(),
                  entry ->
                      entry
                              .getValue()
                              .getSpec()
                              .getUrn()
                              .equals(
                                  PTransformTranslation
                                      .SPLITTABLE_PROCESS_SIZED_ELEMENTS_AND_RESTRICTIONS_URN)
                          && entry.getValue().getUniqueName().contains(TEST_TRANSFORM_ID))
              .getKey();
      RunnerApi.PTransform pTransform =
          pProto.getComponents().getTransformsOrThrow(expandedTransformId);
      String inputPCollectionId =
          pTransform.getInputsOrThrow(ParDoTranslation.getMainInputName(pTransform));
      RunnerApi.PCollection inputPCollection =
          pProto.getComponents().getPcollectionsOrThrow(inputPCollectionId);
      RehydratedComponents rehydratedComponents =
          RehydratedComponents.forComponents(pProto.getComponents());
      Coder<WindowedValue> inputCoder =
          WindowedValue.getFullCoder(
              CoderTranslation.fromProto(
                  pProto.getComponents().getCodersOrThrow(inputPCollection.getCoderId()),
                  rehydratedComponents,
                  TranslationContext.DEFAULT),
              (Coder)
                  CoderTranslation.fromProto(
                      pProto
                          .getComponents()
                          .getCodersOrThrow(
                              pProto
                                  .getComponents()
                                  .getWindowingStrategiesOrThrow(
                                      inputPCollection.getWindowingStrategyId())
                                  .getWindowCoderId()),
                      rehydratedComponents,
                      TranslationContext.DEFAULT));
      String outputPCollectionId = pTransform.getOutputsOrThrow("output");

      ImmutableMap<StateKey, List<String>> stateData =
          ImmutableMap.of(
              iterableSideInputKey(
                  singletonSideInputView.getTagInternal().getId(), ByteString.EMPTY),
              asList("8"));

      FakeBeamFnStateClient fakeClient = new FakeBeamFnStateClient(StringUtf8Coder.of(), stateData);

      BundleSplitListener.InMemory splitListener = BundleSplitListener.InMemory.create();

      PTransformRunnerFactoryTestContext context =
          PTransformRunnerFactoryTestContext.builder(TEST_TRANSFORM_ID, pTransform)
              .beamFnStateClient(fakeClient)
              .processBundleInstructionId("57")
              .pCollections(pProto.getComponentsOrBuilder().getPcollectionsMap())
              .coders(pProto.getComponents().getCodersMap())
              .windowingStrategies(pProto.getComponents().getWindowingStrategiesMap())
              .splitListener(splitListener)
              .build();
      List<WindowedValue<String>> mainOutputValues = new ArrayList<>();
      context.addPCollectionConsumer(
          outputPCollectionId,
          (FnDataReceiver) (FnDataReceiver<WindowedValue<String>>) mainOutputValues::add);

      new FnApiDoFnRunner.Factory<>().createRunnerForPTransform(context);

      Iterables.getOnlyElement(context.getStartBundleFunctions()).run();
      mainOutputValues.clear();

      assertThat(
          context.getPCollectionConsumers().keySet(),
          containsInAnyOrder(inputPCollectionId, outputPCollectionId));

      FnDataReceiver<WindowedValue<?>> mainInput =
          context.getPCollectionConsumer(inputPCollectionId);
      assertThat(mainInput, instanceOf(HandlesSplits.class));

      doFn.setupBlockProcess();
      {
        // Setup and launch the trySplit thread.
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        Future<HandlesSplits.SplitResult> trySplitFuture =
            executorService.submit(
                () -> {
                  try {
                    // Verify that a split before anything is claimed is ignored.
                    doFn.waitForProcessEntered();
                    Assert.assertNull(((HandlesSplits) mainInput).trySplit(0));
                    doFn.unblockProcess();

                    doFn.waitForSplitElementToBeProcessed();
                    // Currently processing "3" out of range [0, 5) elements.
                    assertEquals(0.6, ((HandlesSplits) mainInput).getProgress(), 0.01);

                    // Check that during progressing of an element we report progress
                    assertReportedProgressEquals(
                        context.getShortIdMap(), context.getBundleProgressReporters(), 3.0, 2.0);

                    return ((HandlesSplits) mainInput).trySplit(0);
                  } finally {
                    doFn.trySplitPerformed();
                  }
                });

        // Check that before processing an element we don't report progress
        assertNoReportedProgress(context.getBundleProgressReporters());
        mainInput.accept(
            valueInGlobalWindow(
                KV.of(
                    KV.of("7", KV.of(new OffsetRange(0, 5), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                    2.0)));
        HandlesSplits.SplitResult trySplitResult = trySplitFuture.get();

        // Check that after processing an element we don't report progress
        assertNoReportedProgress(context.getBundleProgressReporters());

        // Since the SPLIT_ELEMENT is 3 we will process 0, 1, 2, 3 then be split.
        // We expect that the watermark advances to MIN + 2 since the manual watermark estimator
        // has yet to be invoked for the split element and that the primary represents [0, 4) with
        // the original watermark while the residual represents [4, 5) with the new MIN + 2
        // watermark.
        assertThat(
            mainOutputValues,
            contains(
                timestampedValueInGlobalWindow(
                    "7:0", GlobalWindow.TIMESTAMP_MIN_VALUE.plus(Duration.millis(0))),
                timestampedValueInGlobalWindow(
                    "7:1", GlobalWindow.TIMESTAMP_MIN_VALUE.plus(Duration.millis(1))),
                timestampedValueInGlobalWindow(
                    "7:2", GlobalWindow.TIMESTAMP_MIN_VALUE.plus(Duration.millis(2))),
                timestampedValueInGlobalWindow(
                    "7:3", GlobalWindow.TIMESTAMP_MIN_VALUE.plus(Duration.millis(3)))));

        BundleApplication primaryRoot = Iterables.getOnlyElement(trySplitResult.getPrimaryRoots());
        DelayedBundleApplication residualRoot =
            Iterables.getOnlyElement(trySplitResult.getResidualRoots());
        assertEquals(ParDoTranslation.getMainInputName(pTransform), primaryRoot.getInputId());
        assertEquals(TEST_TRANSFORM_ID, primaryRoot.getTransformId());
        assertEquals(
            ParDoTranslation.getMainInputName(pTransform),
            residualRoot.getApplication().getInputId());
        assertEquals(TEST_TRANSFORM_ID, residualRoot.getApplication().getTransformId());
        assertEquals(
            valueInGlobalWindow(
                KV.of(
                    KV.of("7", KV.of(new OffsetRange(0, 4), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                    4.0)),
            inputCoder.decode(primaryRoot.getElement().newInput()));
        assertEquals(
            valueInGlobalWindow(
                KV.of(
                    KV.of(
                        "7",
                        KV.of(
                            new OffsetRange(4, 5),
                            GlobalWindow.TIMESTAMP_MIN_VALUE.plus(Duration.millis(2)))),
                    1.0)),
            inputCoder.decode(residualRoot.getApplication().getElement().newInput()));
        Instant expectedOutputWatermark = GlobalWindow.TIMESTAMP_MIN_VALUE.plus(Duration.millis(2));
        assertEquals(
            ImmutableMap.of(
                "output",
                org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.Timestamp.newBuilder()
                    .setSeconds(expectedOutputWatermark.getMillis() / 1000)
                    .setNanos((int) (expectedOutputWatermark.getMillis() % 1000) * 1000000)
                    .build()),
            residualRoot.getApplication().getOutputWatermarksMap());
        // We expect 0 resume delay.
        assertEquals(
            residualRoot.getRequestedTimeDelay().getDefaultInstanceForType(),
            residualRoot.getRequestedTimeDelay());
        // We don't expect the outputs to goto the SDK initiated checkpointing listener.
        assertTrue(splitListener.getPrimaryRoots().isEmpty());
        assertTrue(splitListener.getResidualRoots().isEmpty());
        mainOutputValues.clear();
        executorService.shutdown();
      }

      Iterables.getOnlyElement(context.getFinishBundleFunctions()).run();
      assertThat(mainOutputValues, empty());

      Iterables.getOnlyElement(context.getTearDownFunctions()).run();
      assertThat(mainOutputValues, empty());

      // Assert that state data did not change
      assertEquals(
          new FakeBeamFnStateClient(StringUtf8Coder.of(), stateData).getData(),
          fakeClient.getData());
    }

    @Test
    public void testProcessElementForSizedElementAndRestrictionNoTryClaim() throws Exception {
      Pipeline p = Pipeline.create();
      addExperiment(p.getOptions().as(ExperimentalOptions.class), "beam_fn_api");
      // TODO(BEAM-10097): Remove experiment once all portable runners support this view type
      addExperiment(p.getOptions().as(ExperimentalOptions.class), "use_runner_v2");
      PCollection<String> valuePCollection = p.apply(Create.of("unused"));
      PCollectionView<String> singletonSideInputView = valuePCollection.apply(View.asSingleton());
      WindowObservingTestSplittableDoFn doFn =
          new WindowObservingTestSplittableDoFn(singletonSideInputView);
      doFn.setAbortProcessing();
      valuePCollection.apply(
          TEST_TRANSFORM_ID, ParDo.of(doFn).withSideInputs(singletonSideInputView));

      RunnerApi.Pipeline pProto =
          ProtoOverrides.updateTransform(
              PTransformTranslation.PAR_DO_TRANSFORM_URN,
              PipelineTranslation.toProto(p, SdkComponents.create(p.getOptions()), true),
              SplittableParDoExpander.createSizedReplacement());
      String expandedTransformId =
          Iterables.find(
                  pProto.getComponents().getTransformsMap().entrySet(),
                  entry ->
                      entry
                              .getValue()
                              .getSpec()
                              .getUrn()
                              .equals(
                                  PTransformTranslation
                                      .SPLITTABLE_PROCESS_SIZED_ELEMENTS_AND_RESTRICTIONS_URN)
                          && entry.getValue().getUniqueName().contains(TEST_TRANSFORM_ID))
              .getKey();
      RunnerApi.PTransform pTransform =
          pProto.getComponents().getTransformsOrThrow(expandedTransformId);
      String inputPCollectionId =
          pTransform.getInputsOrThrow(ParDoTranslation.getMainInputName(pTransform));
      RunnerApi.PCollection inputPCollection =
          pProto.getComponents().getPcollectionsOrThrow(inputPCollectionId);
      RehydratedComponents rehydratedComponents =
          RehydratedComponents.forComponents(pProto.getComponents());
      Coder<WindowedValue> inputCoder =
          WindowedValue.getFullCoder(
              CoderTranslation.fromProto(
                  pProto.getComponents().getCodersOrThrow(inputPCollection.getCoderId()),
                  rehydratedComponents,
                  TranslationContext.DEFAULT),
              (Coder)
                  CoderTranslation.fromProto(
                      pProto
                          .getComponents()
                          .getCodersOrThrow(
                              pProto
                                  .getComponents()
                                  .getWindowingStrategiesOrThrow(
                                      inputPCollection.getWindowingStrategyId())
                                  .getWindowCoderId()),
                      rehydratedComponents,
                      TranslationContext.DEFAULT));
      String outputPCollectionId = pTransform.getOutputsOrThrow("output");

      ImmutableMap<StateKey, List<String>> stateData =
          ImmutableMap.of(
              iterableSideInputKey(
                  singletonSideInputView.getTagInternal().getId(), ByteString.EMPTY),
              asList("8"));

      FakeBeamFnStateClient fakeClient = new FakeBeamFnStateClient(StringUtf8Coder.of(), stateData);

      BundleSplitListener.InMemory splitListener = BundleSplitListener.InMemory.create();

      PTransformRunnerFactoryTestContext context =
          PTransformRunnerFactoryTestContext.builder(TEST_TRANSFORM_ID, pTransform)
              .beamFnStateClient(fakeClient)
              .processBundleInstructionId("57")
              .pCollections(pProto.getComponentsOrBuilder().getPcollectionsMap())
              .coders(pProto.getComponents().getCodersMap())
              .windowingStrategies(pProto.getComponents().getWindowingStrategiesMap())
              .splitListener(splitListener)
              .build();
      List<WindowedValue<String>> mainOutputValues = new ArrayList<>();
      context.addPCollectionConsumer(
          outputPCollectionId,
          (FnDataReceiver) (FnDataReceiver<WindowedValue<String>>) mainOutputValues::add);

      new FnApiDoFnRunner.Factory<>().createRunnerForPTransform(context);

      Iterables.getOnlyElement(context.getStartBundleFunctions()).run();
      mainOutputValues.clear();

      assertThat(
          context.getPCollectionConsumers().keySet(),
          containsInAnyOrder(inputPCollectionId, outputPCollectionId));

      FnDataReceiver<WindowedValue<?>> mainInput =
          context.getPCollectionConsumer(inputPCollectionId);
      assertThat(mainInput, instanceOf(HandlesSplits.class));

      {
        // Check that before processing an element we don't report progress
        assertNoReportedProgress(context.getBundleProgressReporters());
        mainInput.accept(
            valueInGlobalWindow(
                KV.of(
                    KV.of("5", KV.of(new OffsetRange(5, 10), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                    5.0)));
        // Check that after processing an element we don't report progress
        assertNoReportedProgress(context.getBundleProgressReporters());

        // Since we set abort processing above, we expect the input restriction to be output with a
        // resume
        // delay.
        BundleApplication primaryRoot = Iterables.getOnlyElement(splitListener.getPrimaryRoots());
        DelayedBundleApplication residualRoot =
            Iterables.getOnlyElement(splitListener.getResidualRoots());
        assertEquals(ParDoTranslation.getMainInputName(pTransform), primaryRoot.getInputId());
        assertEquals(TEST_TRANSFORM_ID, primaryRoot.getTransformId());
        assertEquals(
            ParDoTranslation.getMainInputName(pTransform),
            residualRoot.getApplication().getInputId());
        assertEquals(TEST_TRANSFORM_ID, residualRoot.getApplication().getTransformId());
        assertEquals(
            valueInGlobalWindow(
                KV.of(
                    KV.of("5", KV.of(new OffsetRange(5, 5), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                    0.0)),
            inputCoder.decode(primaryRoot.getElement().newInput()));
        assertEquals(
            valueInGlobalWindow(
                KV.of(
                    KV.of("5", KV.of(new OffsetRange(5, 10), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                    5.0)),
            inputCoder.decode(residualRoot.getApplication().getElement().newInput()));
        assertThat(residualRoot.getApplication().getOutputWatermarksMap(), anEmptyMap());
        assertEquals(
            org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.Duration.newBuilder()
                .setSeconds(54)
                .setNanos(321000000)
                .build(),
            residualRoot.getRequestedTimeDelay());
        splitListener.clear();
      }
    }

    private static final MonitoringInfo WORK_COMPLETED_MI =
        MonitoringInfo.newBuilder()
            .setUrn(MonitoringInfoConstants.Urns.WORK_COMPLETED)
            .setType(MonitoringInfoConstants.TypeUrns.PROGRESS_TYPE)
            .putLabels(MonitoringInfoConstants.Labels.PTRANSFORM, TEST_TRANSFORM_ID)
            .build();

    private static final MonitoringInfo WORK_REMAINING_MI =
        MonitoringInfo.newBuilder()
            .setUrn(MonitoringInfoConstants.Urns.WORK_REMAINING)
            .setType(MonitoringInfoConstants.TypeUrns.PROGRESS_TYPE)
            .putLabels(MonitoringInfoConstants.Labels.PTRANSFORM, TEST_TRANSFORM_ID)
            .build();

    private static void assertNoReportedProgress(List<BundleProgressReporter> reporters) {
      Map<String, ByteString> monitoringData = new HashMap<>();
      for (BundleProgressReporter reporter : reporters) {
        reporter.updateIntermediateMonitoringData(monitoringData);
      }
      assertThat(monitoringData.entrySet(), empty());
    }

    private static void assertReportedProgressEquals(
        ShortIdMap shortIdMap,
        List<BundleProgressReporter> reporters,
        double expectedWorkCompleted,
        double expectedWorkRemaining)
        throws Exception {
      Map<String, ByteString> monitoringData = new HashMap<>();
      for (BundleProgressReporter reporter : reporters) {
        reporter.updateIntermediateMonitoringData(monitoringData);
      }
      String workCompletedShortId = shortIdMap.getOrCreateShortId(WORK_COMPLETED_MI);
      String workRemainingShortId = shortIdMap.getOrCreateShortId(WORK_REMAINING_MI);
      assertTrue(monitoringData.containsKey(workCompletedShortId));
      assertTrue(monitoringData.containsKey(workRemainingShortId));
      assertEquals(
          ByteString.copyFrom(
              CoderUtils.encodeToByteArray(
                  IterableCoder.of(DoubleCoder.of()),
                  Collections.singletonList(expectedWorkCompleted))),
          monitoringData.get(workCompletedShortId));

      assertEquals(
          ByteString.copyFrom(
              CoderUtils.encodeToByteArray(
                  IterableCoder.of(DoubleCoder.of()),
                  Collections.singletonList(expectedWorkRemaining))),
          monitoringData.get(workRemainingShortId));
    }

    @Test
    public void testProcessElementForWindowedSizedElementAndRestriction() throws Exception {
      Pipeline p = Pipeline.create();
      addExperiment(p.getOptions().as(ExperimentalOptions.class), "beam_fn_api");
      // TODO(BEAM-10097): Remove experiment once all portable runners support this view type
      addExperiment(p.getOptions().as(ExperimentalOptions.class), "use_runner_v2");
      PCollection<String> valuePCollection = p.apply(Create.of("unused"));
      PCollectionView<String> singletonSideInputView = valuePCollection.apply(View.asSingleton());
      WindowObservingTestSplittableDoFn doFn =
          new WindowObservingTestSplittableDoFn(singletonSideInputView);

      valuePCollection
          .apply(Window.into(SlidingWindows.of(Duration.standardSeconds(1))))
          .apply(TEST_TRANSFORM_ID, ParDo.of(doFn).withSideInputs(singletonSideInputView));

      RunnerApi.Pipeline pProto =
          ProtoOverrides.updateTransform(
              PTransformTranslation.PAR_DO_TRANSFORM_URN,
              PipelineTranslation.toProto(p, SdkComponents.create(p.getOptions()), true),
              SplittableParDoExpander.createSizedReplacement());
      String expandedTransformId =
          Iterables.find(
                  pProto.getComponents().getTransformsMap().entrySet(),
                  entry ->
                      entry
                              .getValue()
                              .getSpec()
                              .getUrn()
                              .equals(
                                  PTransformTranslation
                                      .SPLITTABLE_PROCESS_SIZED_ELEMENTS_AND_RESTRICTIONS_URN)
                          && entry.getValue().getUniqueName().contains(TEST_TRANSFORM_ID))
              .getKey();
      RunnerApi.PTransform pTransform =
          pProto.getComponents().getTransformsOrThrow(expandedTransformId);
      String inputPCollectionId =
          pTransform.getInputsOrThrow(ParDoTranslation.getMainInputName(pTransform));
      RunnerApi.PCollection inputPCollection =
          pProto.getComponents().getPcollectionsOrThrow(inputPCollectionId);
      RehydratedComponents rehydratedComponents =
          RehydratedComponents.forComponents(pProto.getComponents());
      Coder<WindowedValue> inputCoder =
          WindowedValue.getFullCoder(
              CoderTranslation.fromProto(
                  pProto.getComponents().getCodersOrThrow(inputPCollection.getCoderId()),
                  rehydratedComponents,
                  TranslationContext.DEFAULT),
              (Coder)
                  CoderTranslation.fromProto(
                      pProto
                          .getComponents()
                          .getCodersOrThrow(
                              pProto
                                  .getComponents()
                                  .getWindowingStrategiesOrThrow(
                                      inputPCollection.getWindowingStrategyId())
                                  .getWindowCoderId()),
                      rehydratedComponents,
                      TranslationContext.DEFAULT));
      String outputPCollectionId = pTransform.getOutputsOrThrow("output");

      ImmutableMap<StateKey, List<String>> stateData =
          ImmutableMap.of(
              iterableSideInputKey(
                  singletonSideInputView.getTagInternal().getId(), ByteString.EMPTY),
              asList("8"));

      FakeBeamFnStateClient fakeClient = new FakeBeamFnStateClient(StringUtf8Coder.of(), stateData);

      BundleSplitListener.InMemory splitListener = BundleSplitListener.InMemory.create();
      PTransformRunnerFactoryTestContext context =
          PTransformRunnerFactoryTestContext.builder(TEST_TRANSFORM_ID, pTransform)
              .beamFnStateClient(fakeClient)
              .processBundleInstructionId("57")
              .pCollections(pProto.getComponentsOrBuilder().getPcollectionsMap())
              .coders(pProto.getComponents().getCodersMap())
              .windowingStrategies(pProto.getComponents().getWindowingStrategiesMap())
              .splitListener(splitListener)
              .build();
      List<WindowedValue<String>> mainOutputValues = new ArrayList<>();
      context.addPCollectionConsumer(
          outputPCollectionId,
          (FnDataReceiver) (FnDataReceiver<WindowedValue<String>>) mainOutputValues::add);

      new FnApiDoFnRunner.Factory<>().createRunnerForPTransform(context);

      Iterables.getOnlyElement(context.getStartBundleFunctions()).run();
      mainOutputValues.clear();

      assertThat(
          context.getPCollectionConsumers().keySet(),
          containsInAnyOrder(inputPCollectionId, outputPCollectionId));

      FnDataReceiver<WindowedValue<?>> mainInput =
          context.getPCollectionConsumer(inputPCollectionId);
      assertThat(mainInput, instanceOf(HandlesSplits.class));

      BoundedWindow window1 = new IntervalWindow(new Instant(5), new Instant(10));
      BoundedWindow window2 = new IntervalWindow(new Instant(6), new Instant(11));
      {
        // Check that before processing an element we don't report progress
        assertNoReportedProgress(context.getBundleProgressReporters());
        WindowedValue<?> firstValue =
            valueInWindows(
                KV.of(
                    KV.of(
                        "5",
                        KV.of(
                            new OffsetRange(5, 10),
                            GlobalWindow.TIMESTAMP_MIN_VALUE.plus(Duration.millis(1)))),
                    5.0),
                window1,
                window2);
        mainInput.accept(firstValue);
        // Check that after processing an element we don't report progress
        assertNoReportedProgress(context.getBundleProgressReporters());

        // Since the side input upperBound is 8 we will process 5, 6, and 7 then checkpoint.
        // We expect that the watermark advances to MIN + 7 and that the primary represents [5, 8)
        // with the original watermark while the residual represents [8, 10) with the new MIN + 7
        // watermark.
        //
        // Since we were on the first window, we expect only a single primary root and two residual
        // roots (the split + the unprocessed window).
        BundleApplication primaryRoot = Iterables.getOnlyElement(splitListener.getPrimaryRoots());
        assertEquals(2, splitListener.getResidualRoots().size());
        DelayedBundleApplication residualRoot = splitListener.getResidualRoots().get(1);
        DelayedBundleApplication residualRootForUnprocessedWindows =
            splitListener.getResidualRoots().get(0);
        assertEquals(ParDoTranslation.getMainInputName(pTransform), primaryRoot.getInputId());
        assertEquals(TEST_TRANSFORM_ID, primaryRoot.getTransformId());
        assertEquals(
            ParDoTranslation.getMainInputName(pTransform),
            residualRoot.getApplication().getInputId());
        assertEquals(TEST_TRANSFORM_ID, residualRoot.getApplication().getTransformId());
        Instant expectedOutputWatermark = GlobalWindow.TIMESTAMP_MIN_VALUE.plus(Duration.millis(7));
        Map<String, org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.Timestamp>
            expectedOutputWatmermarkMap =
                ImmutableMap.of(
                    "output",
                    org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.Timestamp.newBuilder()
                        .setSeconds(expectedOutputWatermark.getMillis() / 1000)
                        .setNanos((int) (expectedOutputWatermark.getMillis() % 1000) * 1000000)
                        .build());
        Instant initialWatermark = GlobalWindow.TIMESTAMP_MIN_VALUE.plus(Duration.millis(1));
        Map<String, org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.Timestamp>
            expectedOutputWatmermarkMapForUnprocessedWindows =
                ImmutableMap.of(
                    "output",
                    org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.Timestamp.newBuilder()
                        .setSeconds(initialWatermark.getMillis() / 1000)
                        .setNanos((int) (initialWatermark.getMillis() % 1000) * 1000000)
                        .build());
        assertEquals(
            expectedOutputWatmermarkMap, residualRoot.getApplication().getOutputWatermarksMap());
        assertEquals(
            org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.Duration.newBuilder()
                .setSeconds(54)
                .setNanos(321000000)
                .build(),
            residualRoot.getRequestedTimeDelay());
        assertEquals(
            ParDoTranslation.getMainInputName(pTransform),
            residualRootForUnprocessedWindows.getApplication().getInputId());
        assertEquals(
            TEST_TRANSFORM_ID, residualRootForUnprocessedWindows.getApplication().getTransformId());
        assertEquals(
            residualRootForUnprocessedWindows.getRequestedTimeDelay().getDefaultInstanceForType(),
            residualRootForUnprocessedWindows.getRequestedTimeDelay());
        assertEquals(
            expectedOutputWatmermarkMapForUnprocessedWindows,
            residualRootForUnprocessedWindows.getApplication().getOutputWatermarksMap());

        assertEquals(
            decode(inputCoder, primaryRoot.getElement()),
            WindowedValue.of(
                KV.of(
                    KV.of(
                        "5",
                        KV.of(
                            new OffsetRange(5, 8),
                            GlobalWindow.TIMESTAMP_MIN_VALUE.plus(Duration.millis(1)))),
                    3.0),
                firstValue.getTimestamp(),
                window1,
                firstValue.getPane()));
        assertEquals(
            decode(inputCoder, residualRoot.getApplication().getElement()),
            WindowedValue.of(
                KV.of(
                    KV.of(
                        "5",
                        KV.of(
                            new OffsetRange(8, 10),
                            GlobalWindow.TIMESTAMP_MIN_VALUE.plus(Duration.millis(7)))),
                    2.0),
                firstValue.getTimestamp(),
                window1,
                firstValue.getPane()));
        assertEquals(
            decode(inputCoder, residualRootForUnprocessedWindows.getApplication().getElement()),
            WindowedValue.of(
                KV.of(
                    KV.of(
                        "5",
                        KV.of(
                            new OffsetRange(5, 10),
                            GlobalWindow.TIMESTAMP_MIN_VALUE.plus(Duration.millis(1)))),
                    5.0),
                firstValue.getTimestamp(),
                window2,
                firstValue.getPane()));
        splitListener.clear();

        // Check that before processing an element we don't report progress
        assertNoReportedProgress(context.getBundleProgressReporters());
        WindowedValue<?> secondValue =
            valueInWindows(
                KV.of(
                    KV.of(
                        "2",
                        KV.of(
                            new OffsetRange(0, 2),
                            GlobalWindow.TIMESTAMP_MIN_VALUE.plus(Duration.millis(1)))),
                    2.0),
                window1,
                window2);
        mainInput.accept(secondValue);
        // Check that after processing an element we don't report progress
        assertNoReportedProgress(context.getBundleProgressReporters());

        assertThat(
            mainOutputValues,
            contains(
                WindowedValue.of(
                    "5:5",
                    GlobalWindow.TIMESTAMP_MIN_VALUE.plus(Duration.millis(5)),
                    window1,
                    firstValue.getPane()),
                WindowedValue.of(
                    "5:6",
                    GlobalWindow.TIMESTAMP_MIN_VALUE.plus(Duration.millis(6)),
                    window1,
                    firstValue.getPane()),
                WindowedValue.of(
                    "5:7",
                    GlobalWindow.TIMESTAMP_MIN_VALUE.plus(Duration.millis(7)),
                    window1,
                    firstValue.getPane()),
                WindowedValue.of(
                    "2:0",
                    GlobalWindow.TIMESTAMP_MIN_VALUE.plus(Duration.millis(0)),
                    window1,
                    firstValue.getPane()),
                WindowedValue.of(
                    "2:1",
                    GlobalWindow.TIMESTAMP_MIN_VALUE.plus(Duration.millis(1)),
                    window1,
                    firstValue.getPane()),
                WindowedValue.of(
                    "2:0",
                    GlobalWindow.TIMESTAMP_MIN_VALUE.plus(Duration.millis(0)),
                    window2,
                    firstValue.getPane()),
                WindowedValue.of(
                    "2:1",
                    GlobalWindow.TIMESTAMP_MIN_VALUE.plus(Duration.millis(1)),
                    window2,
                    firstValue.getPane())));
        assertTrue(splitListener.getPrimaryRoots().isEmpty());
        assertTrue(splitListener.getResidualRoots().isEmpty());
        mainOutputValues.clear();
      }

      {
        // Setup and launch the trySplit thread.
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        Future<HandlesSplits.SplitResult> trySplitFuture =
            executorService.submit(
                () -> {
                  try {
                    doFn.waitForSplitElementToBeProcessed();
                    // Currently processing "3" out of range [0, 5) elements for the first window.
                    assertEquals(0.3, ((HandlesSplits) mainInput).getProgress(), 0.01);

                    // Check that during progressing of an element we report progress
                    assertReportedProgressEquals(
                        context.getShortIdMap(), context.getBundleProgressReporters(), 3.0, 7.0);

                    return ((HandlesSplits) mainInput).trySplit(0);
                  } finally {
                    doFn.trySplitPerformed();
                  }
                });

        // Check that before processing an element we don't report progress
        assertNoReportedProgress(context.getBundleProgressReporters());
        WindowedValue<?> splitValue =
            valueInWindows(
                KV.of(
                    KV.of(
                        "7",
                        KV.of(
                            new OffsetRange(0, 5),
                            GlobalWindow.TIMESTAMP_MIN_VALUE.plus(Duration.millis(1)))),
                    2.0),
                window1,
                window2);
        mainInput.accept(splitValue);
        HandlesSplits.SplitResult trySplitResult = trySplitFuture.get();

        // Check that after processing an element we don't report progress

        assertNoReportedProgress(context.getBundleProgressReporters());

        // Since the SPLIT_ELEMENT is 3 we will process 0, 1, 2, 3 then be split on the first
        // window.
        // We expect that the watermark advances to MIN + 2 since the manual watermark estimator
        // has yet to be invoked for the split element and that the primary represents [0, 4) with
        // the original watermark while the residual represents [4, 5) with the new MIN + 2
        // watermark.
        //
        // We expect to see none of the output for the second window.
        assertThat(
            mainOutputValues,
            contains(
                WindowedValue.of(
                    "7:0",
                    GlobalWindow.TIMESTAMP_MIN_VALUE.plus(Duration.millis(0)),
                    window1,
                    splitValue.getPane()),
                WindowedValue.of(
                    "7:1",
                    GlobalWindow.TIMESTAMP_MIN_VALUE.plus(Duration.millis(1)),
                    window1,
                    splitValue.getPane()),
                WindowedValue.of(
                    "7:2",
                    GlobalWindow.TIMESTAMP_MIN_VALUE.plus(Duration.millis(2)),
                    window1,
                    splitValue.getPane()),
                WindowedValue.of(
                    "7:3",
                    GlobalWindow.TIMESTAMP_MIN_VALUE.plus(Duration.millis(3)),
                    window1,
                    splitValue.getPane())));

        BundleApplication primaryRoot = Iterables.getOnlyElement(trySplitResult.getPrimaryRoots());
        assertEquals(2, trySplitResult.getResidualRoots().size());
        DelayedBundleApplication residualRoot = trySplitResult.getResidualRoots().get(1);
        DelayedBundleApplication residualRootInUnprocessedWindows =
            trySplitResult.getResidualRoots().get(0);
        assertEquals(ParDoTranslation.getMainInputName(pTransform), primaryRoot.getInputId());
        assertEquals(TEST_TRANSFORM_ID, primaryRoot.getTransformId());
        assertEquals(
            ParDoTranslation.getMainInputName(pTransform),
            residualRoot.getApplication().getInputId());
        assertEquals(TEST_TRANSFORM_ID, residualRoot.getApplication().getTransformId());
        assertEquals(
            TEST_TRANSFORM_ID, residualRootInUnprocessedWindows.getApplication().getTransformId());
        assertEquals(
            residualRootInUnprocessedWindows.getRequestedTimeDelay().getDefaultInstanceForType(),
            residualRootInUnprocessedWindows.getRequestedTimeDelay());
        Instant initialWatermark = GlobalWindow.TIMESTAMP_MIN_VALUE.plus(Duration.millis(1));
        Instant expectedOutputWatermark = GlobalWindow.TIMESTAMP_MIN_VALUE.plus(Duration.millis(2));
        Map<String, org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.Timestamp>
            expectedOutputWatermarkMapInUnprocessedResiduals =
                ImmutableMap.of(
                    "output",
                    org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.Timestamp.newBuilder()
                        .setSeconds(initialWatermark.getMillis() / 1000)
                        .setNanos((int) (initialWatermark.getMillis() % 1000) * 1000000)
                        .build());
        Map<String, org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.Timestamp>
            expectedOutputWatermarkMap =
                ImmutableMap.of(
                    "output",
                    org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.Timestamp.newBuilder()
                        .setSeconds(expectedOutputWatermark.getMillis() / 1000)
                        .setNanos((int) (expectedOutputWatermark.getMillis() % 1000) * 1000000)
                        .build());
        assertEquals(
            expectedOutputWatermarkMapInUnprocessedResiduals,
            residualRootInUnprocessedWindows.getApplication().getOutputWatermarksMap());
        assertEquals(
            valueInWindows(
                KV.of(
                    KV.of(
                        "7",
                        KV.of(
                            new OffsetRange(0, 4),
                            GlobalWindow.TIMESTAMP_MIN_VALUE.plus(Duration.millis(1)))),
                    4.0),
                window1),
            inputCoder.decode(primaryRoot.getElement().newInput()));
        assertEquals(
            valueInWindows(
                KV.of(
                    KV.of(
                        "7",
                        KV.of(
                            new OffsetRange(4, 5),
                            GlobalWindow.TIMESTAMP_MIN_VALUE.plus(Duration.millis(2)))),
                    1.0),
                window1),
            inputCoder.decode(residualRoot.getApplication().getElement().newInput()));
        assertEquals(
            expectedOutputWatermarkMap, residualRoot.getApplication().getOutputWatermarksMap());
        assertEquals(
            WindowedValue.of(
                KV.of(
                    KV.of(
                        "7",
                        KV.of(
                            new OffsetRange(0, 5),
                            GlobalWindow.TIMESTAMP_MIN_VALUE.plus(Duration.millis(1)))),
                    5.0),
                splitValue.getTimestamp(),
                window2,
                splitValue.getPane()),
            inputCoder.decode(
                residualRootInUnprocessedWindows.getApplication().getElement().newInput()));

        // We expect 0 resume delay.
        assertEquals(
            residualRoot.getRequestedTimeDelay().getDefaultInstanceForType(),
            residualRoot.getRequestedTimeDelay());
        // We don't expect the outputs to goto the SDK initiated checkpointing listener.
        assertTrue(splitListener.getPrimaryRoots().isEmpty());
        assertTrue(splitListener.getResidualRoots().isEmpty());
        mainOutputValues.clear();
        executorService.shutdown();
      }

      Iterables.getOnlyElement(context.getFinishBundleFunctions()).run();
      assertThat(mainOutputValues, empty());

      Iterables.getOnlyElement(context.getTearDownFunctions()).run();
      assertThat(mainOutputValues, empty());

      // Assert that state data did not change
      assertEquals(
          new FakeBeamFnStateClient(StringUtf8Coder.of(), stateData).getData(),
          fakeClient.getData());
    }

    private static <T> T decode(Coder<T> coder, ByteString value) {
      try {
        return coder.decode(value.newInput());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Test
    public void testProcessElementForPairWithRestriction() throws Exception {
      Pipeline p = Pipeline.create();
      PCollection<String> valuePCollection = p.apply(Create.of("unused"));
      PCollectionView<String> singletonSideInputView = valuePCollection.apply(View.asSingleton());
      valuePCollection.apply(
          TEST_TRANSFORM_ID,
          ParDo.of(new WindowObservingTestSplittableDoFn(singletonSideInputView))
              .withSideInputs(singletonSideInputView));
      RunnerApi.Pipeline pProto =
          ProtoOverrides.updateTransform(
              PTransformTranslation.PAR_DO_TRANSFORM_URN,
              PipelineTranslation.toProto(p, SdkComponents.create(p.getOptions()), true),
              SplittableParDoExpander.createSizedReplacement());
      String expandedTransformId =
          Iterables.find(
                  pProto.getComponents().getTransformsMap().entrySet(),
                  entry ->
                      entry
                              .getValue()
                              .getSpec()
                              .getUrn()
                              .equals(PTransformTranslation.SPLITTABLE_PAIR_WITH_RESTRICTION_URN)
                          && entry.getValue().getUniqueName().contains(TEST_TRANSFORM_ID))
              .getKey();
      RunnerApi.PTransform pTransform =
          pProto.getComponents().getTransformsOrThrow(expandedTransformId);
      String inputPCollectionId =
          pTransform.getInputsOrThrow(ParDoTranslation.getMainInputName(pTransform));
      String outputPCollectionId = Iterables.getOnlyElement(pTransform.getOutputsMap().values());

      FakeBeamFnStateClient fakeClient =
          new FakeBeamFnStateClient(StringUtf8Coder.of(), ImmutableMap.of());

      PTransformRunnerFactoryTestContext context =
          PTransformRunnerFactoryTestContext.builder(TEST_TRANSFORM_ID, pTransform)
              .beamFnStateClient(fakeClient)
              .processBundleInstructionId("57")
              .pCollections(pProto.getComponentsOrBuilder().getPcollectionsMap())
              .coders(pProto.getComponents().getCodersMap())
              .windowingStrategies(pProto.getComponents().getWindowingStrategiesMap())
              .build();
      List<WindowedValue<KV<String, OffsetRange>>> mainOutputValues = new ArrayList<>();
      context.addPCollectionConsumer(outputPCollectionId, ((List) mainOutputValues)::add);

      new FnApiDoFnRunner.Factory<>().createRunnerForPTransform(context);

      assertTrue(context.getStartBundleFunctions().isEmpty());
      mainOutputValues.clear();

      assertThat(
          context.getPCollectionConsumers().keySet(),
          containsInAnyOrder(inputPCollectionId, outputPCollectionId));

      FnDataReceiver<WindowedValue<?>> mainInput =
          context.getPCollectionConsumer(inputPCollectionId);
      mainInput.accept(valueInGlobalWindow("5"));
      mainInput.accept(valueInGlobalWindow("2"));
      assertThat(
          mainOutputValues,
          contains(
              valueInGlobalWindow(
                  KV.of(
                      "5",
                      KV.of(
                          new OffsetRange(0, 5),
                          GlobalWindow.TIMESTAMP_MIN_VALUE.plus(Duration.millis(1))))),
              valueInGlobalWindow(
                  KV.of(
                      "2",
                      KV.of(
                          new OffsetRange(0, 2),
                          GlobalWindow.TIMESTAMP_MIN_VALUE.plus(Duration.millis(1)))))));
      mainOutputValues.clear();

      assertTrue(context.getFinishBundleFunctions().isEmpty());
      assertThat(mainOutputValues, empty());

      Iterables.getOnlyElement(context.getTearDownFunctions()).run();
      assertThat(mainOutputValues, empty());
    }

    @Test
    public void testProcessElementForWindowedPairWithRestriction() throws Exception {
      Pipeline p = Pipeline.create();
      PCollection<String> valuePCollection = p.apply(Create.of("unused"));
      PCollectionView<String> singletonSideInputView = valuePCollection.apply(View.asSingleton());
      valuePCollection
          .apply(Window.into(SlidingWindows.of(Duration.standardSeconds(1))))
          .apply(
              TEST_TRANSFORM_ID,
              ParDo.of(new WindowObservingTestSplittableDoFn(singletonSideInputView))
                  .withSideInputs(singletonSideInputView));

      RunnerApi.Pipeline pProto =
          ProtoOverrides.updateTransform(
              PTransformTranslation.PAR_DO_TRANSFORM_URN,
              PipelineTranslation.toProto(p, SdkComponents.create(p.getOptions()), true),
              SplittableParDoExpander.createSizedReplacement());
      String expandedTransformId =
          Iterables.find(
                  pProto.getComponents().getTransformsMap().entrySet(),
                  entry ->
                      entry
                              .getValue()
                              .getSpec()
                              .getUrn()
                              .equals(PTransformTranslation.SPLITTABLE_PAIR_WITH_RESTRICTION_URN)
                          && entry.getValue().getUniqueName().contains(TEST_TRANSFORM_ID))
              .getKey();
      RunnerApi.PTransform pTransform =
          pProto.getComponents().getTransformsOrThrow(expandedTransformId);
      String inputPCollectionId =
          pTransform.getInputsOrThrow(ParDoTranslation.getMainInputName(pTransform));
      String outputPCollectionId = Iterables.getOnlyElement(pTransform.getOutputsMap().values());

      FakeBeamFnStateClient fakeClient =
          new FakeBeamFnStateClient(StringUtf8Coder.of(), ImmutableMap.of());

      PTransformRunnerFactoryTestContext context =
          PTransformRunnerFactoryTestContext.builder(TEST_TRANSFORM_ID, pTransform)
              .beamFnStateClient(fakeClient)
              .processBundleInstructionId("57")
              .pCollections(pProto.getComponentsOrBuilder().getPcollectionsMap())
              .coders(pProto.getComponents().getCodersMap())
              .windowingStrategies(pProto.getComponents().getWindowingStrategiesMap())
              .build();
      List<WindowedValue<KV<String, OffsetRange>>> mainOutputValues = new ArrayList<>();
      context.addPCollectionConsumer(outputPCollectionId, ((List) mainOutputValues)::add);

      new FnApiDoFnRunner.Factory<>().createRunnerForPTransform(context);

      assertTrue(context.getStartBundleFunctions().isEmpty());
      mainOutputValues.clear();

      assertThat(
          context.getPCollectionConsumers().keySet(),
          containsInAnyOrder(inputPCollectionId, outputPCollectionId));

      FnDataReceiver<WindowedValue<?>> mainInput =
          context.getPCollectionConsumer(inputPCollectionId);
      IntervalWindow window1 = new IntervalWindow(new Instant(5), new Instant(10));
      IntervalWindow window2 = new IntervalWindow(new Instant(6), new Instant(11));
      WindowedValue<?> firstValue = valueInWindows("5", window1, window2);
      WindowedValue<?> secondValue = valueInWindows("2", window1, window2);
      mainInput.accept(firstValue);
      mainInput.accept(secondValue);
      assertThat(
          mainOutputValues,
          contains(
              WindowedValue.of(
                  KV.of(
                      "5",
                      KV.of(
                          new OffsetRange(0, 5),
                          GlobalWindow.TIMESTAMP_MIN_VALUE.plus(Duration.millis(1)))),
                  firstValue.getTimestamp(),
                  window1,
                  firstValue.getPane()),
              WindowedValue.of(
                  KV.of(
                      "5",
                      KV.of(
                          new OffsetRange(0, 5),
                          GlobalWindow.TIMESTAMP_MIN_VALUE.plus(Duration.millis(1)))),
                  firstValue.getTimestamp(),
                  window2,
                  firstValue.getPane()),
              WindowedValue.of(
                  KV.of(
                      "2",
                      KV.of(
                          new OffsetRange(0, 2),
                          GlobalWindow.TIMESTAMP_MIN_VALUE.plus(Duration.millis(1)))),
                  secondValue.getTimestamp(),
                  window1,
                  secondValue.getPane()),
              WindowedValue.of(
                  KV.of(
                      "2",
                      KV.of(
                          new OffsetRange(0, 2),
                          GlobalWindow.TIMESTAMP_MIN_VALUE.plus(Duration.millis(1)))),
                  secondValue.getTimestamp(),
                  window2,
                  secondValue.getPane())));
      mainOutputValues.clear();

      assertTrue(context.getFinishBundleFunctions().isEmpty());
      assertThat(mainOutputValues, empty());

      Iterables.getOnlyElement(context.getTearDownFunctions()).run();
      assertThat(mainOutputValues, empty());
    }

    @Test
    public void testProcessElementForWindowedPairWithRestrictionWithNonWindowObservingOptimization()
        throws Exception {
      Pipeline p = Pipeline.create();
      PCollection<String> valuePCollection = p.apply(Create.of("unused"));
      valuePCollection.apply(View.asSingleton());
      valuePCollection
          .apply(Window.into(SlidingWindows.of(Duration.standardSeconds(1))))
          .apply(TEST_TRANSFORM_ID, ParDo.of(new NonWindowObservingTestSplittableDoFn()));

      RunnerApi.Pipeline pProto =
          ProtoOverrides.updateTransform(
              PTransformTranslation.PAR_DO_TRANSFORM_URN,
              PipelineTranslation.toProto(p, SdkComponents.create(p.getOptions()), true),
              SplittableParDoExpander.createSizedReplacement());
      String expandedTransformId =
          Iterables.find(
                  pProto.getComponents().getTransformsMap().entrySet(),
                  entry ->
                      entry
                              .getValue()
                              .getSpec()
                              .getUrn()
                              .equals(PTransformTranslation.SPLITTABLE_PAIR_WITH_RESTRICTION_URN)
                          && entry.getValue().getUniqueName().contains(TEST_TRANSFORM_ID))
              .getKey();
      RunnerApi.PTransform pTransform =
          pProto.getComponents().getTransformsOrThrow(expandedTransformId);
      String inputPCollectionId =
          pTransform.getInputsOrThrow(ParDoTranslation.getMainInputName(pTransform));
      String outputPCollectionId = Iterables.getOnlyElement(pTransform.getOutputsMap().values());

      FakeBeamFnStateClient fakeClient =
          new FakeBeamFnStateClient(StringUtf8Coder.of(), ImmutableMap.of());

      PTransformRunnerFactoryTestContext context =
          PTransformRunnerFactoryTestContext.builder(TEST_TRANSFORM_ID, pTransform)
              .beamFnStateClient(fakeClient)
              .processBundleInstructionId("57")
              .pCollections(pProto.getComponentsOrBuilder().getPcollectionsMap())
              .coders(pProto.getComponents().getCodersMap())
              .windowingStrategies(pProto.getComponents().getWindowingStrategiesMap())
              .build();
      List<WindowedValue<KV<String, OffsetRange>>> mainOutputValues = new ArrayList<>();
      context.addPCollectionConsumer(outputPCollectionId, ((List) mainOutputValues)::add);

      new FnApiDoFnRunner.Factory<>().createRunnerForPTransform(context);

      assertTrue(context.getStartBundleFunctions().isEmpty());
      mainOutputValues.clear();

      assertThat(
          context.getPCollectionConsumers().keySet(),
          containsInAnyOrder(inputPCollectionId, outputPCollectionId));

      FnDataReceiver<WindowedValue<?>> mainInput =
          context.getPCollectionConsumer(inputPCollectionId);
      IntervalWindow window1 = new IntervalWindow(new Instant(5), new Instant(10));
      IntervalWindow window2 = new IntervalWindow(new Instant(6), new Instant(11));
      WindowedValue<?> firstValue = valueInWindows("5", window1, window2);
      WindowedValue<?> secondValue = valueInWindows("2", window1, window2);
      mainInput.accept(firstValue);
      mainInput.accept(secondValue);
      // Ensure that each output element is in all the windows and not one per window.
      assertThat(
          mainOutputValues,
          contains(
              WindowedValue.of(
                  KV.of(
                      "5",
                      KV.of(
                          new OffsetRange(0, 5),
                          GlobalWindow.TIMESTAMP_MIN_VALUE.plus(Duration.millis(1)))),
                  firstValue.getTimestamp(),
                  ImmutableList.of(window1, window2),
                  firstValue.getPane()),
              WindowedValue.of(
                  KV.of(
                      "2",
                      KV.of(
                          new OffsetRange(0, 2),
                          GlobalWindow.TIMESTAMP_MIN_VALUE.plus(Duration.millis(1)))),
                  secondValue.getTimestamp(),
                  ImmutableList.of(window1, window2),
                  secondValue.getPane())));
      mainOutputValues.clear();

      assertTrue(context.getFinishBundleFunctions().isEmpty());
      assertThat(mainOutputValues, empty());

      Iterables.getOnlyElement(context.getTearDownFunctions()).run();
      assertThat(mainOutputValues, empty());
    }

    @Test
    public void testProcessElementForSplitAndSizeRestriction() throws Exception {
      Pipeline p = Pipeline.create();
      PCollection<String> valuePCollection = p.apply(Create.of("unused"));
      PCollectionView<String> singletonSideInputView = valuePCollection.apply(View.asSingleton());
      valuePCollection.apply(
          TEST_TRANSFORM_ID,
          ParDo.of(new WindowObservingTestSplittableDoFn(singletonSideInputView))
              .withSideInputs(singletonSideInputView));

      RunnerApi.Pipeline pProto =
          ProtoOverrides.updateTransform(
              PTransformTranslation.PAR_DO_TRANSFORM_URN,
              PipelineTranslation.toProto(p, SdkComponents.create(p.getOptions()), true),
              SplittableParDoExpander.createSizedReplacement());
      String expandedTransformId =
          Iterables.find(
                  pProto.getComponents().getTransformsMap().entrySet(),
                  entry ->
                      entry
                              .getValue()
                              .getSpec()
                              .getUrn()
                              .equals(
                                  PTransformTranslation.SPLITTABLE_SPLIT_AND_SIZE_RESTRICTIONS_URN)
                          && entry.getValue().getUniqueName().contains(TEST_TRANSFORM_ID))
              .getKey();
      RunnerApi.PTransform pTransform =
          pProto.getComponents().getTransformsOrThrow(expandedTransformId);
      String inputPCollectionId =
          pTransform.getInputsOrThrow(ParDoTranslation.getMainInputName(pTransform));
      String outputPCollectionId = Iterables.getOnlyElement(pTransform.getOutputsMap().values());

      FakeBeamFnStateClient fakeClient =
          new FakeBeamFnStateClient(StringUtf8Coder.of(), ImmutableMap.of());

      PTransformRunnerFactoryTestContext context =
          PTransformRunnerFactoryTestContext.builder(TEST_TRANSFORM_ID, pTransform)
              .beamFnStateClient(fakeClient)
              .processBundleInstructionId("57")
              .pCollections(pProto.getComponentsOrBuilder().getPcollectionsMap())
              .coders(pProto.getComponents().getCodersMap())
              .windowingStrategies(pProto.getComponents().getWindowingStrategiesMap())
              .build();
      List<WindowedValue<KV<KV<String, OffsetRange>, Double>>> mainOutputValues = new ArrayList<>();
      Coder coder =
          KvCoder.of(
              KvCoder.of(
                  StringUtf8Coder.of(), KvCoder.of(OffsetRange.Coder.of(), InstantCoder.of())),
              DoubleCoder.of());
      context.addPCollectionConsumer(outputPCollectionId, ((List) mainOutputValues)::add);

      new FnApiDoFnRunner.Factory<>().createRunnerForPTransform(context);

      assertTrue(context.getStartBundleFunctions().isEmpty());
      mainOutputValues.clear();

      assertThat(
          context.getPCollectionConsumers().keySet(),
          containsInAnyOrder(inputPCollectionId, outputPCollectionId));

      FnDataReceiver<WindowedValue<?>> mainInput =
          context.getPCollectionConsumer(inputPCollectionId);
      mainInput.accept(
          valueInGlobalWindow(
              KV.of("5", KV.of(new OffsetRange(0, 5), GlobalWindow.TIMESTAMP_MIN_VALUE))));
      mainInput.accept(
          valueInGlobalWindow(
              KV.of("2", KV.of(new OffsetRange(0, 2), GlobalWindow.TIMESTAMP_MIN_VALUE))));
      assertThat(
          mainOutputValues,
          contains(
              valueInGlobalWindow(
                  KV.of(
                      KV.of("5", KV.of(new OffsetRange(0, 2), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                      2.0)),
              valueInGlobalWindow(
                  KV.of(
                      KV.of("5", KV.of(new OffsetRange(2, 5), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                      3.0)),
              valueInGlobalWindow(
                  KV.of(
                      KV.of("2", KV.of(new OffsetRange(0, 1), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                      1.0)),
              valueInGlobalWindow(
                  KV.of(
                      KV.of("2", KV.of(new OffsetRange(1, 2), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                      1.0))));
      mainOutputValues.clear();

      assertTrue(context.getFinishBundleFunctions().isEmpty());
      assertThat(mainOutputValues, empty());

      Iterables.getOnlyElement(context.getTearDownFunctions()).run();
      assertThat(mainOutputValues, empty());
    }

    @Test
    public void testProcessElementForWindowedSplitAndSizeRestriction() throws Exception {
      Pipeline p = Pipeline.create();
      PCollection<String> valuePCollection = p.apply(Create.of("unused"));
      PCollectionView<String> singletonSideInputView = valuePCollection.apply(View.asSingleton());
      valuePCollection
          .apply(Window.into(SlidingWindows.of(Duration.standardSeconds(1))))
          .apply(
              TEST_TRANSFORM_ID,
              ParDo.of(new WindowObservingTestSplittableDoFn(singletonSideInputView))
                  .withSideInputs(singletonSideInputView));

      RunnerApi.Pipeline pProto =
          ProtoOverrides.updateTransform(
              PTransformTranslation.PAR_DO_TRANSFORM_URN,
              PipelineTranslation.toProto(p, SdkComponents.create(p.getOptions()), true),
              SplittableParDoExpander.createSizedReplacement());
      String expandedTransformId =
          Iterables.find(
                  pProto.getComponents().getTransformsMap().entrySet(),
                  entry ->
                      entry
                              .getValue()
                              .getSpec()
                              .getUrn()
                              .equals(
                                  PTransformTranslation.SPLITTABLE_SPLIT_AND_SIZE_RESTRICTIONS_URN)
                          && entry.getValue().getUniqueName().contains(TEST_TRANSFORM_ID))
              .getKey();
      RunnerApi.PTransform pTransform =
          pProto.getComponents().getTransformsOrThrow(expandedTransformId);
      String inputPCollectionId =
          pTransform.getInputsOrThrow(ParDoTranslation.getMainInputName(pTransform));
      String outputPCollectionId = Iterables.getOnlyElement(pTransform.getOutputsMap().values());

      FakeBeamFnStateClient fakeClient =
          new FakeBeamFnStateClient(StringUtf8Coder.of(), ImmutableMap.of());

      PTransformRunnerFactoryTestContext context =
          PTransformRunnerFactoryTestContext.builder(TEST_TRANSFORM_ID, pTransform)
              .beamFnStateClient(fakeClient)
              .processBundleInstructionId("57")
              .pCollections(pProto.getComponentsOrBuilder().getPcollectionsMap())
              .coders(pProto.getComponents().getCodersMap())
              .windowingStrategies(pProto.getComponents().getWindowingStrategiesMap())
              .build();
      List<WindowedValue<KV<KV<String, OffsetRange>, Double>>> mainOutputValues = new ArrayList<>();
      Coder coder =
          KvCoder.of(
              KvCoder.of(
                  StringUtf8Coder.of(), KvCoder.of(OffsetRange.Coder.of(), InstantCoder.of())),
              DoubleCoder.of());
      context.addPCollectionConsumer(outputPCollectionId, ((List) mainOutputValues)::add);

      new FnApiDoFnRunner.Factory<>().createRunnerForPTransform(context);

      assertTrue(context.getStartBundleFunctions().isEmpty());
      mainOutputValues.clear();

      assertThat(
          context.getPCollectionConsumers().keySet(),
          containsInAnyOrder(inputPCollectionId, outputPCollectionId));

      FnDataReceiver<WindowedValue<?>> mainInput =
          context.getPCollectionConsumer(inputPCollectionId);
      IntervalWindow window1 = new IntervalWindow(new Instant(5), new Instant(10));
      IntervalWindow window2 = new IntervalWindow(new Instant(6), new Instant(11));
      WindowedValue<?> firstValue =
          valueInWindows(
              KV.of("5", KV.of(new OffsetRange(0, 5), GlobalWindow.TIMESTAMP_MIN_VALUE)),
              window1,
              window2);
      WindowedValue<?> secondValue =
          valueInWindows(
              KV.of("2", KV.of(new OffsetRange(0, 2), GlobalWindow.TIMESTAMP_MIN_VALUE)),
              window1,
              window2);
      mainInput.accept(firstValue);
      mainInput.accept(secondValue);
      assertThat(
          mainOutputValues,
          contains(
              WindowedValue.of(
                  KV.of(
                      KV.of("5", KV.of(new OffsetRange(0, 2), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                      2.0),
                  firstValue.getTimestamp(),
                  window1,
                  firstValue.getPane()),
              WindowedValue.of(
                  KV.of(
                      KV.of("5", KV.of(new OffsetRange(2, 5), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                      3.0),
                  firstValue.getTimestamp(),
                  window1,
                  firstValue.getPane()),
              WindowedValue.of(
                  KV.of(
                      KV.of("5", KV.of(new OffsetRange(0, 2), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                      2.0),
                  firstValue.getTimestamp(),
                  window2,
                  firstValue.getPane()),
              WindowedValue.of(
                  KV.of(
                      KV.of("5", KV.of(new OffsetRange(2, 5), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                      3.0),
                  firstValue.getTimestamp(),
                  window2,
                  firstValue.getPane()),
              WindowedValue.of(
                  KV.of(
                      KV.of("2", KV.of(new OffsetRange(0, 1), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                      1.0),
                  firstValue.getTimestamp(),
                  window1,
                  firstValue.getPane()),
              WindowedValue.of(
                  KV.of(
                      KV.of("2", KV.of(new OffsetRange(1, 2), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                      1.0),
                  firstValue.getTimestamp(),
                  window1,
                  firstValue.getPane()),
              WindowedValue.of(
                  KV.of(
                      KV.of("2", KV.of(new OffsetRange(0, 1), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                      1.0),
                  firstValue.getTimestamp(),
                  window2,
                  firstValue.getPane()),
              WindowedValue.of(
                  KV.of(
                      KV.of("2", KV.of(new OffsetRange(1, 2), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                      1.0),
                  firstValue.getTimestamp(),
                  window2,
                  firstValue.getPane())));
      mainOutputValues.clear();

      assertTrue(context.getFinishBundleFunctions().isEmpty());
      assertThat(mainOutputValues, empty());

      Iterables.getOnlyElement(context.getTearDownFunctions()).run();
      assertThat(mainOutputValues, empty());
    }

    @Test
    public void
        testProcessElementForWindowedSplitAndSizeRestrictionWithNonWindowObservingOptimization()
            throws Exception {
      Pipeline p = Pipeline.create();
      PCollection<String> valuePCollection = p.apply(Create.of("unused"));
      valuePCollection
          .apply(Window.into(SlidingWindows.of(Duration.standardSeconds(1))))
          .apply(TEST_TRANSFORM_ID, ParDo.of(new NonWindowObservingTestSplittableDoFn()));

      RunnerApi.Pipeline pProto =
          ProtoOverrides.updateTransform(
              PTransformTranslation.PAR_DO_TRANSFORM_URN,
              PipelineTranslation.toProto(p, SdkComponents.create(p.getOptions()), true),
              SplittableParDoExpander.createSizedReplacement());
      String expandedTransformId =
          Iterables.find(
                  pProto.getComponents().getTransformsMap().entrySet(),
                  entry ->
                      entry
                              .getValue()
                              .getSpec()
                              .getUrn()
                              .equals(
                                  PTransformTranslation.SPLITTABLE_SPLIT_AND_SIZE_RESTRICTIONS_URN)
                          && entry.getValue().getUniqueName().contains(TEST_TRANSFORM_ID))
              .getKey();
      RunnerApi.PTransform pTransform =
          pProto.getComponents().getTransformsOrThrow(expandedTransformId);
      String inputPCollectionId =
          pTransform.getInputsOrThrow(ParDoTranslation.getMainInputName(pTransform));
      String outputPCollectionId = Iterables.getOnlyElement(pTransform.getOutputsMap().values());

      PTransformRunnerFactoryTestContext context =
          PTransformRunnerFactoryTestContext.builder(TEST_TRANSFORM_ID, pTransform)
              .processBundleInstructionId("57")
              .pCollections(pProto.getComponentsOrBuilder().getPcollectionsMap())
              .coders(pProto.getComponents().getCodersMap())
              .windowingStrategies(pProto.getComponents().getWindowingStrategiesMap())
              .build();
      List<WindowedValue<KV<KV<String, OffsetRange>, Double>>> mainOutputValues = new ArrayList<>();
      Coder coder =
          KvCoder.of(
              KvCoder.of(
                  StringUtf8Coder.of(), KvCoder.of(OffsetRange.Coder.of(), InstantCoder.of())),
              DoubleCoder.of());
      context.addPCollectionConsumer(outputPCollectionId, ((List) mainOutputValues)::add);

      new FnApiDoFnRunner.Factory<>().createRunnerForPTransform(context);

      assertTrue(context.getStartBundleFunctions().isEmpty());
      mainOutputValues.clear();

      assertThat(
          context.getPCollectionConsumers().keySet(),
          containsInAnyOrder(inputPCollectionId, outputPCollectionId));

      FnDataReceiver<WindowedValue<?>> mainInput =
          context.getPCollectionConsumer(inputPCollectionId);
      IntervalWindow window1 = new IntervalWindow(new Instant(5), new Instant(10));
      IntervalWindow window2 = new IntervalWindow(new Instant(6), new Instant(11));
      WindowedValue<?> firstValue =
          valueInWindows(
              KV.of("5", KV.of(new OffsetRange(0, 5), GlobalWindow.TIMESTAMP_MIN_VALUE)),
              window1,
              window2);
      WindowedValue<?> secondValue =
          valueInWindows(
              KV.of("2", KV.of(new OffsetRange(0, 2), GlobalWindow.TIMESTAMP_MIN_VALUE)),
              window1,
              window2);
      mainInput.accept(firstValue);
      mainInput.accept(secondValue);
      // Ensure that each output element is in all the windows and not one per window.
      assertThat(
          mainOutputValues,
          contains(
              WindowedValue.of(
                  KV.of(
                      KV.of("5", KV.of(new OffsetRange(0, 2), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                      2.0),
                  firstValue.getTimestamp(),
                  ImmutableList.of(window1, window2),
                  firstValue.getPane()),
              WindowedValue.of(
                  KV.of(
                      KV.of("5", KV.of(new OffsetRange(2, 5), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                      3.0),
                  firstValue.getTimestamp(),
                  ImmutableList.of(window1, window2),
                  firstValue.getPane()),
              WindowedValue.of(
                  KV.of(
                      KV.of("2", KV.of(new OffsetRange(0, 1), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                      1.0),
                  firstValue.getTimestamp(),
                  ImmutableList.of(window1, window2),
                  firstValue.getPane()),
              WindowedValue.of(
                  KV.of(
                      KV.of("2", KV.of(new OffsetRange(1, 2), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                      1.0),
                  firstValue.getTimestamp(),
                  ImmutableList.of(window1, window2),
                  firstValue.getPane())));
      mainOutputValues.clear();

      assertTrue(context.getFinishBundleFunctions().isEmpty());
      assertThat(mainOutputValues, empty());

      Iterables.getOnlyElement(context.getTearDownFunctions()).run();
      assertThat(mainOutputValues, empty());
    }

    private static SplitResult createSplitResult(double fractionOfRemainder) {
      ByteStringOutputStream primaryBytes = new ByteStringOutputStream();
      ByteStringOutputStream residualBytes = new ByteStringOutputStream();
      try {
        DoubleCoder.of().encode(fractionOfRemainder, primaryBytes);
        DoubleCoder.of().encode(1 - fractionOfRemainder, residualBytes);
      } catch (Exception e) {
        // No-op.
      }
      return SplitResult.of(
          ImmutableList.of(
              BundleApplication.newBuilder()
                  .setElement(primaryBytes.toByteString())
                  .setInputId("mainInputId-process")
                  .setTransformId("processPTransfromId")
                  .build()),
          ImmutableList.of(
              DelayedBundleApplication.newBuilder()
                  .setApplication(
                      BundleApplication.newBuilder()
                          .setElement(residualBytes.toByteString())
                          .setInputId("mainInputId-process")
                          .setTransformId("processPTransfromId")
                          .build())
                  .build()));
    }

    private static class SplittableFnDataReceiver
        implements HandlesSplits, FnDataReceiver<WindowedValue> {
      SplittableFnDataReceiver(
          List<WindowedValue<KV<KV<String, OffsetRange>, Double>>> mainOutputValues) {
        this.mainOutputValues = mainOutputValues;
      }

      private final List<WindowedValue<KV<KV<String, OffsetRange>, Double>>> mainOutputValues;

      @Override
      public SplitResult trySplit(double fractionOfRemainder) {
        return createSplitResult(fractionOfRemainder);
      }

      @Override
      public double getProgress() {
        return 0.7;
      }

      @Override
      public void accept(WindowedValue input) throws Exception {
        mainOutputValues.add(input);
      }
    }

    @Test
    public void testProcessElementForTruncateAndSizeRestrictionForwardSplitWhenObservingWindows()
        throws Exception {
      Pipeline p = Pipeline.create();
      PCollection<String> valuePCollection = p.apply(Create.of("unused"));
      PCollectionView<String> singletonSideInputView = valuePCollection.apply(View.asSingleton());
      WindowObservingTestSplittableDoFn doFn =
          WindowObservingTestSplittableDoFn.forSplitAtTruncate(singletonSideInputView);
      valuePCollection
          .apply(Window.into(SlidingWindows.of(Duration.standardSeconds(1))))
          .apply(TEST_TRANSFORM_ID, ParDo.of(doFn).withSideInputs(singletonSideInputView));

      RunnerApi.Pipeline pProto =
          ProtoOverrides.updateTransform(
              PTransformTranslation.PAR_DO_TRANSFORM_URN,
              PipelineTranslation.toProto(p, SdkComponents.create(p.getOptions()), true),
              SplittableParDoExpander.createTruncateReplacement());
      String expandedTransformId =
          Iterables.find(
                  pProto.getComponents().getTransformsMap().entrySet(),
                  entry ->
                      entry
                              .getValue()
                              .getSpec()
                              .getUrn()
                              .equals(
                                  PTransformTranslation.SPLITTABLE_TRUNCATE_SIZED_RESTRICTION_URN)
                          && entry.getValue().getUniqueName().contains(TEST_TRANSFORM_ID))
              .getKey();
      RunnerApi.PTransform pTransform =
          pProto.getComponents().getTransformsOrThrow(expandedTransformId);
      String inputPCollectionId =
          pTransform.getInputsOrThrow(ParDoTranslation.getMainInputName(pTransform));
      RunnerApi.PCollection inputPCollection =
          pProto.getComponents().getPcollectionsOrThrow(inputPCollectionId);
      RehydratedComponents rehydratedComponents =
          RehydratedComponents.forComponents(pProto.getComponents());
      Coder<WindowedValue> inputCoder =
          WindowedValue.getFullCoder(
              CoderTranslation.fromProto(
                  pProto.getComponents().getCodersOrThrow(inputPCollection.getCoderId()),
                  rehydratedComponents,
                  TranslationContext.DEFAULT),
              (Coder)
                  CoderTranslation.fromProto(
                      pProto
                          .getComponents()
                          .getCodersOrThrow(
                              pProto
                                  .getComponents()
                                  .getWindowingStrategiesOrThrow(
                                      inputPCollection.getWindowingStrategyId())
                                  .getWindowCoderId()),
                      rehydratedComponents,
                      TranslationContext.DEFAULT));

      String outputPCollectionId = Iterables.getOnlyElement(pTransform.getOutputsMap().values());

      FakeBeamFnStateClient fakeClient =
          new FakeBeamFnStateClient(StringUtf8Coder.of(), ImmutableMap.of());

      PTransformRunnerFactoryTestContext context =
          PTransformRunnerFactoryTestContext.builder(TEST_TRANSFORM_ID, pTransform)
              .beamFnStateClient(fakeClient)
              .processBundleInstructionId("57")
              .pCollections(pProto.getComponentsOrBuilder().getPcollectionsMap())
              .coders(pProto.getComponents().getCodersMap())
              .windowingStrategies(pProto.getComponents().getWindowingStrategiesMap())
              .build();
      List<WindowedValue<KV<KV<String, OffsetRange>, Double>>> mainOutputValues = new ArrayList<>();
      Coder coder =
          KvCoder.of(
              KvCoder.of(
                  StringUtf8Coder.of(), KvCoder.of(OffsetRange.Coder.of(), InstantCoder.of())),
              DoubleCoder.of());
      context.addPCollectionConsumer(
          outputPCollectionId, (FnDataReceiver) new SplittableFnDataReceiver(mainOutputValues));

      new FnApiDoFnRunner.Factory<>().createRunnerForPTransform(context);
      FnDataReceiver<WindowedValue<?>> mainInput =
          context.getPCollectionConsumer(inputPCollectionId);
      assertThat(mainInput, instanceOf(HandlesSplits.class));

      mainOutputValues.clear();
      BoundedWindow window1 = new IntervalWindow(new Instant(5), new Instant(10));
      BoundedWindow window2 = new IntervalWindow(new Instant(6), new Instant(11));
      BoundedWindow window3 = new IntervalWindow(new Instant(7), new Instant(12));
      // Setup and launch the trySplit thread.
      ExecutorService executorService = Executors.newSingleThreadExecutor();
      Future<HandlesSplits.SplitResult> trySplitFuture =
          executorService.submit(
              () -> {
                try {
                  doFn.waitForSplitElementToBeProcessed();
                  HandlesSplits.SplitResult result = ((HandlesSplits) mainInput).trySplit(0);
                  Assert.assertNotNull(result);
                  return result;
                } finally {
                  doFn.trySplitPerformed();
                }
              });

      WindowedValue<?> splitValue =
          valueInWindows(
              KV.of(
                  KV.of("7", KV.of(new OffsetRange(0, 6), GlobalWindow.TIMESTAMP_MIN_VALUE)), 6.0),
              window1,
              window2,
              window3);
      mainInput.accept(splitValue);
      HandlesSplits.SplitResult trySplitResult = trySplitFuture.get();

      // We expect that there are outputs from window1 and window2
      assertThat(
          mainOutputValues,
          contains(
              WindowedValue.of(
                  KV.of(
                      KV.of("7", KV.of(new OffsetRange(0, 3), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                      3.0),
                  splitValue.getTimestamp(),
                  window1,
                  splitValue.getPane()),
              WindowedValue.of(
                  KV.of(
                      KV.of("7", KV.of(new OffsetRange(0, 3), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                      3.0),
                  splitValue.getTimestamp(),
                  window2,
                  splitValue.getPane())));

      SplitResult expectedElementSplit = createSplitResult(0);
      BundleApplication expectedElementSplitPrimary =
          Iterables.getOnlyElement(expectedElementSplit.getPrimaryRoots());
      ByteStringOutputStream primaryBytes = new ByteStringOutputStream();
      inputCoder.encode(
          WindowedValue.of(
              KV.of(
                  KV.of("7", KV.of(new OffsetRange(0, 6), GlobalWindow.TIMESTAMP_MIN_VALUE)), 6.0),
              splitValue.getTimestamp(),
              window1,
              splitValue.getPane()),
          primaryBytes);
      BundleApplication expectedWindowedPrimary =
          BundleApplication.newBuilder()
              .setElement(primaryBytes.toByteString())
              .setInputId(ParDoTranslation.getMainInputName(pTransform))
              .setTransformId(TEST_TRANSFORM_ID)
              .build();
      DelayedBundleApplication expectedElementSplitResidual =
          Iterables.getOnlyElement(expectedElementSplit.getResidualRoots());
      ByteStringOutputStream residualBytes = new ByteStringOutputStream();
      inputCoder.encode(
          WindowedValue.of(
              KV.of(
                  KV.of("7", KV.of(new OffsetRange(0, 6), GlobalWindow.TIMESTAMP_MIN_VALUE)), 6.0),
              splitValue.getTimestamp(),
              window3,
              splitValue.getPane()),
          residualBytes);
      DelayedBundleApplication expectedWindowedResidual =
          DelayedBundleApplication.newBuilder()
              .setApplication(
                  BundleApplication.newBuilder()
                      .setElement(residualBytes.toByteString())
                      .setInputId(ParDoTranslation.getMainInputName(pTransform))
                      .setTransformId(TEST_TRANSFORM_ID)
                      .build())
              .build();
      assertThat(
          trySplitResult.getPrimaryRoots(),
          contains(expectedWindowedPrimary, expectedElementSplitPrimary));
      assertThat(
          trySplitResult.getResidualRoots(),
          contains(expectedWindowedResidual, expectedElementSplitResidual));
    }

    @Test
    public void testProcessElementForTruncateAndSizeRestrictionForwardSplitWithoutObservingWindow()
        throws Exception {
      Pipeline p = Pipeline.create();
      PCollection<String> valuePCollection = p.apply(Create.of("unused"));
      valuePCollection.apply(
          TEST_TRANSFORM_ID, ParDo.of(new NonWindowObservingTestSplittableDoFn()));

      RunnerApi.Pipeline pProto =
          ProtoOverrides.updateTransform(
              PTransformTranslation.PAR_DO_TRANSFORM_URN,
              PipelineTranslation.toProto(p, SdkComponents.create(p.getOptions()), true),
              SplittableParDoExpander.createTruncateReplacement());
      String expandedTransformId =
          Iterables.find(
                  pProto.getComponents().getTransformsMap().entrySet(),
                  entry ->
                      entry
                              .getValue()
                              .getSpec()
                              .getUrn()
                              .equals(
                                  PTransformTranslation.SPLITTABLE_TRUNCATE_SIZED_RESTRICTION_URN)
                          && entry.getValue().getUniqueName().contains(TEST_TRANSFORM_ID))
              .getKey();
      RunnerApi.PTransform pTransform =
          pProto.getComponents().getTransformsOrThrow(expandedTransformId);
      String inputPCollectionId =
          pTransform.getInputsOrThrow(ParDoTranslation.getMainInputName(pTransform));
      String outputPCollectionId = Iterables.getOnlyElement(pTransform.getOutputsMap().values());

      FakeBeamFnStateClient fakeClient =
          new FakeBeamFnStateClient(StringUtf8Coder.of(), ImmutableMap.of());

      PTransformRunnerFactoryTestContext context =
          PTransformRunnerFactoryTestContext.builder(TEST_TRANSFORM_ID, pTransform)
              .beamFnStateClient(fakeClient)
              .processBundleInstructionId("57")
              .pCollections(pProto.getComponentsOrBuilder().getPcollectionsMap())
              .coders(pProto.getComponents().getCodersMap())
              .windowingStrategies(pProto.getComponents().getWindowingStrategiesMap())
              .build();
      List<WindowedValue<KV<KV<String, OffsetRange>, Double>>> mainOutputValues = new ArrayList<>();
      Coder coder =
          KvCoder.of(KvCoder.of(StringUtf8Coder.of(), OffsetRange.Coder.of()), DoubleCoder.of());
      context.addPCollectionConsumer(
          outputPCollectionId, (FnDataReceiver) new SplittableFnDataReceiver(mainOutputValues));

      new FnApiDoFnRunner.Factory<>().createRunnerForPTransform(context);
      FnDataReceiver<WindowedValue<?>> mainInput =
          context.getPCollectionConsumer(inputPCollectionId);
      assertThat(mainInput, instanceOf(HandlesSplits.class));

      assertEquals(0.7, ((HandlesSplits) mainInput).getProgress(), 0.0);
      assertEquals(createSplitResult(0.4), ((HandlesSplits) mainInput).trySplit(0.4));
    }

    @Test
    public void testProcessElementForTruncateAndSizeRestriction() throws Exception {
      Pipeline p = Pipeline.create();
      PCollection<String> valuePCollection = p.apply(Create.of("unused"));
      valuePCollection.apply(
          TEST_TRANSFORM_ID, ParDo.of(new NonWindowObservingTestSplittableDoFn()));

      RunnerApi.Pipeline pProto =
          ProtoOverrides.updateTransform(
              PTransformTranslation.PAR_DO_TRANSFORM_URN,
              PipelineTranslation.toProto(p, SdkComponents.create(p.getOptions()), true),
              SplittableParDoExpander.createTruncateReplacement());
      String expandedTransformId =
          Iterables.find(
                  pProto.getComponents().getTransformsMap().entrySet(),
                  entry ->
                      entry
                              .getValue()
                              .getSpec()
                              .getUrn()
                              .equals(
                                  PTransformTranslation.SPLITTABLE_TRUNCATE_SIZED_RESTRICTION_URN)
                          && entry.getValue().getUniqueName().contains(TEST_TRANSFORM_ID))
              .getKey();
      RunnerApi.PTransform pTransform =
          pProto.getComponents().getTransformsOrThrow(expandedTransformId);
      String inputPCollectionId =
          pTransform.getInputsOrThrow(ParDoTranslation.getMainInputName(pTransform));
      String outputPCollectionId = Iterables.getOnlyElement(pTransform.getOutputsMap().values());

      FakeBeamFnStateClient fakeClient =
          new FakeBeamFnStateClient(StringUtf8Coder.of(), ImmutableMap.of());

      PTransformRunnerFactoryTestContext context =
          PTransformRunnerFactoryTestContext.builder(TEST_TRANSFORM_ID, pTransform)
              .beamFnStateClient(fakeClient)
              .processBundleInstructionId("57")
              .pCollections(pProto.getComponentsOrBuilder().getPcollectionsMap())
              .coders(pProto.getComponents().getCodersMap())
              .windowingStrategies(pProto.getComponents().getWindowingStrategiesMap())
              .build();
      List<WindowedValue<KV<KV<String, OffsetRange>, Double>>> mainOutputValues = new ArrayList<>();
      Coder coder =
          KvCoder.of(
              KvCoder.of(
                  StringUtf8Coder.of(), KvCoder.of(OffsetRange.Coder.of(), InstantCoder.of())),
              DoubleCoder.of());
      context.addPCollectionConsumer(
          outputPCollectionId, (FnDataReceiver) new SplittableFnDataReceiver(mainOutputValues));

      new FnApiDoFnRunner.Factory<>().createRunnerForPTransform(context);

      assertTrue(context.getStartBundleFunctions().isEmpty());
      mainOutputValues.clear();

      assertThat(
          context.getPCollectionConsumers().keySet(),
          containsInAnyOrder(inputPCollectionId, outputPCollectionId));

      FnDataReceiver<WindowedValue<?>> mainInput =
          context.getPCollectionConsumer(inputPCollectionId);
      assertThat(mainInput, instanceOf(HandlesSplits.class));

      mainInput.accept(
          valueInGlobalWindow(
              KV.of(
                  KV.of("5", KV.of(new OffsetRange(0, 5), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                  5.0)));
      mainInput.accept(
          valueInGlobalWindow(
              KV.of(
                  KV.of("2", KV.of(new OffsetRange(0, 2), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                  2.0)));
      assertThat(
          mainOutputValues,
          contains(
              valueInGlobalWindow(
                  KV.of(
                      KV.of("5", KV.of(new OffsetRange(0, 2), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                      2.0)),
              valueInGlobalWindow(
                  KV.of(
                      KV.of("2", KV.of(new OffsetRange(0, 1), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                      1.0))));
      mainOutputValues.clear();

      assertTrue(context.getFinishBundleFunctions().isEmpty());
      assertThat(mainOutputValues, empty());

      Iterables.getOnlyElement(context.getTearDownFunctions()).run();
      assertThat(mainOutputValues, empty());
    }

    @Test
    public void testProcessElementForWindowedTruncateAndSizeRestriction() throws Exception {
      Pipeline p = Pipeline.create();
      PCollection<String> valuePCollection = p.apply(Create.of("unused"));
      PCollectionView<String> singletonSideInputView = valuePCollection.apply(View.asSingleton());
      valuePCollection
          .apply(Window.into(SlidingWindows.of(Duration.standardSeconds(1))))
          .apply(
              TEST_TRANSFORM_ID,
              ParDo.of(new WindowObservingTestSplittableDoFn(singletonSideInputView))
                  .withSideInputs(singletonSideInputView));

      RunnerApi.Pipeline pProto =
          ProtoOverrides.updateTransform(
              PTransformTranslation.PAR_DO_TRANSFORM_URN,
              PipelineTranslation.toProto(p, SdkComponents.create(p.getOptions()), true),
              SplittableParDoExpander.createTruncateReplacement());
      String expandedTransformId =
          Iterables.find(
                  pProto.getComponents().getTransformsMap().entrySet(),
                  entry ->
                      entry
                              .getValue()
                              .getSpec()
                              .getUrn()
                              .equals(
                                  PTransformTranslation.SPLITTABLE_TRUNCATE_SIZED_RESTRICTION_URN)
                          && entry.getValue().getUniqueName().contains(TEST_TRANSFORM_ID))
              .getKey();
      RunnerApi.PTransform pTransform =
          pProto.getComponents().getTransformsOrThrow(expandedTransformId);
      String inputPCollectionId =
          pTransform.getInputsOrThrow(ParDoTranslation.getMainInputName(pTransform));
      String outputPCollectionId = Iterables.getOnlyElement(pTransform.getOutputsMap().values());

      FakeBeamFnStateClient fakeClient =
          new FakeBeamFnStateClient(StringUtf8Coder.of(), ImmutableMap.of());

      PTransformRunnerFactoryTestContext context =
          PTransformRunnerFactoryTestContext.builder(TEST_TRANSFORM_ID, pTransform)
              .beamFnStateClient(fakeClient)
              .processBundleInstructionId("57")
              .pCollections(pProto.getComponentsOrBuilder().getPcollectionsMap())
              .coders(pProto.getComponents().getCodersMap())
              .windowingStrategies(pProto.getComponents().getWindowingStrategiesMap())
              .build();
      List<WindowedValue<KV<KV<String, OffsetRange>, Double>>> mainOutputValues = new ArrayList<>();
      Coder coder =
          KvCoder.of(
              KvCoder.of(
                  StringUtf8Coder.of(), KvCoder.of(OffsetRange.Coder.of(), InstantCoder.of())),
              DoubleCoder.of());
      context.addPCollectionConsumer(
          outputPCollectionId, (FnDataReceiver) new SplittableFnDataReceiver(mainOutputValues));

      new FnApiDoFnRunner.Factory<>().createRunnerForPTransform(context);

      assertTrue(context.getStartBundleFunctions().isEmpty());
      mainOutputValues.clear();

      assertThat(
          context.getPCollectionConsumers().keySet(),
          containsInAnyOrder(inputPCollectionId, outputPCollectionId));

      FnDataReceiver<WindowedValue<?>> mainInput =
          context.getPCollectionConsumer(inputPCollectionId);
      assertThat(mainInput, instanceOf(HandlesSplits.class));

      IntervalWindow window1 = new IntervalWindow(new Instant(5), new Instant(10));
      IntervalWindow window2 = new IntervalWindow(new Instant(6), new Instant(11));
      WindowedValue<?> firstValue =
          valueInWindows(
              KV.of(
                  KV.of("5", KV.of(new OffsetRange(0, 5), GlobalWindow.TIMESTAMP_MIN_VALUE)), 5.0),
              window1,
              window2);
      WindowedValue<?> secondValue =
          valueInWindows(
              KV.of(
                  KV.of("2", KV.of(new OffsetRange(0, 2), GlobalWindow.TIMESTAMP_MIN_VALUE)), 2.0),
              window1,
              window2);
      mainInput.accept(firstValue);
      mainInput.accept(secondValue);
      assertThat(
          mainOutputValues,
          contains(
              WindowedValue.of(
                  KV.of(
                      KV.of("5", KV.of(new OffsetRange(0, 2), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                      2.0),
                  firstValue.getTimestamp(),
                  window1,
                  firstValue.getPane()),
              WindowedValue.of(
                  KV.of(
                      KV.of("5", KV.of(new OffsetRange(0, 2), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                      2.0),
                  firstValue.getTimestamp(),
                  window2,
                  firstValue.getPane()),
              WindowedValue.of(
                  KV.of(
                      KV.of("2", KV.of(new OffsetRange(0, 1), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                      1.0),
                  firstValue.getTimestamp(),
                  window1,
                  firstValue.getPane()),
              WindowedValue.of(
                  KV.of(
                      KV.of("2", KV.of(new OffsetRange(0, 1), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                      1.0),
                  firstValue.getTimestamp(),
                  window2,
                  firstValue.getPane())));
      mainOutputValues.clear();

      assertTrue(context.getFinishBundleFunctions().isEmpty());
      assertThat(mainOutputValues, empty());

      Iterables.getOnlyElement(context.getTearDownFunctions()).run();
      assertThat(mainOutputValues, empty());
    }

    @Test
    public void
        testProcessElementForWindowedTruncateAndSizeRestrictionWithNonWindowObservingOptimization()
            throws Exception {
      Pipeline p = Pipeline.create();
      PCollection<String> valuePCollection = p.apply(Create.of("unused"));
      valuePCollection
          .apply(Window.into(SlidingWindows.of(Duration.standardSeconds(1))))
          .apply(TEST_TRANSFORM_ID, ParDo.of(new NonWindowObservingTestSplittableDoFn()));

      RunnerApi.Pipeline pProto =
          ProtoOverrides.updateTransform(
              PTransformTranslation.PAR_DO_TRANSFORM_URN,
              PipelineTranslation.toProto(p, SdkComponents.create(p.getOptions()), true),
              SplittableParDoExpander.createTruncateReplacement());
      String expandedTransformId =
          Iterables.find(
                  pProto.getComponents().getTransformsMap().entrySet(),
                  entry ->
                      entry
                              .getValue()
                              .getSpec()
                              .getUrn()
                              .equals(
                                  PTransformTranslation.SPLITTABLE_TRUNCATE_SIZED_RESTRICTION_URN)
                          && entry.getValue().getUniqueName().contains(TEST_TRANSFORM_ID))
              .getKey();
      RunnerApi.PTransform pTransform =
          pProto.getComponents().getTransformsOrThrow(expandedTransformId);
      String inputPCollectionId =
          pTransform.getInputsOrThrow(ParDoTranslation.getMainInputName(pTransform));
      String outputPCollectionId = Iterables.getOnlyElement(pTransform.getOutputsMap().values());

      PTransformRunnerFactoryTestContext context =
          PTransformRunnerFactoryTestContext.builder(TEST_TRANSFORM_ID, pTransform)
              .processBundleInstructionId("57")
              .pCollections(pProto.getComponentsOrBuilder().getPcollectionsMap())
              .coders(pProto.getComponents().getCodersMap())
              .windowingStrategies(pProto.getComponents().getWindowingStrategiesMap())
              .build();
      List<WindowedValue<KV<KV<String, OffsetRange>, Double>>> mainOutputValues = new ArrayList<>();
      Coder coder =
          KvCoder.of(
              KvCoder.of(
                  StringUtf8Coder.of(), KvCoder.of(OffsetRange.Coder.of(), InstantCoder.of())),
              DoubleCoder.of());
      context.addPCollectionConsumer(
          outputPCollectionId, (FnDataReceiver) new SplittableFnDataReceiver(mainOutputValues));

      new FnApiDoFnRunner.Factory<>().createRunnerForPTransform(context);

      assertTrue(context.getStartBundleFunctions().isEmpty());
      mainOutputValues.clear();

      assertThat(
          context.getPCollectionConsumers().keySet(),
          containsInAnyOrder(inputPCollectionId, outputPCollectionId));

      FnDataReceiver<WindowedValue<?>> mainInput =
          context.getPCollectionConsumer(inputPCollectionId);
      assertThat(mainInput, instanceOf(HandlesSplits.class));

      IntervalWindow window1 = new IntervalWindow(new Instant(5), new Instant(10));
      IntervalWindow window2 = new IntervalWindow(new Instant(6), new Instant(11));
      WindowedValue<?> firstValue =
          valueInWindows(
              KV.of(
                  KV.of("5", KV.of(new OffsetRange(0, 5), GlobalWindow.TIMESTAMP_MIN_VALUE)), 5.0),
              window1,
              window2);
      WindowedValue<?> secondValue =
          valueInWindows(
              KV.of(
                  KV.of("2", KV.of(new OffsetRange(0, 2), GlobalWindow.TIMESTAMP_MIN_VALUE)), 2.0),
              window1,
              window2);
      mainInput.accept(firstValue);
      mainInput.accept(secondValue);
      // Ensure that each output element is in all the windows and not one per window.
      assertThat(
          mainOutputValues,
          contains(
              WindowedValue.of(
                  KV.of(
                      KV.of("5", KV.of(new OffsetRange(0, 2), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                      2.0),
                  firstValue.getTimestamp(),
                  ImmutableList.of(window1, window2),
                  firstValue.getPane()),
              WindowedValue.of(
                  KV.of(
                      KV.of("2", KV.of(new OffsetRange(0, 1), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                      1.0),
                  firstValue.getTimestamp(),
                  ImmutableList.of(window1, window2),
                  firstValue.getPane())));
      mainOutputValues.clear();

      assertTrue(context.getFinishBundleFunctions().isEmpty());
      assertThat(mainOutputValues, empty());

      Iterables.getOnlyElement(context.getTearDownFunctions()).run();
      assertThat(mainOutputValues, empty());
    }

    /**
     * A {@link DoFn} that outputs elements with timestamp equal to the input timestamp minus the
     * input element.
     */
    private static class SkewingDoFn extends DoFn<String, String> {
      private final Duration allowedSkew;

      private SkewingDoFn(Duration allowedSkew) {
        this.allowedSkew = allowedSkew;
      }

      @ProcessElement
      public void processElement(ProcessContext context) {
        Duration duration = Duration.millis(Long.valueOf(context.element()));
        context.outputWithTimestamp(context.element(), context.timestamp().minus(duration));
      }

      @Override
      public Duration getAllowedTimestampSkew() {
        return allowedSkew;
      }
    }

    private static class OutputFnDataReceiver implements FnDataReceiver<WindowedValue> {
      OutputFnDataReceiver(List<WindowedValue<String>> mainOutputValues) {
        this.mainOutputValues = mainOutputValues;
      }

      private final List<WindowedValue<String>> mainOutputValues;

      @Override
      public void accept(WindowedValue input) throws Exception {
        mainOutputValues.add(input);
      }
    }

    @Test
    public void testDoFnSkewNotAllowed() throws Exception {
      Pipeline p = Pipeline.create();
      PCollection<String> valuePCollection = p.apply(Create.of("0", "1"));
      PCollection<String> outputPCollection =
          valuePCollection.apply(TEST_TRANSFORM_ID, ParDo.of(new SkewingDoFn(Duration.ZERO)));

      SdkComponents sdkComponents = SdkComponents.create(p.getOptions());
      RunnerApi.Pipeline pProto = PipelineTranslation.toProto(p, sdkComponents);
      String inputPCollectionId = sdkComponents.registerPCollection(valuePCollection);
      String outputPCollectionId = sdkComponents.registerPCollection(outputPCollection);
      RunnerApi.PTransform pTransform =
          pProto
              .getComponents()
              .getTransformsOrThrow(
                  pProto
                      .getComponents()
                      .getTransformsOrThrow(TEST_TRANSFORM_ID)
                      .getSubtransforms(0));

      PTransformRunnerFactoryTestContext context =
          PTransformRunnerFactoryTestContext.builder(TEST_TRANSFORM_ID, pTransform)
              .processBundleInstructionId("57")
              .pCollections(pProto.getComponentsOrBuilder().getPcollectionsMap())
              .coders(pProto.getComponents().getCodersMap())
              .windowingStrategies(pProto.getComponents().getWindowingStrategiesMap())
              .build();
      List<WindowedValue<String>> mainOutputValues = new ArrayList<>();
      Coder coder = StringUtf8Coder.of();
      context.addPCollectionConsumer(
          outputPCollectionId, (FnDataReceiver) new OutputFnDataReceiver(mainOutputValues));

      new FnApiDoFnRunner.Factory<>().createRunnerForPTransform(context);

      mainOutputValues.clear();
      FnDataReceiver<WindowedValue<?>> mainInput =
          context.getPCollectionConsumer(inputPCollectionId);
      mainInput.accept(valueInGlobalWindow("0"));

      String message =
          assertThrows(
                  UserCodeException.class,
                  () -> {
                    mainInput.accept(timestampedValueInGlobalWindow("1", new Instant(0L)));
                  })
              .getMessage();

      assertThat(
          message,
          allOf(
              containsString(
                  String.format("timestamp %s", new Instant(0).minus(Duration.millis(1L)))),
              containsString(
                  String.format(
                      "allowed skew (%s)",
                      PeriodFormat.getDefault().print(Duration.ZERO.toPeriod())))));
    }

    @Test
    public void testDoFnSkewAllowed() throws Exception {
      Pipeline p = Pipeline.create();
      PCollection<String> valuePCollection = p.apply(Create.of("0", "3"));
      PCollection<String> outputPCollection =
          valuePCollection.apply(TEST_TRANSFORM_ID, ParDo.of(new SkewingDoFn(Duration.millis(5L))));

      SdkComponents sdkComponents = SdkComponents.create(p.getOptions());
      RunnerApi.Pipeline pProto = PipelineTranslation.toProto(p, sdkComponents);
      String inputPCollectionId = sdkComponents.registerPCollection(valuePCollection);
      String outputPCollectionId = sdkComponents.registerPCollection(outputPCollection);
      RunnerApi.PTransform pTransform =
          pProto
              .getComponents()
              .getTransformsOrThrow(
                  pProto
                      .getComponents()
                      .getTransformsOrThrow(TEST_TRANSFORM_ID)
                      .getSubtransforms(0));

      List<WindowedValue<String>> mainOutputValues = new ArrayList<>();
      PTransformRunnerFactoryTestContext context =
          PTransformRunnerFactoryTestContext.builder(TEST_TRANSFORM_ID, pTransform)
              .processBundleInstructionId("57")
              .pCollections(pProto.getComponentsOrBuilder().getPcollectionsMap())
              .coders(pProto.getComponents().getCodersMap())
              .windowingStrategies(pProto.getComponents().getWindowingStrategiesMap())
              .build();
      Coder coder = StringUtf8Coder.of();
      context.addPCollectionConsumer(
          outputPCollectionId, (FnDataReceiver) new OutputFnDataReceiver(mainOutputValues));

      new FnApiDoFnRunner.Factory<>().createRunnerForPTransform(context);

      mainOutputValues.clear();

      FnDataReceiver<WindowedValue<?>> mainInput =
          context.getPCollectionConsumer(inputPCollectionId);
      mainInput.accept(valueInGlobalWindow("0"));
      mainInput.accept(timestampedValueInGlobalWindow("3", new Instant(0L)));
    }
  }

  @RunWith(JUnit4.class)
  public static class SplitTest {
    @Rule public final ExpectedException expected = ExpectedException.none();
    private IntervalWindow window1;
    private IntervalWindow window2;
    private IntervalWindow window3;
    private WindowedValue<String> currentElement;
    private OffsetRange currentRestriction;
    private Instant currentWatermarkEstimatorState;
    private Instant initialWatermark;
    KV<Instant, Instant> watermarkAndState;

    private static final String PROCESS_TRANSFORM_ID = "processPTransformId";
    private static final String TRUNCATE_TRANSFORM_ID = "truncatePTransformId";
    private static final String PROCESS_INPUT_ID = "processInputId";
    private static final String TRUNCATE_INPUT_ID = "truncateInputId";
    private static final String PROCESS_OUTPUT_ID = "processOutputId";
    private static final String TRUNCATE_OUTPUT_ID = "truncateOutputId";

    private KV<WindowedValue, WindowedValue> createSplitInWindow(
        OffsetRange primaryRestriction, OffsetRange residualRestriction, BoundedWindow window) {
      return KV.of(
          WindowedValue.of(
              KV.of(
                  currentElement.getValue(),
                  KV.of(primaryRestriction, currentWatermarkEstimatorState)),
              currentElement.getTimestamp(),
              window,
              currentElement.getPane()),
          WindowedValue.of(
              KV.of(
                  currentElement.getValue(),
                  KV.of(residualRestriction, watermarkAndState.getValue())),
              currentElement.getTimestamp(),
              window,
              currentElement.getPane()));
    }

    private KV<WindowedValue, WindowedValue> createSplitAcrossWindows(
        List<BoundedWindow> primaryWindows, List<BoundedWindow> residualWindows) {
      return KV.of(
          primaryWindows.isEmpty()
              ? null
              : WindowedValue.of(
                  KV.of(
                      currentElement.getValue(),
                      KV.of(currentRestriction, currentWatermarkEstimatorState)),
                  currentElement.getTimestamp(),
                  primaryWindows,
                  currentElement.getPane()),
          residualWindows.isEmpty()
              ? null
              : WindowedValue.of(
                  KV.of(
                      currentElement.getValue(),
                      KV.of(currentRestriction, currentWatermarkEstimatorState)),
                  currentElement.getTimestamp(),
                  residualWindows,
                  currentElement.getPane()));
    }

    private KV<WindowedValue, WindowedValue> createSplitWithSizeInWindow(
        OffsetRange primaryRestriction, OffsetRange residualRestriction, BoundedWindow window) {
      return KV.of(
          WindowedValue.of(
              KV.of(
                  KV.of(
                      currentElement.getValue(),
                      KV.of(primaryRestriction, currentWatermarkEstimatorState)),
                  (double) (primaryRestriction.getTo() - primaryRestriction.getFrom())),
              currentElement.getTimestamp(),
              window,
              currentElement.getPane()),
          WindowedValue.of(
              KV.of(
                  KV.of(
                      currentElement.getValue(),
                      KV.of(residualRestriction, watermarkAndState.getValue())),
                  (double) (residualRestriction.getTo() - residualRestriction.getFrom())),
              currentElement.getTimestamp(),
              window,
              currentElement.getPane()));
    }

    private KV<WindowedValue, WindowedValue> createSplitWithSizeAcrossWindows(
        List<BoundedWindow> primaryWindows, List<BoundedWindow> residualWindows) {
      return KV.of(
          primaryWindows.isEmpty()
              ? null
              : WindowedValue.of(
                  KV.of(
                      KV.of(
                          currentElement.getValue(),
                          KV.of(currentRestriction, currentWatermarkEstimatorState)),
                      (double) (currentRestriction.getTo() - currentRestriction.getFrom())),
                  currentElement.getTimestamp(),
                  primaryWindows,
                  currentElement.getPane()),
          residualWindows.isEmpty()
              ? null
              : WindowedValue.of(
                  KV.of(
                      KV.of(
                          currentElement.getValue(),
                          KV.of(currentRestriction, currentWatermarkEstimatorState)),
                      (double) (currentRestriction.getTo() - currentRestriction.getFrom())),
                  currentElement.getTimestamp(),
                  residualWindows,
                  currentElement.getPane()));
    }

    @Before
    public void setUp() {
      window1 = new IntervalWindow(Instant.ofEpochMilli(0), Instant.ofEpochMilli(10));
      window2 = new IntervalWindow(Instant.ofEpochMilli(10), Instant.ofEpochMilli(20));
      window3 = new IntervalWindow(Instant.ofEpochMilli(20), Instant.ofEpochMilli(30));
      currentElement =
          WindowedValue.of(
              "a",
              Instant.ofEpochMilli(57),
              ImmutableList.of(window1, window2, window3),
              PaneInfo.NO_FIRING);
      currentRestriction = new OffsetRange(0L, 100L);
      currentWatermarkEstimatorState = Instant.ofEpochMilli(21);
      initialWatermark = Instant.ofEpochMilli(25);
      watermarkAndState = KV.of(Instant.ofEpochMilli(42), Instant.ofEpochMilli(42));
    }

    @Test
    public void testScaledProgress() throws Exception {
      Progress elementProgress = Progress.from(2, 8);
      // There is only one window.
      Progress scaledResult = FnApiDoFnRunner.scaleProgress(elementProgress, 0, 1);
      assertEquals(2, scaledResult.getWorkCompleted(), 0.0);
      assertEquals(8, scaledResult.getWorkRemaining(), 0.0);

      // We are at the first window of 3 in total.
      scaledResult = FnApiDoFnRunner.scaleProgress(elementProgress, 0, 3);
      assertEquals(2, scaledResult.getWorkCompleted(), 0.0);
      assertEquals(28, scaledResult.getWorkRemaining(), 0.0);

      // We are at the second window of 3 in total.
      scaledResult = FnApiDoFnRunner.scaleProgress(elementProgress, 1, 3);
      assertEquals(12, scaledResult.getWorkCompleted(), 0.0);
      assertEquals(18, scaledResult.getWorkRemaining(), 0.0);

      // We are at the last window of 3 in total.
      scaledResult = FnApiDoFnRunner.scaleProgress(elementProgress, 2, 3);
      assertEquals(22, scaledResult.getWorkCompleted(), 0.0);
      assertEquals(8, scaledResult.getWorkRemaining(), 0.0);
    }

    @Test
    public void testComputeSplitForProcessOrTruncateWithNullTrackerAndSplitDelegate()
        throws Exception {
      expected.expect(IllegalArgumentException.class);
      FnApiDoFnRunner.computeSplitForProcessOrTruncate(
          currentElement,
          currentRestriction,
          window1,
          ImmutableList.copyOf(currentElement.getWindows()),
          currentWatermarkEstimatorState,
          0.0,
          null,
          null,
          null,
          0,
          3);
    }

    @Test
    public void testComputeSplitForProcessOrTruncateWithNotNullTrackerAndDelegate()
        throws Exception {
      expected.expect(IllegalArgumentException.class);
      FnApiDoFnRunner.computeSplitForProcessOrTruncate(
          currentElement,
          currentRestriction,
          window1,
          ImmutableList.copyOf(currentElement.getWindows()),
          currentWatermarkEstimatorState,
          0.0,
          new OffsetRangeTracker(currentRestriction),
          createSplitDelegate(0.3, 0.0, null),
          null,
          0,
          3);
    }

    @Test
    public void testComputeSplitForProcessOrTruncateWithInvalidWatermarkAndState()
        throws Exception {
      expected.expect(NullPointerException.class);
      FnApiDoFnRunner.computeSplitForProcessOrTruncate(
          currentElement,
          currentRestriction,
          window1,
          ImmutableList.copyOf(currentElement.getWindows()),
          currentWatermarkEstimatorState,
          0.0,
          new OffsetRangeTracker(currentRestriction),
          null,
          null,
          0,
          3);
    }

    @Test
    public void testTrySplitForProcessCheckpointOnFirstWindow() throws Exception {
      List<BoundedWindow> windows = ImmutableList.copyOf(currentElement.getWindows());
      OffsetRangeTracker tracker = new OffsetRangeTracker(currentRestriction);
      tracker.tryClaim(30L);
      SplitResultsWithStopIndex result =
          FnApiDoFnRunner.<Instant>computeSplitForProcessOrTruncate(
              currentElement,
              currentRestriction,
              window1,
              windows,
              currentWatermarkEstimatorState,
              0.0,
              tracker,
              null,
              watermarkAndState,
              0,
              3);
      assertEquals(1, result.getNewWindowStopIndex());
      KV<WindowedValue, WindowedValue> expectedElementSplit =
          createSplitInWindow(new OffsetRange(0, 31), new OffsetRange(31, 100), window1);
      KV<WindowedValue, WindowedValue> expectedWindowSplit =
          createSplitAcrossWindows(ImmutableList.of(), ImmutableList.of(window2, window3));
      assertEquals(expectedElementSplit.getKey(), result.getWindowSplit().getPrimarySplitRoot());
      assertEquals(expectedElementSplit.getValue(), result.getWindowSplit().getResidualSplitRoot());
      assertEquals(
          expectedWindowSplit.getKey(),
          result.getWindowSplit().getPrimaryInFullyProcessedWindowsRoot());
      assertEquals(
          expectedWindowSplit.getValue(),
          result.getWindowSplit().getResidualInUnprocessedWindowsRoot());
    }

    @Test
    public void testTrySplitForProcessCheckpointOnFirstWindowAfterOneSplit() throws Exception {
      List<BoundedWindow> windows = ImmutableList.copyOf(currentElement.getWindows());
      OffsetRangeTracker tracker = new OffsetRangeTracker(currentRestriction);
      tracker.tryClaim(30L);
      SplitResultsWithStopIndex result =
          FnApiDoFnRunner.<Instant>computeSplitForProcessOrTruncate(
              currentElement,
              currentRestriction,
              window1,
              windows,
              currentWatermarkEstimatorState,
              0.0,
              tracker,
              null,
              watermarkAndState,
              0,
              2);
      assertEquals(1, result.getNewWindowStopIndex());
      KV<WindowedValue, WindowedValue> expectedElementSplit =
          createSplitInWindow(new OffsetRange(0, 31), new OffsetRange(31, 100), window1);
      KV<WindowedValue, WindowedValue> expectedWindowSplit =
          createSplitAcrossWindows(ImmutableList.of(), ImmutableList.of(window2));
      assertEquals(expectedElementSplit.getKey(), result.getWindowSplit().getPrimarySplitRoot());
      assertEquals(expectedElementSplit.getValue(), result.getWindowSplit().getResidualSplitRoot());
      assertEquals(
          expectedWindowSplit.getKey(),
          result.getWindowSplit().getPrimaryInFullyProcessedWindowsRoot());
      assertEquals(
          expectedWindowSplit.getValue(),
          result.getWindowSplit().getResidualInUnprocessedWindowsRoot());
    }

    @Test
    public void testTrySplitForProcessSplitOnFirstWindow() throws Exception {
      List<BoundedWindow> windows = ImmutableList.copyOf(currentElement.getWindows());
      OffsetRangeTracker tracker = new OffsetRangeTracker(currentRestriction);
      tracker.tryClaim(30L);
      SplitResultsWithStopIndex result =
          FnApiDoFnRunner.<Instant>computeSplitForProcessOrTruncate(
              currentElement,
              currentRestriction,
              window1,
              windows,
              currentWatermarkEstimatorState,
              0.2,
              tracker,
              null,
              watermarkAndState,
              0,
              3);
      assertEquals(1, result.getNewWindowStopIndex());
      KV<WindowedValue, WindowedValue> expectedElementSplit =
          createSplitInWindow(new OffsetRange(0, 84), new OffsetRange(84, 100), window1);
      KV<WindowedValue, WindowedValue> expectedWindowSplit =
          createSplitAcrossWindows(ImmutableList.of(), ImmutableList.of(window2, window3));
      assertEquals(expectedElementSplit.getKey(), result.getWindowSplit().getPrimarySplitRoot());
      assertEquals(expectedElementSplit.getValue(), result.getWindowSplit().getResidualSplitRoot());
      assertEquals(
          expectedWindowSplit.getKey(),
          result.getWindowSplit().getPrimaryInFullyProcessedWindowsRoot());
      assertEquals(
          expectedWindowSplit.getValue(),
          result.getWindowSplit().getResidualInUnprocessedWindowsRoot());
    }

    @Test
    public void testTrySplitForProcessSplitOnMiddleWindow() throws Exception {
      List<BoundedWindow> windows = ImmutableList.copyOf(currentElement.getWindows());
      OffsetRangeTracker tracker = new OffsetRangeTracker(currentRestriction);
      tracker.tryClaim(30L);
      SplitResultsWithStopIndex result =
          FnApiDoFnRunner.computeSplitForProcessOrTruncate(
              currentElement,
              currentRestriction,
              window2,
              windows,
              currentWatermarkEstimatorState,
              0.2,
              tracker,
              null,
              watermarkAndState,
              1,
              3);
      assertEquals(2, result.getNewWindowStopIndex());
      // Java uses BigDecimal so 0.2 * 170 = 63.9...
      // BigDecimal.longValue() will round down to 63 instead of the expected 64
      KV<WindowedValue, WindowedValue> expectedElementSplit =
          createSplitInWindow(new OffsetRange(0, 63), new OffsetRange(63, 100), window2);
      KV<WindowedValue, WindowedValue> expectedWindowSplit =
          createSplitAcrossWindows(ImmutableList.of(window1), ImmutableList.of(window3));
      assertEquals(expectedElementSplit.getKey(), result.getWindowSplit().getPrimarySplitRoot());
      assertEquals(expectedElementSplit.getValue(), result.getWindowSplit().getResidualSplitRoot());
      assertEquals(
          expectedWindowSplit.getKey(),
          result.getWindowSplit().getPrimaryInFullyProcessedWindowsRoot());
      assertEquals(
          expectedWindowSplit.getValue(),
          result.getWindowSplit().getResidualInUnprocessedWindowsRoot());
    }

    @Test
    public void testTrySplitForProcessSplitOnLastWindow() throws Exception {
      List<BoundedWindow> windows = ImmutableList.copyOf(currentElement.getWindows());
      OffsetRangeTracker tracker = new OffsetRangeTracker(currentRestriction);
      tracker.tryClaim(30L);
      SplitResultsWithStopIndex result =
          FnApiDoFnRunner.computeSplitForProcessOrTruncate(
              currentElement,
              currentRestriction,
              window3,
              windows,
              currentWatermarkEstimatorState,
              0.2,
              tracker,
              null,
              watermarkAndState,
              2,
              3);
      assertEquals(3, result.getNewWindowStopIndex());
      KV<WindowedValue, WindowedValue> expectedElementSplit =
          createSplitInWindow(new OffsetRange(0, 44), new OffsetRange(44, 100), window3);
      KV<WindowedValue, WindowedValue> expectedWindowSplit =
          createSplitAcrossWindows(ImmutableList.of(window1, window2), ImmutableList.of());
      assertEquals(expectedElementSplit.getKey(), result.getWindowSplit().getPrimarySplitRoot());
      assertEquals(expectedElementSplit.getValue(), result.getWindowSplit().getResidualSplitRoot());
      assertEquals(
          expectedWindowSplit.getKey(),
          result.getWindowSplit().getPrimaryInFullyProcessedWindowsRoot());
      assertEquals(
          expectedWindowSplit.getValue(),
          result.getWindowSplit().getResidualInUnprocessedWindowsRoot());
    }

    @Test
    public void testTrySplitForProcessSplitOnFirstWindowFallback() throws Exception {
      List<BoundedWindow> windows = ImmutableList.copyOf(currentElement.getWindows());
      OffsetRangeTracker tracker = new OffsetRangeTracker(currentRestriction);
      tracker.tryClaim(100L);
      assertNull(tracker.trySplit(0.0));
      SplitResultsWithStopIndex result =
          FnApiDoFnRunner.computeSplitForProcessOrTruncate(
              currentElement,
              currentRestriction,
              window3,
              windows,
              currentWatermarkEstimatorState,
              0,
              tracker,
              null,
              watermarkAndState,
              0,
              3);
      assertEquals(1, result.getNewWindowStopIndex());
      KV<WindowedValue, WindowedValue> expectedWindowSplit =
          createSplitAcrossWindows(ImmutableList.of(window1), ImmutableList.of(window2, window3));
      assertNull(result.getWindowSplit().getPrimarySplitRoot());
      assertNull(result.getWindowSplit().getResidualSplitRoot());
      assertEquals(
          expectedWindowSplit.getKey(),
          result.getWindowSplit().getPrimaryInFullyProcessedWindowsRoot());
      assertEquals(
          expectedWindowSplit.getValue(),
          result.getWindowSplit().getResidualInUnprocessedWindowsRoot());
    }

    @Test
    public void testTrySplitForProcessSplitOnLastWindowWhenNoElementSplit() throws Exception {
      List<BoundedWindow> windows = ImmutableList.copyOf(currentElement.getWindows());
      OffsetRangeTracker tracker = new OffsetRangeTracker(currentRestriction);
      tracker.tryClaim(100L);
      assertNull(tracker.trySplit(0.0));
      SplitResultsWithStopIndex result =
          FnApiDoFnRunner.computeSplitForProcessOrTruncate(
              currentElement,
              currentRestriction,
              window3,
              windows,
              currentWatermarkEstimatorState,
              0,
              tracker,
              null,
              watermarkAndState,
              2,
              3);
      assertNull(result);
    }

    @Test
    public void testTrySplitForProcessOnWindowBoundaryRoundUp() throws Exception {
      List<BoundedWindow> windows = ImmutableList.copyOf(currentElement.getWindows());
      OffsetRangeTracker tracker = new OffsetRangeTracker(currentRestriction);
      tracker.tryClaim(30L);
      SplitResultsWithStopIndex result =
          FnApiDoFnRunner.computeSplitForProcessOrTruncate(
              currentElement,
              currentRestriction,
              window2,
              windows,
              currentWatermarkEstimatorState,
              0.6,
              tracker,
              null,
              watermarkAndState,
              0,
              3);
      assertEquals(2, result.getNewWindowStopIndex());
      KV<WindowedValue, WindowedValue> expectedWindowSplit =
          createSplitAcrossWindows(ImmutableList.of(window1, window2), ImmutableList.of(window3));
      assertNull(result.getWindowSplit().getPrimarySplitRoot());
      assertNull(result.getWindowSplit().getResidualSplitRoot());
      assertEquals(
          expectedWindowSplit.getKey(),
          result.getWindowSplit().getPrimaryInFullyProcessedWindowsRoot());
      assertEquals(
          expectedWindowSplit.getValue(),
          result.getWindowSplit().getResidualInUnprocessedWindowsRoot());
    }

    @Test
    public void testTrySplitForProcessOnWindowBoundaryRoundDown() throws Exception {
      List<BoundedWindow> windows = ImmutableList.copyOf(currentElement.getWindows());
      OffsetRangeTracker tracker = new OffsetRangeTracker(currentRestriction);
      tracker.tryClaim(30L);
      SplitResultsWithStopIndex result =
          FnApiDoFnRunner.computeSplitForProcessOrTruncate(
              currentElement,
              currentRestriction,
              window2,
              windows,
              currentWatermarkEstimatorState,
              0.3,
              tracker,
              null,
              watermarkAndState,
              0,
              3);
      assertEquals(1, result.getNewWindowStopIndex());
      KV<WindowedValue, WindowedValue> expectedWindowSplit =
          createSplitAcrossWindows(ImmutableList.of(window1), ImmutableList.of(window2, window3));
      assertNull(result.getWindowSplit().getPrimarySplitRoot());
      assertNull(result.getWindowSplit().getResidualSplitRoot());
      assertEquals(
          expectedWindowSplit.getKey(),
          result.getWindowSplit().getPrimaryInFullyProcessedWindowsRoot());
      assertEquals(
          expectedWindowSplit.getValue(),
          result.getWindowSplit().getResidualInUnprocessedWindowsRoot());
    }

    @Test
    public void testTrySplitForProcessOnWindowBoundaryRoundDownOnLastWindow() throws Exception {
      List<BoundedWindow> windows = ImmutableList.copyOf(currentElement.getWindows());
      OffsetRangeTracker tracker = new OffsetRangeTracker(currentRestriction);
      tracker.tryClaim(30L);
      SplitResultsWithStopIndex result =
          FnApiDoFnRunner.computeSplitForProcessOrTruncate(
              currentElement,
              currentRestriction,
              window2,
              windows,
              currentWatermarkEstimatorState,
              0.9,
              tracker,
              null,
              watermarkAndState,
              0,
              3);
      assertEquals(2, result.getNewWindowStopIndex());
      KV<WindowedValue, WindowedValue> expectedWindowSplit =
          createSplitAcrossWindows(ImmutableList.of(window1, window2), ImmutableList.of(window3));
      assertNull(result.getWindowSplit().getPrimarySplitRoot());
      assertNull(result.getWindowSplit().getResidualSplitRoot());
      assertEquals(
          expectedWindowSplit.getKey(),
          result.getWindowSplit().getPrimaryInFullyProcessedWindowsRoot());
      assertEquals(
          expectedWindowSplit.getValue(),
          result.getWindowSplit().getResidualInUnprocessedWindowsRoot());
    }

    private HandlesSplits createSplitDelegate(
        double progress, double expectedFraction, HandlesSplits.SplitResult result) {
      return new HandlesSplits() {
        @Override
        public SplitResult trySplit(double fractionOfRemainder) {
          checkArgument(fractionOfRemainder == expectedFraction);
          return result;
        }

        @Override
        public double getProgress() {
          return progress;
        }
      };
    }

    @Test
    public void testTrySplitForTruncateCheckpointOnFirstWindow() throws Exception {
      List<BoundedWindow> windows = ImmutableList.copyOf(currentElement.getWindows());
      SplitResult splitResult =
          SplitResult.of(
              ImmutableList.of(BundleApplication.getDefaultInstance()),
              ImmutableList.of(DelayedBundleApplication.getDefaultInstance()));
      HandlesSplits splitDelegate = createSplitDelegate(0.3, 0.0, splitResult);
      SplitResultsWithStopIndex result =
          FnApiDoFnRunner.computeSplitForProcessOrTruncate(
              currentElement,
              currentRestriction,
              window1,
              windows,
              currentWatermarkEstimatorState,
              0.0,
              null,
              splitDelegate,
              null,
              0,
              3);
      assertEquals(1, result.getNewWindowStopIndex());
      KV<WindowedValue, WindowedValue> expectedWindowSplit =
          createSplitAcrossWindows(ImmutableList.of(), ImmutableList.of(window2, window3));
      assertEquals(splitResult, result.getDownstreamSplit());
      assertNull(result.getWindowSplit().getPrimarySplitRoot());
      assertNull(result.getWindowSplit().getResidualSplitRoot());
      assertEquals(
          expectedWindowSplit.getKey(),
          result.getWindowSplit().getPrimaryInFullyProcessedWindowsRoot());
      assertEquals(
          expectedWindowSplit.getValue(),
          result.getWindowSplit().getResidualInUnprocessedWindowsRoot());
    }

    @Test
    public void testTrySplitForTruncateCheckpointOnFirstWindowAfterOneSplit() throws Exception {
      List<BoundedWindow> windows = ImmutableList.copyOf(currentElement.getWindows());
      SplitResult splitResult =
          SplitResult.of(
              ImmutableList.of(BundleApplication.getDefaultInstance()),
              ImmutableList.of(DelayedBundleApplication.getDefaultInstance()));
      HandlesSplits splitDelegate = createSplitDelegate(0.3, 0.0, splitResult);
      SplitResultsWithStopIndex result =
          FnApiDoFnRunner.computeSplitForProcessOrTruncate(
              currentElement,
              currentRestriction,
              window1,
              windows,
              currentWatermarkEstimatorState,
              0.0,
              null,
              splitDelegate,
              null,
              0,
              2);
      assertEquals(1, result.getNewWindowStopIndex());
      KV<WindowedValue, WindowedValue> expectedWindowSplit =
          createSplitAcrossWindows(ImmutableList.of(), ImmutableList.of(window2));
      assertEquals(splitResult, result.getDownstreamSplit());
      assertNull(result.getWindowSplit().getPrimarySplitRoot());
      assertNull(result.getWindowSplit().getResidualSplitRoot());
      assertEquals(
          expectedWindowSplit.getKey(),
          result.getWindowSplit().getPrimaryInFullyProcessedWindowsRoot());
      assertEquals(
          expectedWindowSplit.getValue(),
          result.getWindowSplit().getResidualInUnprocessedWindowsRoot());
    }

    @Test
    public void testTrySplitForTruncateSplitOnFirstWindow() throws Exception {
      List<BoundedWindow> windows = ImmutableList.copyOf(currentElement.getWindows());
      SplitResult splitResult =
          SplitResult.of(
              ImmutableList.of(BundleApplication.getDefaultInstance()),
              ImmutableList.of(DelayedBundleApplication.getDefaultInstance()));
      HandlesSplits splitDelegate = createSplitDelegate(0.3, 0.54, splitResult);
      SplitResultsWithStopIndex result =
          FnApiDoFnRunner.computeSplitForProcessOrTruncate(
              currentElement,
              currentRestriction,
              window1,
              windows,
              currentWatermarkEstimatorState,
              0.2,
              null,
              splitDelegate,
              null,
              0,
              3);
      assertEquals(1, result.getNewWindowStopIndex());
      KV<WindowedValue, WindowedValue> expectedWindowSplit =
          createSplitAcrossWindows(ImmutableList.of(), ImmutableList.of(window2, window3));
      assertEquals(splitResult, result.getDownstreamSplit());
      assertNull(result.getWindowSplit().getPrimarySplitRoot());
      assertNull(result.getWindowSplit().getResidualSplitRoot());
      assertEquals(
          expectedWindowSplit.getKey(),
          result.getWindowSplit().getPrimaryInFullyProcessedWindowsRoot());
      assertEquals(
          expectedWindowSplit.getValue(),
          result.getWindowSplit().getResidualInUnprocessedWindowsRoot());
    }

    @Test
    public void testTrySplitForTruncateSplitOnMiddleWindow() throws Exception {
      List<BoundedWindow> windows = ImmutableList.copyOf(currentElement.getWindows());
      SplitResult splitResult =
          SplitResult.of(
              ImmutableList.of(BundleApplication.getDefaultInstance()),
              ImmutableList.of(DelayedBundleApplication.getDefaultInstance()));
      HandlesSplits splitDelegate = createSplitDelegate(0.3, 0.34, splitResult);
      SplitResultsWithStopIndex result =
          FnApiDoFnRunner.computeSplitForProcessOrTruncate(
              currentElement,
              currentRestriction,
              window1,
              windows,
              currentWatermarkEstimatorState,
              0.2,
              null,
              splitDelegate,
              null,
              1,
              3);
      assertEquals(2, result.getNewWindowStopIndex());
      KV<WindowedValue, WindowedValue> expectedWindowSplit =
          createSplitAcrossWindows(ImmutableList.of(window1), ImmutableList.of(window3));
      assertEquals(splitResult, result.getDownstreamSplit());
      assertNull(result.getWindowSplit().getPrimarySplitRoot());
      assertNull(result.getWindowSplit().getResidualSplitRoot());
      assertEquals(
          expectedWindowSplit.getKey(),
          result.getWindowSplit().getPrimaryInFullyProcessedWindowsRoot());
      assertEquals(
          expectedWindowSplit.getValue(),
          result.getWindowSplit().getResidualInUnprocessedWindowsRoot());
    }

    @Test
    public void testTrySplitForTruncateSplitOnLastWindow() throws Exception {
      List<BoundedWindow> windows = ImmutableList.copyOf(currentElement.getWindows());
      SplitResult splitResult =
          SplitResult.of(
              ImmutableList.of(BundleApplication.getDefaultInstance()),
              ImmutableList.of(DelayedBundleApplication.getDefaultInstance()));
      HandlesSplits splitDelegate = createSplitDelegate(0.3, 0.2, splitResult);
      SplitResultsWithStopIndex result =
          FnApiDoFnRunner.computeSplitForProcessOrTruncate(
              currentElement,
              currentRestriction,
              window1,
              windows,
              currentWatermarkEstimatorState,
              0.2,
              null,
              splitDelegate,
              null,
              2,
              3);
      assertEquals(3, result.getNewWindowStopIndex());
      KV<WindowedValue, WindowedValue> expectedWindowSplit =
          createSplitAcrossWindows(ImmutableList.of(window1, window2), ImmutableList.of());
      assertEquals(splitResult, result.getDownstreamSplit());
      assertNull(result.getWindowSplit().getPrimarySplitRoot());
      assertNull(result.getWindowSplit().getResidualSplitRoot());
      assertEquals(
          expectedWindowSplit.getKey(),
          result.getWindowSplit().getPrimaryInFullyProcessedWindowsRoot());
      assertEquals(
          expectedWindowSplit.getValue(),
          result.getWindowSplit().getResidualInUnprocessedWindowsRoot());
    }

    @Test
    public void testTrySplitForTruncateSplitOnFirstWindowFallback() throws Exception {
      List<BoundedWindow> windows = ImmutableList.copyOf(currentElement.getWindows());
      SplitResult unusedSplitResult =
          SplitResult.of(
              ImmutableList.of(BundleApplication.getDefaultInstance()),
              ImmutableList.of(DelayedBundleApplication.getDefaultInstance()));
      HandlesSplits splitDelegate = createSplitDelegate(1.0, 0.0, unusedSplitResult);
      SplitResultsWithStopIndex result =
          FnApiDoFnRunner.computeSplitForProcessOrTruncate(
              currentElement,
              currentRestriction,
              window1,
              windows,
              currentWatermarkEstimatorState,
              0.0,
              null,
              splitDelegate,
              null,
              0,
              3);
      assertEquals(1, result.getNewWindowStopIndex());
      KV<WindowedValue, WindowedValue> expectedWindowSplit =
          createSplitAcrossWindows(ImmutableList.of(window1), ImmutableList.of(window2, window3));
      assertNull(result.getDownstreamSplit());
      assertNull(result.getWindowSplit().getPrimarySplitRoot());
      assertNull(result.getWindowSplit().getResidualSplitRoot());
      assertEquals(
          expectedWindowSplit.getKey(),
          result.getWindowSplit().getPrimaryInFullyProcessedWindowsRoot());
      assertEquals(
          expectedWindowSplit.getValue(),
          result.getWindowSplit().getResidualInUnprocessedWindowsRoot());
    }

    @Test
    public void testTrySplitForTruncateSplitOnLastWindowWhenNoElementSplit() throws Exception {
      List<BoundedWindow> windows = ImmutableList.copyOf(currentElement.getWindows());
      HandlesSplits splitDelegate = createSplitDelegate(1.0, 0.0, null);
      SplitResultsWithStopIndex result =
          FnApiDoFnRunner.computeSplitForProcessOrTruncate(
              currentElement,
              currentRestriction,
              window1,
              windows,
              currentWatermarkEstimatorState,
              0.0,
              null,
              splitDelegate,
              null,
              2,
              3);
      assertNull(result);
    }

    @Test
    public void testTrySplitForTruncateOnWindowBoundaryRoundUp() throws Exception {
      List<BoundedWindow> windows = ImmutableList.copyOf(currentElement.getWindows());
      SplitResult unusedSplitResult =
          SplitResult.of(
              ImmutableList.of(BundleApplication.getDefaultInstance()),
              ImmutableList.of(DelayedBundleApplication.getDefaultInstance()));
      HandlesSplits splitDelegate = createSplitDelegate(0.3, 0.0, unusedSplitResult);
      SplitResultsWithStopIndex result =
          FnApiDoFnRunner.computeSplitForProcessOrTruncate(
              currentElement,
              currentRestriction,
              window1,
              windows,
              currentWatermarkEstimatorState,
              0.6,
              null,
              splitDelegate,
              null,
              0,
              3);
      assertEquals(2, result.getNewWindowStopIndex());
      KV<WindowedValue, WindowedValue> expectedWindowSplit =
          createSplitAcrossWindows(ImmutableList.of(window1, window2), ImmutableList.of(window3));
      assertNull(result.getDownstreamSplit());
      assertNull(result.getWindowSplit().getPrimarySplitRoot());
      assertNull(result.getWindowSplit().getResidualSplitRoot());
      assertEquals(
          expectedWindowSplit.getKey(),
          result.getWindowSplit().getPrimaryInFullyProcessedWindowsRoot());
      assertEquals(
          expectedWindowSplit.getValue(),
          result.getWindowSplit().getResidualInUnprocessedWindowsRoot());
    }

    @Test
    public void testTrySplitForTruncateOnWindowBoundaryRoundDown() throws Exception {
      List<BoundedWindow> windows = ImmutableList.copyOf(currentElement.getWindows());
      SplitResult unusedSplitResult =
          SplitResult.of(
              ImmutableList.of(BundleApplication.getDefaultInstance()),
              ImmutableList.of(DelayedBundleApplication.getDefaultInstance()));
      HandlesSplits splitDelegate = createSplitDelegate(0.3, 0.0, unusedSplitResult);
      SplitResultsWithStopIndex result =
          FnApiDoFnRunner.computeSplitForProcessOrTruncate(
              currentElement,
              currentRestriction,
              window1,
              windows,
              currentWatermarkEstimatorState,
              0.3,
              null,
              splitDelegate,
              null,
              0,
              3);
      assertEquals(1, result.getNewWindowStopIndex());
      KV<WindowedValue, WindowedValue> expectedWindowSplit =
          createSplitAcrossWindows(ImmutableList.of(window1), ImmutableList.of(window2, window3));
      assertNull(result.getDownstreamSplit());
      assertNull(result.getWindowSplit().getPrimarySplitRoot());
      assertNull(result.getWindowSplit().getResidualSplitRoot());
      assertEquals(
          expectedWindowSplit.getKey(),
          result.getWindowSplit().getPrimaryInFullyProcessedWindowsRoot());
      assertEquals(
          expectedWindowSplit.getValue(),
          result.getWindowSplit().getResidualInUnprocessedWindowsRoot());
    }

    @Test
    public void testTrySplitForTruncateOnWindowBoundaryRoundDownOnLastWindow() throws Exception {
      List<BoundedWindow> windows = ImmutableList.copyOf(currentElement.getWindows());
      SplitResult unusedSplitResult =
          SplitResult.of(
              ImmutableList.of(BundleApplication.getDefaultInstance()),
              ImmutableList.of(DelayedBundleApplication.getDefaultInstance()));
      HandlesSplits splitDelegate = createSplitDelegate(0.3, 0.0, unusedSplitResult);
      SplitResultsWithStopIndex result =
          FnApiDoFnRunner.computeSplitForProcessOrTruncate(
              currentElement,
              currentRestriction,
              window1,
              windows,
              currentWatermarkEstimatorState,
              0.6,
              null,
              splitDelegate,
              null,
              0,
              3);
      assertEquals(2, result.getNewWindowStopIndex());
      KV<WindowedValue, WindowedValue> expectedWindowSplit =
          createSplitAcrossWindows(ImmutableList.of(window1, window2), ImmutableList.of(window3));
      assertNull(result.getDownstreamSplit());
      assertNull(result.getWindowSplit().getPrimarySplitRoot());
      assertNull(result.getWindowSplit().getResidualSplitRoot());
      assertEquals(
          expectedWindowSplit.getKey(),
          result.getWindowSplit().getPrimaryInFullyProcessedWindowsRoot());
      assertEquals(
          expectedWindowSplit.getValue(),
          result.getWindowSplit().getResidualInUnprocessedWindowsRoot());
    }

    @Test
    public void testConstructSplitResultWithInvalidElementSplits() throws Exception {
      expected.expect(IllegalArgumentException.class);
      FnApiDoFnRunner.constructSplitResult(
          WindowedSplitResult.forRoots(
              null,
              WindowedValue.valueInGlobalWindow("elementPrimary"),
              WindowedValue.valueInGlobalWindow("elementResidual"),
              null),
          HandlesSplits.SplitResult.of(
              ImmutableList.of(BundleApplication.getDefaultInstance()),
              ImmutableList.of(DelayedBundleApplication.getDefaultInstance())),
          WindowedValue.getFullCoder(VoidCoder.of(), GlobalWindow.Coder.INSTANCE),
          Instant.now(),
          null,
          "ptransformId",
          "inputId",
          ImmutableList.of("outputId"),
          null);
    }

    private Coder getFullInputCoder(
        Coder elementCoder, Coder restrictionCoder, Coder watermarkStateCoder, Coder windowCoder) {
      Coder inputCoder =
          KvCoder.of(
              KvCoder.of(elementCoder, KvCoder.of(restrictionCoder, watermarkStateCoder)),
              DoubleCoder.of());
      return WindowedValue.getFullCoder(inputCoder, windowCoder);
    }

    private HandlesSplits.SplitResult getProcessElementSplit(String transformId, String inputId) {
      return SplitResult.of(
          ImmutableList.of(
              BundleApplication.newBuilder()
                  .setTransformId(transformId)
                  .setInputId(inputId)
                  .build()),
          ImmutableList.of(
              DelayedBundleApplication.newBuilder()
                  .setApplication(
                      BundleApplication.newBuilder()
                          .setTransformId(transformId)
                          .setInputId(inputId)
                          .build())
                  .setRequestedTimeDelay(Durations.fromMillis(1000L))
                  .build()));
    }

    private org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.Timestamp toTimestamp(
        Instant time) {
      return org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.Timestamp.newBuilder()
          .setSeconds(time.getMillis() / 1000)
          .setNanos((int) (time.getMillis() % 1000) * 1000000)
          .build();
    }

    @Test
    public void testConstructSplitResultWithElementSplitFromDelegate() throws Exception {
      Coder fullInputCoder =
          getFullInputCoder(
              StringUtf8Coder.of(),
              OffsetRange.Coder.of(),
              InstantCoder.of(),
              IntervalWindow.getCoder());
      HandlesSplits.SplitResult elementSplit =
          getProcessElementSplit(PROCESS_TRANSFORM_ID, PROCESS_INPUT_ID);
      HandlesSplits.SplitResult result =
          FnApiDoFnRunner.constructSplitResult(
              null,
              elementSplit,
              fullInputCoder,
              null,
              null,
              TRUNCATE_TRANSFORM_ID,
              TRUNCATE_INPUT_ID,
              ImmutableList.of(TRUNCATE_OUTPUT_ID),
              null);
      assertEquals(elementSplit.getPrimaryRoots(), result.getPrimaryRoots());
      assertEquals(elementSplit.getResidualRoots(), result.getResidualRoots());
    }

    @Test
    public void testConstructSplitResultWithElementSplitFromTracker() throws Exception {
      Coder fullInputCoder =
          getFullInputCoder(
              StringUtf8Coder.of(),
              OffsetRange.Coder.of(),
              InstantCoder.of(),
              IntervalWindow.getCoder());
      KV<WindowedValue, WindowedValue> elementSplit =
          createSplitWithSizeInWindow(new OffsetRange(0, 31), new OffsetRange(31, 100), window1);
      HandlesSplits.SplitResult result =
          FnApiDoFnRunner.constructSplitResult(
              WindowedSplitResult.forRoots(
                  null, elementSplit.getKey(), elementSplit.getValue(), null),
              null,
              fullInputCoder,
              null,
              watermarkAndState,
              PROCESS_TRANSFORM_ID,
              PROCESS_INPUT_ID,
              ImmutableList.of(PROCESS_OUTPUT_ID),
              Duration.millis(100L));
      assertEquals(1, result.getPrimaryRoots().size());
      BundleApplication primaryRoot = result.getPrimaryRoots().get(0);
      assertEquals(PROCESS_TRANSFORM_ID, primaryRoot.getTransformId());
      assertEquals(PROCESS_INPUT_ID, primaryRoot.getInputId());
      assertEquals(
          elementSplit.getKey(), fullInputCoder.decode(primaryRoot.getElement().newInput()));

      assertEquals(1, result.getResidualRoots().size());
      DelayedBundleApplication residualRoot = result.getResidualRoots().get(0);
      assertEquals(Durations.fromMillis(100L), residualRoot.getRequestedTimeDelay());
      assertEquals(PROCESS_TRANSFORM_ID, residualRoot.getApplication().getTransformId());
      assertEquals(PROCESS_INPUT_ID, residualRoot.getApplication().getInputId());
      assertEquals(
          toTimestamp(watermarkAndState.getValue()),
          residualRoot.getApplication().getOutputWatermarksMap().get(PROCESS_OUTPUT_ID));
      assertEquals(
          elementSplit.getValue(),
          fullInputCoder.decode(residualRoot.getApplication().getElement().newInput()));
    }

    @Test
    public void testConstructSplitResultWithOnlyWindowSplits() throws Exception {
      Coder fullInputCoder =
          getFullInputCoder(
              StringUtf8Coder.of(),
              OffsetRange.Coder.of(),
              InstantCoder.of(),
              IntervalWindow.getCoder());
      KV<WindowedValue, WindowedValue> windowSplit =
          createSplitWithSizeAcrossWindows(
              ImmutableList.of(window1), ImmutableList.of(window2, window3));
      HandlesSplits.SplitResult result =
          FnApiDoFnRunner.constructSplitResult(
              WindowedSplitResult.forRoots(
                  windowSplit.getKey(), null, null, windowSplit.getValue()),
              null,
              fullInputCoder,
              initialWatermark,
              watermarkAndState,
              PROCESS_TRANSFORM_ID,
              PROCESS_INPUT_ID,
              ImmutableList.of(PROCESS_OUTPUT_ID),
              Duration.millis(100L));
      assertEquals(1, result.getPrimaryRoots().size());
      BundleApplication primaryRoot = result.getPrimaryRoots().get(0);
      assertEquals(PROCESS_TRANSFORM_ID, primaryRoot.getTransformId());
      assertEquals(PROCESS_INPUT_ID, primaryRoot.getInputId());
      assertEquals(
          windowSplit.getKey(), fullInputCoder.decode(primaryRoot.getElement().newInput()));

      assertEquals(1, result.getResidualRoots().size());
      DelayedBundleApplication residualRoot = result.getResidualRoots().get(0);
      assertEquals(
          org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.Duration.getDefaultInstance(),
          residualRoot.getRequestedTimeDelay());
      assertEquals(PROCESS_TRANSFORM_ID, residualRoot.getApplication().getTransformId());
      assertEquals(PROCESS_INPUT_ID, residualRoot.getApplication().getInputId());
      assertEquals(
          toTimestamp(initialWatermark),
          residualRoot.getApplication().getOutputWatermarksMap().get(PROCESS_OUTPUT_ID));
      assertEquals(
          windowSplit.getValue(),
          fullInputCoder.decode(residualRoot.getApplication().getElement().newInput()));
    }

    @Test
    public void testConstructSplitResultWithElementAndWindowSplitFromProcess() throws Exception {
      Coder fullInputCoder =
          getFullInputCoder(
              StringUtf8Coder.of(),
              OffsetRange.Coder.of(),
              InstantCoder.of(),
              IntervalWindow.getCoder());
      KV<WindowedValue, WindowedValue> windowSplit =
          createSplitWithSizeAcrossWindows(ImmutableList.of(window1), ImmutableList.of(window3));
      KV<WindowedValue, WindowedValue> elementSplit =
          createSplitWithSizeInWindow(new OffsetRange(0, 31), new OffsetRange(31, 100), window2);
      HandlesSplits.SplitResult result =
          FnApiDoFnRunner.constructSplitResult(
              WindowedSplitResult.forRoots(
                  windowSplit.getKey(),
                  elementSplit.getKey(),
                  elementSplit.getValue(),
                  windowSplit.getValue()),
              null,
              fullInputCoder,
              initialWatermark,
              watermarkAndState,
              PROCESS_TRANSFORM_ID,
              PROCESS_INPUT_ID,
              ImmutableList.of(PROCESS_OUTPUT_ID),
              Duration.millis(100L));
      assertEquals(2, result.getPrimaryRoots().size());
      BundleApplication windowPrimary = result.getPrimaryRoots().get(0);
      BundleApplication elementPrimary = result.getPrimaryRoots().get(1);
      assertEquals(PROCESS_TRANSFORM_ID, windowPrimary.getTransformId());
      assertEquals(PROCESS_INPUT_ID, windowPrimary.getInputId());
      assertEquals(
          windowSplit.getKey(), fullInputCoder.decode(windowPrimary.getElement().newInput()));
      assertEquals(PROCESS_TRANSFORM_ID, elementPrimary.getTransformId());
      assertEquals(PROCESS_INPUT_ID, elementPrimary.getInputId());
      assertEquals(
          elementSplit.getKey(), fullInputCoder.decode(elementPrimary.getElement().newInput()));

      assertEquals(2, result.getResidualRoots().size());
      DelayedBundleApplication windowResidual = result.getResidualRoots().get(0);
      DelayedBundleApplication elementResidual = result.getResidualRoots().get(1);
      assertEquals(
          org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.Duration.getDefaultInstance(),
          windowResidual.getRequestedTimeDelay());
      assertEquals(PROCESS_TRANSFORM_ID, windowResidual.getApplication().getTransformId());
      assertEquals(PROCESS_INPUT_ID, windowResidual.getApplication().getInputId());
      assertEquals(
          toTimestamp(initialWatermark),
          windowResidual.getApplication().getOutputWatermarksMap().get(PROCESS_OUTPUT_ID));
      assertEquals(
          windowSplit.getValue(),
          fullInputCoder.decode(windowResidual.getApplication().getElement().newInput()));
      assertEquals(Durations.fromMillis(100L), elementResidual.getRequestedTimeDelay());
      assertEquals(PROCESS_TRANSFORM_ID, elementResidual.getApplication().getTransformId());
      assertEquals(PROCESS_INPUT_ID, elementResidual.getApplication().getInputId());
      assertEquals(
          toTimestamp(watermarkAndState.getValue()),
          elementResidual.getApplication().getOutputWatermarksMap().get(PROCESS_OUTPUT_ID));
      assertEquals(
          elementSplit.getValue(),
          fullInputCoder.decode(elementResidual.getApplication().getElement().newInput()));
    }

    @Test
    public void testConstructSplitResultWithElementAndWindowSplitFromTruncate() throws Exception {
      Coder fullInputCoder =
          getFullInputCoder(
              StringUtf8Coder.of(),
              OffsetRange.Coder.of(),
              InstantCoder.of(),
              IntervalWindow.getCoder());
      KV<WindowedValue, WindowedValue> windowSplit =
          createSplitWithSizeAcrossWindows(ImmutableList.of(window1), ImmutableList.of(window3));
      HandlesSplits.SplitResult elementSplit =
          getProcessElementSplit(PROCESS_TRANSFORM_ID, PROCESS_INPUT_ID);
      HandlesSplits.SplitResult result =
          FnApiDoFnRunner.constructSplitResult(
              WindowedSplitResult.forRoots(
                  windowSplit.getKey(), null, null, windowSplit.getValue()),
              elementSplit,
              fullInputCoder,
              initialWatermark,
              watermarkAndState,
              TRUNCATE_TRANSFORM_ID,
              TRUNCATE_INPUT_ID,
              ImmutableList.of(TRUNCATE_OUTPUT_ID),
              Duration.millis(100L));
      assertEquals(2, result.getPrimaryRoots().size());
      BundleApplication windowPrimary = result.getPrimaryRoots().get(0);
      BundleApplication elementPrimary = result.getPrimaryRoots().get(1);
      assertEquals(TRUNCATE_TRANSFORM_ID, windowPrimary.getTransformId());
      assertEquals(TRUNCATE_INPUT_ID, windowPrimary.getInputId());
      assertEquals(
          windowSplit.getKey(), fullInputCoder.decode(windowPrimary.getElement().newInput()));
      assertEquals(elementSplit.getPrimaryRoots().get(0), elementPrimary);

      assertEquals(2, result.getResidualRoots().size());
      DelayedBundleApplication windowResidual = result.getResidualRoots().get(0);
      DelayedBundleApplication elementResidual = result.getResidualRoots().get(1);
      assertEquals(
          org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.Duration.getDefaultInstance(),
          windowResidual.getRequestedTimeDelay());
      assertEquals(TRUNCATE_TRANSFORM_ID, windowResidual.getApplication().getTransformId());
      assertEquals(TRUNCATE_INPUT_ID, windowResidual.getApplication().getInputId());
      assertEquals(
          toTimestamp(initialWatermark),
          windowResidual.getApplication().getOutputWatermarksMap().get(TRUNCATE_OUTPUT_ID));
      assertEquals(
          windowSplit.getValue(),
          fullInputCoder.decode(windowResidual.getApplication().getElement().newInput()));
      assertEquals(elementSplit.getResidualRoots().get(0), elementResidual);
    }
  }
}
