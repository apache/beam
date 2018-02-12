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

package org.apache.beam.runners.core.construction;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableList;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.ParDoPayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.SideInput;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.CombiningState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Combine.BinaryCombineLongFn;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ParDo.MultiOutput;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Suite;

/** Tests for {@link ParDoTranslation}. */
@RunWith(Suite.class)
@Suite.SuiteClasses({
  ParDoTranslationTest.TestParDoPayloadTranslation.class,
  ParDoTranslationTest.TestStateAndTimerTranslation.class
})
public class ParDoTranslationTest {

  /**
   * Tests for translating various {@link ParDo} transforms to/from {@link ParDoPayload} protos.
   */
  @RunWith(Parameterized.class)
  public static class TestParDoPayloadTranslation {
    public static TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);

    private static PCollectionView<Long> singletonSideInput =
        p.apply("GenerateSingleton", GenerateSequence.from(0L).to(1L)).apply(View.asSingleton());
    private static PCollectionView<Map<Long, Iterable<String>>> multimapSideInput =
        p.apply("CreateMultimap", Create.of(KV.of(1L, "foo"), KV.of(1L, "bar"), KV.of(2L, "spam")))
            .setCoder(KvCoder.of(VarLongCoder.of(), StringUtf8Coder.of()))
            .apply(View.asMultimap());

    private static PCollection<KV<Long, String>> mainInput =
        p.apply(
            "CreateMainInput", Create.empty(KvCoder.of(VarLongCoder.of(), StringUtf8Coder.of())));

    @Parameters(name = "{index}: {0}")
    public static Iterable<ParDo.MultiOutput<?, ?>> data() {
      return ImmutableList.of(
          ParDo.of(new DropElementsFn()).withOutputTags(new TupleTag<>(), TupleTagList.empty()),
          ParDo.of(new DropElementsFn())
              .withOutputTags(new TupleTag<>(), TupleTagList.empty())
              .withSideInputs(singletonSideInput, multimapSideInput),
          ParDo.of(new DropElementsFn())
              .withOutputTags(
                  new TupleTag<>(),
                  TupleTagList.of(new TupleTag<byte[]>() {}).and(new TupleTag<Integer>() {}))
              .withSideInputs(singletonSideInput, multimapSideInput),
          ParDo.of(new DropElementsFn())
              .withOutputTags(
                  new TupleTag<>(),
                  TupleTagList.of(new TupleTag<byte[]>() {}).and(new TupleTag<Integer>() {})),
          ParDo.of(new SplittableDropElementsFn())
              .withOutputTags(new TupleTag<>(), TupleTagList.empty()));
    }

    @Parameter(0)
    public ParDo.MultiOutput<KV<Long, String>, Void> parDo;

    @Test
    public void testToAndFromProto() throws Exception {
      SdkComponents components = SdkComponents.create();
      ParDoPayload payload = ParDoTranslation.translateParDo(parDo, components);

      assertThat(ParDoTranslation.getDoFn(payload), Matchers.equalTo(parDo.getFn()));
      assertThat(
          ParDoTranslation.getMainOutputTag(payload), Matchers.equalTo(parDo.getMainOutputTag()));
      for (PCollectionView<?> view : parDo.getSideInputs()) {
        payload.getSideInputsOrThrow(view.getTagInternal().getId());
      }
    }

    @Test
    public void toAndFromTransformProto() throws Exception {
      Map<TupleTag<?>, PValue> inputs = new HashMap<>();
      inputs.put(new TupleTag<KV<Long, String>>() {}, mainInput);
      inputs.putAll(parDo.getAdditionalInputs());
      PCollectionTuple output = mainInput.apply(parDo);

      SdkComponents sdkComponents = SdkComponents.create();

      // Encode
      RunnerApi.PTransform protoTransform =
          PTransformTranslation.toProto(
              AppliedPTransform.<PCollection<KV<Long, String>>, PCollection<Void>, MultiOutput>of(
                  "foo", inputs, output.expand(), parDo, p),
              sdkComponents);
      RunnerApi.Components components = sdkComponents.toComponents();
      RehydratedComponents rehydratedComponents =
          RehydratedComponents.forComponents(components);

      // Decode
      Pipeline.create();

      ParDoPayload parDoPayload = ParDoPayload.parseFrom(protoTransform.getSpec().getPayload());
      for (PCollectionView<?> view : parDo.getSideInputs()) {
        SideInput sideInput = parDoPayload.getSideInputsOrThrow(view.getTagInternal().getId());
        PCollectionView<?> restoredView =
            PCollectionViewTranslation.viewFromProto(
                sideInput,
                view.getTagInternal().getId(),
                view.getPCollection(),
                protoTransform,
                rehydratedComponents);
        assertThat(restoredView.getTagInternal(), Matchers.equalTo(view.getTagInternal()));
        assertThat(restoredView.getViewFn(), instanceOf(view.getViewFn().getClass()));
        assertThat(
            restoredView.getWindowMappingFn(), instanceOf(view.getWindowMappingFn().getClass()));
        assertThat(
            restoredView.getWindowingStrategyInternal(),
            Matchers.equalTo(view.getWindowingStrategyInternal().fixDefaults()));
        assertThat(restoredView.getCoderInternal(), Matchers.equalTo(view.getCoderInternal()));
      }
      String mainInputId = sdkComponents.registerPCollection(mainInput);
      assertThat(
          ParDoTranslation.getMainInput(protoTransform, components),
          equalTo(components.getPcollectionsOrThrow(mainInputId)));
    }
  }

  /**
   * Tests for translating state and timer bits to/from protos.
   */
  @RunWith(Parameterized.class)
  public static class TestStateAndTimerTranslation {

    @Parameters(name = "{index}: {0}")
    public static Iterable<StateSpec<?>> stateSpecs() {
      return ImmutableList.of(
          StateSpecs.value(VarIntCoder.of()),
          StateSpecs.bag(VarIntCoder.of()),
          StateSpecs.set(VarIntCoder.of()),
          StateSpecs.map(StringUtf8Coder.of(), VarIntCoder.of()));
    }

    @Parameter
    public StateSpec<?> stateSpec;

    @Test
    public void testStateSpecToFromProto() throws Exception {
      // Encode
      SdkComponents sdkComponents = SdkComponents.create();
      RunnerApi.StateSpec stateSpecProto =
          ParDoTranslation.translateStateSpec(stateSpec, sdkComponents);

      // Decode
      RehydratedComponents rehydratedComponents =
          RehydratedComponents.forComponents(sdkComponents.toComponents());
      StateSpec<?> deserializedStateSpec =
          ParDoTranslation.fromProto(stateSpecProto, rehydratedComponents);

      assertThat(stateSpec, Matchers.equalTo(deserializedStateSpec));
    }
  }

  private static class DropElementsFn extends DoFn<KV<Long, String>, Void> {
    @ProcessElement
    public void proc(ProcessContext context, BoundedWindow window) {
      context.output(null);
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof DropElementsFn;
    }

    @Override
    public int hashCode() {
      return DropElementsFn.class.hashCode();
    }
  }

  private static class SplittableDropElementsFn extends DoFn<KV<Long, String>, Void> {
    @ProcessElement
    public void proc(ProcessContext context, RestrictionTracker<Integer, ?> restriction) {
      context.output(null);
    }

    @GetInitialRestriction
    public Integer restriction(KV<Long, String> elem) {
      return 42;
    }

    @NewTracker
    public RestrictionTracker<Integer, ?> newTracker(Integer restriction) {
      throw new UnsupportedOperationException("Should never be called; only to test translation");
    }


    @Override
    public boolean equals(Object other) {
      return other instanceof SplittableDropElementsFn;
    }

    @Override
    public int hashCode() {
      return SplittableDropElementsFn.class.hashCode();
    }
  }

  @SuppressWarnings("unused")
  private static class StateTimerDropElementsFn extends DoFn<KV<Long, String>, Void> {
    private static final String BAG_STATE_ID = "bagState";
    private static final String COMBINING_STATE_ID = "combiningState";
    private static final String EVENT_TIMER_ID = "eventTimer";
    private static final String PROCESSING_TIMER_ID = "processingTimer";

    @StateId(BAG_STATE_ID)
    private final StateSpec<BagState<String>> bagState = StateSpecs.bag(StringUtf8Coder.of());

    @StateId(COMBINING_STATE_ID)
    private final StateSpec<CombiningState<Long, long[], Long>> combiningState =
        StateSpecs.combining(
            new BinaryCombineLongFn() {
              @Override
              public long apply(long left, long right) {
                return Math.max(left, right);
              }

              @Override
              public long identity() {
                return Long.MIN_VALUE;
              }
            });

    @TimerId(EVENT_TIMER_ID)
    private final TimerSpec eventTimer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    @TimerId(PROCESSING_TIMER_ID)
    private final TimerSpec processingTimer = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

    @ProcessElement
    public void dropInput(
        ProcessContext context,
        BoundedWindow window,
        @StateId(BAG_STATE_ID) BagState<String> bagStateState,
        @StateId(COMBINING_STATE_ID) CombiningState<Long, long[], Long> combiningStateState,
        @TimerId(EVENT_TIMER_ID) Timer eventTimerTimer,
        @TimerId(PROCESSING_TIMER_ID) Timer processingTimerTimer) {
      context.output(null);
    }

    @OnTimer(EVENT_TIMER_ID)
    public void onEventTime(OnTimerContext context) {}

    @OnTimer(PROCESSING_TIMER_ID)
    public void onProcessingTime(OnTimerContext context) {}

    @Override
    public boolean equals(Object other) {
      return other instanceof StateTimerDropElementsFn;
    }

    @Override
    public int hashCode() {
      return StateTimerDropElementsFn.class.hashCode();
    }
  }
}
