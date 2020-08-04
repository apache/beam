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

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

import java.io.Serializable;
import java.util.Collections;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.io.DefaultFilenamePolicy;
import org.apache.beam.sdk.io.DynamicFileDestinations;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.FileBasedSink.FilenamePolicy;
import org.apache.beam.sdk.io.LocalResources;
import org.apache.beam.sdk.io.WriteFiles;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.PTransformMatcher;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.View.CreatePCollectionView;
import org.apache.beam.sdk.transforms.ViewFn;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PCollectionViews;
import org.apache.beam.sdk.values.PCollectionViews.IterableViewFn;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.hamcrest.Matchers;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link PTransformMatcher}. */
@RunWith(JUnit4.class)
public class PTransformMatchersTest implements Serializable {
  @Rule
  public transient TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);

  /**
   * Gets the {@link AppliedPTransform} that has a created {@code PCollection<KV<String, Integer>>}
   * as input.
   */
  private AppliedPTransform<?, ?, ?> getAppliedTransform(PTransform pardo) {
    PCollection<KV<String, Integer>> input =
        PCollection.createPrimitiveOutputInternal(
            p,
            WindowingStrategy.globalDefault(),
            IsBounded.BOUNDED,
            KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()));
    input.setName("dummy input");

    PCollection<Integer> output =
        PCollection.createPrimitiveOutputInternal(
            p, WindowingStrategy.globalDefault(), IsBounded.BOUNDED, VarIntCoder.of());
    output.setName("dummy output");

    return AppliedPTransform.of("pardo", input.expand(), output.expand(), pardo, p);
  }

  @Test
  public void classEqualToMatchesSameClass() {
    PTransformMatcher matcher = PTransformMatchers.classEqualTo(ParDo.SingleOutput.class);
    AppliedPTransform<?, ?, ?> application =
        getAppliedTransform(
            ParDo.of(
                new DoFn<KV<String, Integer>, Integer>() {
                  @ProcessElement
                  public void doStuff(ProcessContext ctxt) {}
                }));

    assertThat(matcher.matches(application), is(true));
  }

  @Test
  public void classEqualToDoesNotMatchSubclass() {
    class MyPTransform extends PTransform<PCollection<KV<String, Integer>>, PCollection<Integer>> {
      @Override
      public PCollection<Integer> expand(PCollection<KV<String, Integer>> input) {
        return PCollection.createPrimitiveOutputInternal(
            input.getPipeline(), input.getWindowingStrategy(), input.isBounded(), VarIntCoder.of());
      }
    }

    PTransformMatcher matcher = PTransformMatchers.classEqualTo(MyPTransform.class);
    MyPTransform subclass = new MyPTransform() {};

    assertThat(subclass.getClass(), not(Matchers.<Class<?>>equalTo(MyPTransform.class)));
    assertThat(subclass, instanceOf(MyPTransform.class));

    AppliedPTransform<?, ?, ?> application = getAppliedTransform(subclass);

    assertThat(matcher.matches(application), is(false));
  }

  @Test
  public void classEqualToDoesNotMatchUnrelatedClass() {
    PTransformMatcher matcher = PTransformMatchers.classEqualTo(ParDo.SingleOutput.class);
    AppliedPTransform<?, ?, ?> application =
        getAppliedTransform(Window.<KV<String, Integer>>into(new GlobalWindows()));

    assertThat(matcher.matches(application), is(false));
  }

  private DoFn<KV<String, Integer>, Integer> doFn =
      new DoFn<KV<String, Integer>, Integer>() {
        @ProcessElement
        public void simpleProcess(ProcessContext ctxt) {
          ctxt.output(ctxt.element().getValue() + 1);
        }
      };

  private abstract static class SomeTracker extends RestrictionTracker<Void, Void> {}

  private DoFn<KV<String, Integer>, Integer> splittableDoFn =
      new DoFn<KV<String, Integer>, Integer>() {
        @ProcessElement
        public void processElement(
            ProcessContext context, RestrictionTracker<Void, Void> tracker) {}

        @GetInitialRestriction
        public Void getInitialRestriction(@Element KV<String, Integer> element) {
          return null;
        }

        @NewTracker
        public SomeTracker newTracker(@Restriction Void restriction) {
          return null;
        }
      };
  private DoFn<KV<String, Integer>, Integer> doFnWithState =
      new DoFn<KV<String, Integer>, Integer>() {
        private final String stateId = "mystate";

        @StateId(stateId)
        private final StateSpec<ValueState<Integer>> intState = StateSpecs.value(VarIntCoder.of());

        @ProcessElement
        public void processElement(ProcessContext c, @StateId(stateId) ValueState<Integer> state) {
          Integer currentValue = MoreObjects.firstNonNull(state.read(), 0);
          c.output(currentValue);
          state.write(currentValue + 1);
        }
      };
  private DoFn<KV<String, Integer>, Integer> doFnWithTimers =
      new DoFn<KV<String, Integer>, Integer>() {
        private final String timerId = "myTimer";

        @TimerId(timerId)
        private final TimerSpec spec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

        @ProcessElement
        public void processElement(ProcessContext context, @TimerId(timerId) Timer timer) {
          timer.offset(Duration.standardSeconds(1)).setRelative();
          context.output(3);
        }

        @OnTimer(timerId)
        public void onTimer(OnTimerContext context) {
          context.output(42);
        }
      };

  /** Demonstrates that a {@link ParDo.SingleOutput} does not match any ParDo matcher. */
  @Test
  public void parDoSingle() {
    AppliedPTransform<?, ?, ?> parDoApplication = getAppliedTransform(ParDo.of(doFn));

    assertThat(PTransformMatchers.splittableParDoMulti().matches(parDoApplication), is(false));
    assertThat(PTransformMatchers.stateOrTimerParDoMulti().matches(parDoApplication), is(false));
    assertThat(PTransformMatchers.splittableParDoSingle().matches(parDoApplication), is(false));
    assertThat(PTransformMatchers.stateOrTimerParDoSingle().matches(parDoApplication), is(false));
  }

  @Test
  public void parDoSingleSplittable() {
    AppliedPTransform<?, ?, ?> parDoApplication = getAppliedTransform(ParDo.of(splittableDoFn));
    assertThat(PTransformMatchers.splittableParDoSingle().matches(parDoApplication), is(true));

    assertThat(PTransformMatchers.splittableParDoMulti().matches(parDoApplication), is(false));
    assertThat(PTransformMatchers.stateOrTimerParDoMulti().matches(parDoApplication), is(false));
    assertThat(PTransformMatchers.stateOrTimerParDoSingle().matches(parDoApplication), is(false));
  }

  @Test
  public void parDoSingleWithState() {
    AppliedPTransform<?, ?, ?> parDoApplication = getAppliedTransform(ParDo.of(doFnWithState));
    assertThat(PTransformMatchers.stateOrTimerParDoSingle().matches(parDoApplication), is(true));

    assertThat(PTransformMatchers.splittableParDoMulti().matches(parDoApplication), is(false));
    assertThat(PTransformMatchers.stateOrTimerParDoMulti().matches(parDoApplication), is(false));
    assertThat(PTransformMatchers.splittableParDoSingle().matches(parDoApplication), is(false));
  }

  @Test
  public void parDoSingleWithTimers() {
    AppliedPTransform<?, ?, ?> parDoApplication = getAppliedTransform(ParDo.of(doFnWithTimers));
    assertThat(PTransformMatchers.stateOrTimerParDoSingle().matches(parDoApplication), is(true));

    assertThat(PTransformMatchers.splittableParDoMulti().matches(parDoApplication), is(false));
    assertThat(PTransformMatchers.stateOrTimerParDoMulti().matches(parDoApplication), is(false));
    assertThat(PTransformMatchers.splittableParDoSingle().matches(parDoApplication), is(false));
  }

  @Test
  public void parDoMulti() {
    AppliedPTransform<?, ?, ?> parDoApplication =
        getAppliedTransform(ParDo.of(doFn).withOutputTags(new TupleTag<>(), TupleTagList.empty()));

    assertThat(PTransformMatchers.splittableParDoMulti().matches(parDoApplication), is(false));
    assertThat(PTransformMatchers.stateOrTimerParDoMulti().matches(parDoApplication), is(false));
    assertThat(PTransformMatchers.splittableParDoSingle().matches(parDoApplication), is(false));
    assertThat(PTransformMatchers.stateOrTimerParDoSingle().matches(parDoApplication), is(false));
  }

  @Test
  public void parDoMultiSplittable() {
    AppliedPTransform<?, ?, ?> parDoApplication =
        getAppliedTransform(
            ParDo.of(splittableDoFn).withOutputTags(new TupleTag<>(), TupleTagList.empty()));
    assertThat(PTransformMatchers.splittableParDoMulti().matches(parDoApplication), is(true));

    assertThat(PTransformMatchers.stateOrTimerParDoMulti().matches(parDoApplication), is(false));
    assertThat(PTransformMatchers.splittableParDoSingle().matches(parDoApplication), is(false));
    assertThat(PTransformMatchers.stateOrTimerParDoSingle().matches(parDoApplication), is(false));
  }

  @Test
  public void parDoSplittable() {
    AppliedPTransform<?, ?, ?> parDoApplication =
        getAppliedTransform(
            ParDo.of(splittableDoFn).withOutputTags(new TupleTag<>(), TupleTagList.empty()));
    assertThat(PTransformMatchers.splittableParDo().matches(parDoApplication), is(true));

    assertThat(PTransformMatchers.stateOrTimerParDoMulti().matches(parDoApplication), is(false));
    assertThat(PTransformMatchers.splittableParDoSingle().matches(parDoApplication), is(false));
    assertThat(PTransformMatchers.stateOrTimerParDoSingle().matches(parDoApplication), is(false));
  }

  @Test
  public void parDoMultiWithState() {
    AppliedPTransform<?, ?, ?> parDoApplication =
        getAppliedTransform(
            ParDo.of(doFnWithState).withOutputTags(new TupleTag<>(), TupleTagList.empty()));
    assertThat(PTransformMatchers.stateOrTimerParDoMulti().matches(parDoApplication), is(true));

    assertThat(PTransformMatchers.splittableParDoMulti().matches(parDoApplication), is(false));
    assertThat(PTransformMatchers.splittableParDoSingle().matches(parDoApplication), is(false));
    assertThat(PTransformMatchers.stateOrTimerParDoSingle().matches(parDoApplication), is(false));
  }

  @Test
  public void parDoWithState() {
    AppliedPTransform<?, ?, ?> statefulApplication =
        getAppliedTransform(
            ParDo.of(doFnWithState).withOutputTags(new TupleTag<>(), TupleTagList.empty()));
    assertThat(PTransformMatchers.stateOrTimerParDo().matches(statefulApplication), is(true));

    AppliedPTransform<?, ?, ?> splittableApplication =
        getAppliedTransform(
            ParDo.of(splittableDoFn).withOutputTags(new TupleTag<>(), TupleTagList.empty()));
    assertThat(PTransformMatchers.stateOrTimerParDo().matches(splittableApplication), is(false));
  }

  @Test
  public void parDoMultiWithTimers() {
    AppliedPTransform<?, ?, ?> parDoApplication =
        getAppliedTransform(
            ParDo.of(doFnWithTimers).withOutputTags(new TupleTag<>(), TupleTagList.empty()));
    assertThat(PTransformMatchers.stateOrTimerParDoMulti().matches(parDoApplication), is(true));

    assertThat(PTransformMatchers.splittableParDoMulti().matches(parDoApplication), is(false));
    assertThat(PTransformMatchers.splittableParDoSingle().matches(parDoApplication), is(false));
    assertThat(PTransformMatchers.stateOrTimerParDoSingle().matches(parDoApplication), is(false));
  }

  @Test
  public void parDoRequiresStableInput() {
    DoFn<Object, Object> doFnRSI =
        new DoFn<Object, Object>() {
          @RequiresStableInput
          @ProcessElement
          public void process(ProcessContext ctxt) {}
        };

    AppliedPTransform<?, ?, ?> single = getAppliedTransform(ParDo.of(doFn));
    AppliedPTransform<?, ?, ?> singleRSI = getAppliedTransform(ParDo.of(doFnRSI));
    AppliedPTransform<?, ?, ?> multi =
        getAppliedTransform(ParDo.of(doFn).withOutputTags(new TupleTag<>(), TupleTagList.empty()));
    AppliedPTransform<?, ?, ?> multiRSI =
        getAppliedTransform(
            ParDo.of(doFnRSI).withOutputTags(new TupleTag<>(), TupleTagList.empty()));

    assertThat(PTransformMatchers.requiresStableInputParDoSingle().matches(single), is(false));
    assertThat(PTransformMatchers.requiresStableInputParDoSingle().matches(singleRSI), is(true));
    assertThat(PTransformMatchers.requiresStableInputParDoSingle().matches(multi), is(false));
    assertThat(PTransformMatchers.requiresStableInputParDoSingle().matches(multiRSI), is(false));
    assertThat(PTransformMatchers.requiresStableInputParDoMulti().matches(single), is(false));
    assertThat(PTransformMatchers.requiresStableInputParDoMulti().matches(singleRSI), is(false));
    assertThat(PTransformMatchers.requiresStableInputParDoMulti().matches(multi), is(false));
    assertThat(PTransformMatchers.requiresStableInputParDoMulti().matches(multiRSI), is(true));
  }

  @Test
  public void parDoWithFnTypeWithMatchingType() {
    DoFn<Object, Object> fn =
        new DoFn<Object, Object>() {
          @ProcessElement
          public void process(ProcessContext ctxt) {}
        };
    AppliedPTransform<?, ?, ?> parDoSingle = getAppliedTransform(ParDo.of(fn));
    AppliedPTransform<?, ?, ?> parDoMulti =
        getAppliedTransform(ParDo.of(fn).withOutputTags(new TupleTag<>(), TupleTagList.empty()));

    PTransformMatcher matcher = PTransformMatchers.parDoWithFnType(fn.getClass());
    assertThat(matcher.matches(parDoSingle), is(true));
    assertThat(matcher.matches(parDoMulti), is(true));
  }

  @Test
  public void parDoWithFnTypeWithNoMatch() {
    DoFn<Object, Object> fn =
        new DoFn<Object, Object>() {
          @ProcessElement
          public void process(ProcessContext ctxt) {}
        };
    AppliedPTransform<?, ?, ?> parDoSingle = getAppliedTransform(ParDo.of(fn));
    AppliedPTransform<?, ?, ?> parDoMulti =
        getAppliedTransform(ParDo.of(fn).withOutputTags(new TupleTag<>(), TupleTagList.empty()));

    PTransformMatcher matcher = PTransformMatchers.parDoWithFnType(doFnWithState.getClass());
    assertThat(matcher.matches(parDoSingle), is(false));
    assertThat(matcher.matches(parDoMulti), is(false));
  }

  @Test
  public void parDoWithFnTypeNotParDo() {
    AppliedPTransform<?, ?, ?> notParDo = getAppliedTransform(Create.empty(VoidCoder.of()));
    PTransformMatcher matcher = PTransformMatchers.parDoWithFnType(doFnWithState.getClass());
    assertThat(matcher.matches(notParDo), is(false));
  }

  @Test
  public void createViewWithViewFn() {
    PCollection<Integer> input = p.apply(Create.of(1));
    PCollectionView<Iterable<Integer>> view = input.apply(View.asIterable());
    ViewFn<?, ?> viewFn = view.getViewFn();
    CreatePCollectionView<?, ?> createView = CreatePCollectionView.of(view);

    PTransformMatcher matcher = PTransformMatchers.createViewWithViewFn(viewFn.getClass());
    assertThat(matcher.matches(getAppliedTransform(createView)), is(true));
  }

  @Test
  public void createViewWithViewFnDifferentViewFn() {
    PCollection<Integer> input = p.apply(Create.of(1));
    PCollectionView<Iterable<Integer>> view = input.apply(View.asIterable());

    // Purposely create a subclass to get a different class then what was expected.
    IterableViewFn<Integer> viewFn =
        new PCollectionViews.IterableViewFn<Integer>(() -> TypeDescriptors.integers()) {};
    CreatePCollectionView<?, ?> createView = CreatePCollectionView.of(view);

    PTransformMatcher matcher = PTransformMatchers.createViewWithViewFn(viewFn.getClass());
    assertThat(matcher.matches(getAppliedTransform(createView)), is(false));
  }

  @Test
  public void createViewWithViewFnNotCreatePCollectionView() {
    PCollection<Integer> input = p.apply(Create.of(1));
    PCollectionView<Iterable<Integer>> view = input.apply(View.asIterable());
    PTransformMatcher matcher =
        PTransformMatchers.createViewWithViewFn(view.getViewFn().getClass());
    assertThat(matcher.matches(getAppliedTransform(View.asIterable())), is(false));
  }

  @Test
  public void emptyFlattenWithEmptyFlatten() {
    AppliedPTransform application =
        AppliedPTransform.of(
            "EmptyFlatten",
            Collections.emptyMap(),
            Collections.singletonMap(
                new TupleTag<Integer>(),
                PCollection.createPrimitiveOutputInternal(
                    p, WindowingStrategy.globalDefault(), IsBounded.BOUNDED, VarIntCoder.of())),
            Flatten.pCollections(),
            p);

    assertThat(PTransformMatchers.emptyFlatten().matches(application), is(true));
  }

  @Test
  public void emptyFlattenWithNonEmptyFlatten() {
    AppliedPTransform application =
        AppliedPTransform.of(
            "Flatten",
            Collections.singletonMap(
                new TupleTag<Integer>(),
                PCollection.createPrimitiveOutputInternal(
                    p, WindowingStrategy.globalDefault(), IsBounded.BOUNDED, VarIntCoder.of())),
            Collections.singletonMap(
                new TupleTag<Integer>(),
                PCollection.createPrimitiveOutputInternal(
                    p, WindowingStrategy.globalDefault(), IsBounded.BOUNDED, VarIntCoder.of())),
            Flatten.pCollections(),
            p);

    assertThat(PTransformMatchers.emptyFlatten().matches(application), is(false));
  }

  @Test
  public void emptyFlattenWithNonFlatten() {
    AppliedPTransform application =
        AppliedPTransform
            .<PCollection<Iterable<Integer>>, PCollection<Integer>, Flatten.Iterables<Integer>>of(
                "EmptyFlatten",
                Collections.emptyMap(),
                Collections.singletonMap(
                    new TupleTag<Integer>(),
                    PCollection.createPrimitiveOutputInternal(
                        p, WindowingStrategy.globalDefault(), IsBounded.BOUNDED, VarIntCoder.of())),
                /* This isn't actually possible to construct, but for the sake of example */
                Flatten.iterables(),
                p);

    assertThat(PTransformMatchers.emptyFlatten().matches(application), is(false));
  }

  @Test
  public void flattenWithDuplicateInputsWithoutDuplicates() {
    AppliedPTransform application =
        AppliedPTransform.of(
            "Flatten",
            Collections.singletonMap(
                new TupleTag<Integer>(),
                PCollection.createPrimitiveOutputInternal(
                    p, WindowingStrategy.globalDefault(), IsBounded.BOUNDED, VarIntCoder.of())),
            Collections.singletonMap(
                new TupleTag<Integer>(),
                PCollection.createPrimitiveOutputInternal(
                    p, WindowingStrategy.globalDefault(), IsBounded.BOUNDED, VarIntCoder.of())),
            Flatten.pCollections(),
            p);

    assertThat(PTransformMatchers.flattenWithDuplicateInputs().matches(application), is(false));
  }

  @Test
  public void flattenWithDuplicateInputsWithDuplicates() {
    PCollection<Integer> duplicate =
        PCollection.createPrimitiveOutputInternal(
            p, WindowingStrategy.globalDefault(), IsBounded.BOUNDED, VarIntCoder.of());
    AppliedPTransform application =
        AppliedPTransform.of(
            "Flatten",
            ImmutableMap.<TupleTag<?>, PValue>builder()
                .put(new TupleTag<Integer>(), duplicate)
                .put(new TupleTag<Integer>(), duplicate)
                .build(),
            Collections.singletonMap(
                new TupleTag<Integer>(),
                PCollection.createPrimitiveOutputInternal(
                    p, WindowingStrategy.globalDefault(), IsBounded.BOUNDED, VarIntCoder.of())),
            Flatten.pCollections(),
            p);

    assertThat(PTransformMatchers.flattenWithDuplicateInputs().matches(application), is(true));
  }

  @Test
  public void flattenWithDuplicateInputsNonFlatten() {
    AppliedPTransform application =
        AppliedPTransform
            .<PCollection<Iterable<Integer>>, PCollection<Integer>, Flatten.Iterables<Integer>>of(
                "EmptyFlatten",
                Collections.emptyMap(),
                Collections.singletonMap(
                    new TupleTag<Integer>(),
                    PCollection.createPrimitiveOutputInternal(
                        p, WindowingStrategy.globalDefault(), IsBounded.BOUNDED, VarIntCoder.of())),
                /* This isn't actually possible to construct, but for the sake of example */
                Flatten.iterables(),
                p);

    assertThat(PTransformMatchers.flattenWithDuplicateInputs().matches(application), is(false));
  }

  @Test
  public void writeWithRunnerDeterminedSharding() {
    ResourceId outputDirectory = LocalResources.fromString("/foo/bar", true /* isDirectory */);
    FilenamePolicy policy =
        DefaultFilenamePolicy.fromStandardParameters(
            StaticValueProvider.of(outputDirectory),
            DefaultFilenamePolicy.DEFAULT_UNWINDOWED_SHARD_TEMPLATE,
            "",
            false);
    WriteFiles<Integer, Void, Integer> write =
        WriteFiles.to(
            new FileBasedSink<Integer, Void, Integer>(
                StaticValueProvider.of(outputDirectory), DynamicFileDestinations.constant(policy)) {
              @Override
              public WriteOperation<Void, Integer> createWriteOperation() {
                return null;
              }
            });
    assertThat(
        PTransformMatchers.writeWithRunnerDeterminedSharding().matches(appliedWrite(write)),
        is(true));

    WriteFiles<Integer, Void, Integer> withStaticSharding = write.withNumShards(3);
    assertThat(
        PTransformMatchers.writeWithRunnerDeterminedSharding()
            .matches(appliedWrite(withStaticSharding)),
        is(false));

    WriteFiles<Integer, Void, Integer> withCustomSharding =
        write.withSharding(Sum.integersGlobally().asSingletonView());
    assertThat(
        PTransformMatchers.writeWithRunnerDeterminedSharding()
            .matches(appliedWrite(withCustomSharding)),
        is(false));
  }

  private AppliedPTransform<?, ?, ?> appliedWrite(WriteFiles<Integer, Void, Integer> write) {
    return AppliedPTransform.of(
        "WriteFiles", Collections.emptyMap(), Collections.emptyMap(), write, p);
  }

  private static class FakeFilenamePolicy extends FilenamePolicy {
    @Override
    public ResourceId windowedFilename(
        int shardNumber,
        int numShards,
        BoundedWindow window,
        PaneInfo paneInfo,
        FileBasedSink.OutputFileHints outputFileHints) {
      throw new UnsupportedOperationException("should not be called");
    }

    @Nullable
    @Override
    public ResourceId unwindowedFilename(
        int shardNumber, int numShards, FileBasedSink.OutputFileHints outputFileHints) {
      throw new UnsupportedOperationException("should not be called");
    }
  }
}
