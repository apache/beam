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

package org.apache.beam.runners.core;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

import com.google.common.base.MoreObjects;
import java.io.Serializable;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.runners.PTransformMatcher;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.TimeDomain;
import org.apache.beam.sdk.util.Timer;
import org.apache.beam.sdk.util.TimerSpec;
import org.apache.beam.sdk.util.TimerSpecs;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.util.state.StateSpec;
import org.apache.beam.sdk.util.state.StateSpecs;
import org.apache.beam.sdk.util.state.ValueState;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.hamcrest.Matchers;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link PTransformMatcher}.
 */
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
            p, WindowingStrategy.globalDefault(), IsBounded.BOUNDED);
    PCollection<Integer> output =
        PCollection.createPrimitiveOutputInternal(
            p, WindowingStrategy.globalDefault(), IsBounded.BOUNDED);

    return AppliedPTransform.of("pardo", input.expand(), output.expand(), pardo, p);
  }

  @Test
  public void classEqualToMatchesSameClass() {
    PTransformMatcher matcher = PTransformMatchers.classEqualTo(ParDo.Bound.class);
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
            input.getPipeline(), input.getWindowingStrategy(), input.isBounded());
      }
    }
    PTransformMatcher matcher = PTransformMatchers.classEqualTo(MyPTransform.class);
    MyPTransform subclass = new MyPTransform() {};

    assertThat(subclass.getClass(), not(Matchers.<Class<?>>equalTo(MyPTransform.class)));
    assertThat(subclass, instanceOf(MyPTransform.class));

    AppliedPTransform<?, ?, ?> application =
        getAppliedTransform(subclass);

    assertThat(matcher.matches(application), is(false));
  }

  @Test
  public void classEqualToDoesNotMatchUnrelatedClass() {
    PTransformMatcher matcher = PTransformMatchers.classEqualTo(ParDo.Bound.class);
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
  private abstract static class SomeTracker implements RestrictionTracker<Void> {}
  private DoFn<KV<String, Integer>, Integer> splittableDoFn =
      new DoFn<KV<String, Integer>, Integer>() {
        @ProcessElement
        public void processElement(ProcessContext context, SomeTracker tracker) {}

        @GetInitialRestriction
        public Void getInitialRestriction(KV<String, Integer> element) {
          return null;
        }

        @NewTracker
        public SomeTracker newTracker(Void restriction) {
          return null;
        }
      };
  private DoFn<KV<String, Integer>, Integer> doFnWithState =
      new DoFn<KV<String, Integer>, Integer>() {
        private final String stateId = "mystate";

        @StateId(stateId)
        private final StateSpec<Object, ValueState<Integer>> intState =
            StateSpecs.value(VarIntCoder.of());

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
          timer.setForNowPlus(Duration.standardSeconds(1));
          context.output(3);
        }

        @OnTimer(timerId)
        public void onTimer(OnTimerContext context) {
          context.output(42);
        }
      };

  /**
   * Demonstrates that a {@link ParDo.Bound} does not match any ParDo matcher.
   */
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
    AppliedPTransform<?, ?, ?> parDoApplication =
        getAppliedTransform(ParDo.of(doFnWithTimers));
    assertThat(PTransformMatchers.stateOrTimerParDoSingle().matches(parDoApplication), is(true));

    assertThat(PTransformMatchers.splittableParDoMulti().matches(parDoApplication), is(false));
    assertThat(PTransformMatchers.stateOrTimerParDoMulti().matches(parDoApplication), is(false));
    assertThat(PTransformMatchers.splittableParDoSingle().matches(parDoApplication), is(false));
  }

  @Test
  public void parDoMulti() {
    AppliedPTransform<?, ?, ?> parDoApplication =
        getAppliedTransform(
            ParDo.of(doFn).withOutputTags(new TupleTag<Integer>(), TupleTagList.empty()));

    assertThat(PTransformMatchers.splittableParDoMulti().matches(parDoApplication), is(false));
    assertThat(PTransformMatchers.stateOrTimerParDoMulti().matches(parDoApplication), is(false));
    assertThat(PTransformMatchers.splittableParDoSingle().matches(parDoApplication), is(false));
    assertThat(PTransformMatchers.stateOrTimerParDoSingle().matches(parDoApplication), is(false));
  }

  @Test
  public void parDoMultiSplittable() {
    AppliedPTransform<?, ?, ?> parDoApplication =
        getAppliedTransform(
            ParDo.of(splittableDoFn).withOutputTags(new TupleTag<Integer>(), TupleTagList.empty()));
    assertThat(PTransformMatchers.splittableParDoMulti().matches(parDoApplication), is(true));

    assertThat(PTransformMatchers.stateOrTimerParDoMulti().matches(parDoApplication), is(false));
    assertThat(PTransformMatchers.splittableParDoSingle().matches(parDoApplication), is(false));
    assertThat(PTransformMatchers.stateOrTimerParDoSingle().matches(parDoApplication), is(false));
  }

  @Test
  public void parDoMultiWithState() {
    AppliedPTransform<?, ?, ?> parDoApplication =
        getAppliedTransform(
            ParDo.of(doFnWithState).withOutputTags(new TupleTag<Integer>(), TupleTagList.empty()));
    assertThat(PTransformMatchers.stateOrTimerParDoMulti().matches(parDoApplication), is(true));

    assertThat(PTransformMatchers.splittableParDoMulti().matches(parDoApplication), is(false));
    assertThat(PTransformMatchers.splittableParDoSingle().matches(parDoApplication), is(false));
    assertThat(PTransformMatchers.stateOrTimerParDoSingle().matches(parDoApplication), is(false));
  }

  @Test
  public void parDoMultiWithTimers() {
    AppliedPTransform<?, ?, ?> parDoApplication =
        getAppliedTransform(
            ParDo.of(doFnWithTimers).withOutputTags(new TupleTag<Integer>(), TupleTagList.empty()));
    assertThat(PTransformMatchers.stateOrTimerParDoMulti().matches(parDoApplication), is(true));

    assertThat(PTransformMatchers.splittableParDoMulti().matches(parDoApplication), is(false));
    assertThat(PTransformMatchers.splittableParDoSingle().matches(parDoApplication), is(false));
    assertThat(PTransformMatchers.stateOrTimerParDoSingle().matches(parDoApplication), is(false));
  }
}
