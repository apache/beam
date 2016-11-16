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
package org.apache.beam.runners.direct;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.Serializable;
import org.apache.beam.runners.direct.DirectRunner.CommittedBundle;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.util.state.CopyOnAccessInMemoryStateInternals;
import org.apache.beam.sdk.util.state.StateInternals;
import org.apache.beam.sdk.util.state.StateNamespace;
import org.apache.beam.sdk.util.state.StateNamespaces;
import org.apache.beam.sdk.util.state.StateSpec;
import org.apache.beam.sdk.util.state.StateSpecs;
import org.apache.beam.sdk.util.state.StateTag;
import org.apache.beam.sdk.util.state.StateTags;
import org.apache.beam.sdk.util.state.ValueState;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

/** Tests for {@link ParDoEvaluator}. */
@RunWith(JUnit4.class)
public class ParDoEvaluatorFactoryTest implements Serializable {
  @Mock private transient EvaluationContext mockEvaluationContext;
  @Mock private transient DirectExecutionContext mockExecutionContext;
  @Mock private transient DirectExecutionContext.DirectStepContext mockStepContext;
  @Mock private transient ParDoEvaluatorFactory.TransformHooks mockHooks;

  private static final String KEY = "any-key";
  private transient StateInternals<Object> stateInternals =
      CopyOnAccessInMemoryStateInternals.<Object>withUnderlying(KEY, null);

  private static final BundleFactory BUNDLE_FACTORY = ImmutableListBundleFactory.create();

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    when((StateInternals<Object>) mockStepContext.stateInternals()).thenReturn(stateInternals);
  }

  @Test
  public void windowCleanupScheduled() throws Exception {
    // To test the factory, first we set up a pipeline and then we use the constructed
    // pipeline to create the right parameters to pass to the factory
    TestPipeline pipeline = TestPipeline.create();

    final String stateId = "my-state-id";

    // For consistency, window it into FixedWindows. Actually we will fabricate an input bundle.
    PCollection<Integer> input =
        pipeline
            .apply(Create.of(1, 2, 3))
            .apply(Window.<Integer>into(FixedWindows.of(Duration.millis(10))));

    PCollection<Integer> produced =
        input.apply(
            ParDo.of(
                new DoFn<Integer, Integer>() {
                  @StateId(stateId)
                  private final StateSpec<Object, ValueState<String>> spec =
                      StateSpecs.value(StringUtf8Coder.of());

                  @ProcessElement
                  public void process(ProcessContext c) {}
                }));

    ParDoEvaluatorFactory<
            Integer, Integer, PCollection<Integer>,
            PTransform<PCollection<? extends Integer>, PCollection<Integer>>>
        factory = new ParDoEvaluatorFactory(mockEvaluationContext, mockHooks);

    AppliedPTransform<
            PCollection<? extends Integer>, PCollection<Integer>, ParDo.Bound<Integer, Integer>>
        producingTransform = (AppliedPTransform) produced.getProducingTransformInternal();

    // The DoFn will be extracted from the hooks, which encapsulates single vs multi
    when(mockHooks.getDoFn(producingTransform.getTransform()))
        .thenReturn(producingTransform.getTransform().getNewFn());

    // Then there will be a digging down to the step context to get the state internals
    when(mockEvaluationContext.getExecutionContext(
            eq(producingTransform), Mockito.<StructuralKey>any()))
        .thenReturn(mockExecutionContext);
    when(mockExecutionContext.getOrCreateStepContext(anyString(), anyString()))
        .thenReturn(mockStepContext);

    IntervalWindow firstWindow = new IntervalWindow(new Instant(0), new Instant(9));
    IntervalWindow secondWindow = new IntervalWindow(new Instant(10), new Instant(19));

    StateNamespace firstWindowNamespace =
        StateNamespaces.window(IntervalWindow.getCoder(), firstWindow);
    StateNamespace secondWindowNamespace =
        StateNamespaces.window(IntervalWindow.getCoder(), secondWindow);
    StateTag<Object, ValueState<String>> tag =
        StateTags.tagForSpec(stateId, StateSpecs.value(StringUtf8Coder.of()));

    // Set up non-empty state. We don't mock + verify calls to clear() but instead
    // check that state is actually empty. We musn't care how it is accomplished.
    stateInternals.state(firstWindowNamespace, tag).write("first");
    stateInternals.state(secondWindowNamespace, tag).write("second");

    // A single bundle with some elements in the global window; it should register cleanup for the
    // global window state merely by having the evaluator created. The cleanup logic does not
    // depend on the window.
    CommittedBundle<Integer> inputBundle =
        BUNDLE_FACTORY
            .createBundle(input)
            .add(WindowedValue.of(1, new Instant(3), firstWindow, PaneInfo.NO_FIRING))
            .add(WindowedValue.of(2, new Instant(11), secondWindow, PaneInfo.NO_FIRING))
            .commit(Instant.now());
    TransformEvaluator<Integer> evaluator = factory.forApplication(producingTransform, inputBundle);

    ArgumentCaptor<Runnable> argumentCaptor = ArgumentCaptor.forClass(Runnable.class);
    verify(mockEvaluationContext)
        .scheduleAfterWindowExpiration(
            eq(produced),
            eq(firstWindow),
            Mockito.<WindowingStrategy<?, ?>>any(),
            argumentCaptor.capture());

    // Should actually clear the state for the first window
    argumentCaptor.getValue().run();
    assertThat(stateInternals.state(firstWindowNamespace, tag).read(), nullValue());
    assertThat(stateInternals.state(secondWindowNamespace, tag).read(), equalTo("second"));

    verify(mockEvaluationContext)
        .scheduleAfterWindowExpiration(
            eq(produced),
            eq(secondWindow),
            Mockito.<WindowingStrategy<?, ?>>any(),
            argumentCaptor.capture());

    // Should actually clear the state for the second window
    argumentCaptor.getValue().run();
    assertThat(stateInternals.state(secondWindowNamespace, tag).read(), nullValue());
  }
}
