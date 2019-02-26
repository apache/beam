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
package org.apache.beam.runners.dataflow.worker.util;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import java.util.Arrays;
import java.util.List;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.InMemoryStateInternals;
import org.apache.beam.runners.core.NullSideInputReader;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateInternalsFactory;
import org.apache.beam.runners.core.StepContext;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.dataflow.worker.GroupAlsoByWindowFnRunner;
import org.apache.beam.runners.dataflow.worker.util.GroupAlsoByWindowProperties.GroupAlsoByWindowDoFnFactory;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Unit tests for {@link BatchGroupAlsoByWindowReshuffleFn}.
 *
 * <p>Note the absence of tests for sessions, as merging window functions are not supported.
 */
@RunWith(JUnit4.class)
public class BatchGroupAlsoByWindowReshuffleDoFnTest {

  @Rule public final transient ExpectedException thrown = ExpectedException.none();

  private static final String STEP_NAME = "GABWStep";

  private class GABWReshuffleDoFnFactory
      implements GroupAlsoByWindowDoFnFactory<String, String, Iterable<String>> {
    @Override
    public <W extends BoundedWindow>
        BatchGroupAlsoByWindowFn<String, String, Iterable<String>> forStrategy(
            WindowingStrategy<?, W> windowingStrategy,
            StateInternalsFactory<String> stateInternalsFactory) {
      // ignores windowing strategy.
      return new BatchGroupAlsoByWindowReshuffleFn<String, String, W>();
    }
  }

  @SafeVarargs
  private static <K, InputT, OutputT, W extends BoundedWindow>
      List<WindowedValue<KV<K, OutputT>>> runGABW(
          GroupAlsoByWindowDoFnFactory<K, InputT, OutputT> gabwFactory,
          WindowingStrategy<?, W> windowingStrategy,
          K key,
          WindowedValue<InputT>... values) {

    TupleTag<KV<K, OutputT>> outputTag = new TupleTag<>();
    ListOutputManager outputManager = new ListOutputManager();

    DoFnRunner<KV<K, Iterable<WindowedValue<InputT>>>, KV<K, OutputT>> runner =
        makeRunner(gabwFactory, windowingStrategy, outputTag, outputManager);

    runner.startBundle();

    if (values.length > 0) {
      runner.processElement(
          new ValueInEmptyWindows<>(
              KV.of(
                  key,
                  (Iterable<WindowedValue<InputT>>) Arrays.<WindowedValue<InputT>>asList(values))));
    }

    runner.finishBundle();

    List<WindowedValue<KV<K, OutputT>>> result = outputManager.getOutput(outputTag);

    // Sanity check for corruption
    for (WindowedValue<KV<K, OutputT>> elem : result) {
      assertThat(elem.getValue().getKey(), equalTo(key));
    }

    return result;
  }

  private static <K, InputT, OutputT, W extends BoundedWindow>
      DoFnRunner<KV<K, Iterable<WindowedValue<InputT>>>, KV<K, OutputT>> makeRunner(
          GroupAlsoByWindowDoFnFactory<K, InputT, OutputT> fnFactory,
          WindowingStrategy<?, W> windowingStrategy,
          TupleTag<KV<K, OutputT>> outputTag,
          DoFnRunners.OutputManager outputManager) {

    final StepContext stepContext = new TestStepContext(STEP_NAME);

    StateInternalsFactory<K> stateInternalsFactory = key -> stepContext.stateInternals();

    BatchGroupAlsoByWindowFn<K, InputT, OutputT> fn =
        fnFactory.forStrategy(windowingStrategy, stateInternalsFactory);

    return new GroupAlsoByWindowFnRunner<>(
        PipelineOptionsFactory.create(),
        fn,
        NullSideInputReader.empty(),
        outputManager,
        outputTag,
        stepContext);
  }

  private static BoundedWindow window(long start, long end) {
    return new IntervalWindow(new Instant(start), new Instant(end));
  }

  @Test
  public void testEmptyInputEmptyOutput() throws Exception {
    GroupAlsoByWindowProperties.emptyInputEmptyOutput(new GABWReshuffleDoFnFactory());
  }

  /**
   * Tests that for a simple sequence of elements on the same key, {@link
   * BatchGroupAlsoByWindowReshuffleFn} fires each element in a single pane.
   */
  @Test
  public void testReshuffleFiresEveryElement() throws Exception {
    GABWReshuffleDoFnFactory gabwFactory = new GABWReshuffleDoFnFactory();

    WindowingStrategy<?, IntervalWindow> windowingStrategy =
        WindowingStrategy.of(FixedWindows.of(Duration.millis(10)));

    List<WindowedValue<KV<String, Iterable<String>>>> result =
        runGABW(
            gabwFactory,
            windowingStrategy,
            "key",
            WindowedValue.of(
                "v1", new Instant(1), Arrays.asList(window(0, 10)), PaneInfo.NO_FIRING),
            WindowedValue.of(
                "v2", new Instant(2), Arrays.asList(window(0, 10)), PaneInfo.NO_FIRING),
            WindowedValue.of(
                "v3", new Instant(13), Arrays.asList(window(10, 20)), PaneInfo.NO_FIRING));

    assertThat(result.size(), equalTo(3));

    WindowedValue<KV<String, Iterable<String>>> item0 = result.get(0);
    assertThat(item0.getValue().getValue(), contains("v1"));
    assertThat(item0.getTimestamp(), equalTo(new Instant(1)));
    assertThat(item0.getWindows(), contains(window(0, 10)));

    WindowedValue<KV<String, Iterable<String>>> item1 = result.get(1);
    assertThat(item1.getValue().getValue(), contains("v2"));
    assertThat(item1.getTimestamp(), equalTo(new Instant(2)));
    assertThat(item1.getWindows(), contains(window(0, 10)));

    WindowedValue<KV<String, Iterable<String>>> item2 = result.get(2);
    assertThat(item2.getValue().getValue(), contains("v3"));
    assertThat(item2.getTimestamp(), equalTo(new Instant(13)));
    assertThat(item2.getWindows(), contains(window(10, 20)));
  }

  private static final class TestStepContext implements StepContext {
    private StateInternals stateInternals;

    private TestStepContext(String stepName) {
      this.stateInternals = InMemoryStateInternals.forKey(stepName);
    }

    @Override
    public TimerInternals timerInternals() {
      throw new UnsupportedOperationException();
    }

    @Override
    public StateInternals stateInternals() {
      return stateInternals;
    }
  }
}
