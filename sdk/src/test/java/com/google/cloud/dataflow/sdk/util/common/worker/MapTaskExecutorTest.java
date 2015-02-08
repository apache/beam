/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.util.common.worker;

import static com.google.cloud.dataflow.sdk.runners.worker.ReaderTestUtils.approximateProgressAtIndex;
import static com.google.cloud.dataflow.sdk.runners.worker.ReaderTestUtils.forkRequestAtIndex;
import static com.google.cloud.dataflow.sdk.runners.worker.ReaderTestUtils.positionAtIndex;
import static com.google.cloud.dataflow.sdk.runners.worker.ReaderTestUtils.positionFromForkResult;
import static com.google.cloud.dataflow.sdk.runners.worker.ReaderTestUtils.positionFromProgress;
import static com.google.cloud.dataflow.sdk.runners.worker.SourceTranslationUtils.cloudPositionToReaderPosition;
import static com.google.cloud.dataflow.sdk.runners.worker.SourceTranslationUtils.cloudProgressToReaderProgress;
import static com.google.cloud.dataflow.sdk.runners.worker.SourceTranslationUtils.forkRequestToApproximateProgress;
import static com.google.cloud.dataflow.sdk.util.common.Counter.AggregationKind.SUM;

import com.google.api.services.dataflow.model.ApproximateProgress;
import com.google.cloud.dataflow.sdk.util.common.Counter;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.util.common.CounterSet.AddCounterMutator;
import com.google.cloud.dataflow.sdk.util.common.worker.ExecutorTestUtils.TestReader;
import com.google.cloud.dataflow.sdk.util.common.worker.ExecutorTestUtils.TestReceiver;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Tests for MapTaskExecutor.
 */
@RunWith(JUnit4.class)
public class MapTaskExecutorTest {
  static class TestOperation extends Operation {
    String label;
    List<String> log;

    private static CounterSet counterSet = new CounterSet();
    private static String counterPrefix = "test-";
    private static StateSampler stateSampler =
        new StateSampler(counterPrefix, counterSet.getAddCounterMutator());

    TestOperation(String label, List<String> log) {
      super(label, new OutputReceiver[] {}, counterPrefix, counterSet.getAddCounterMutator(),
          stateSampler);
      this.label = label;
      this.log = log;
    }

    TestOperation(String outputName, String counterPrefix,
        CounterSet.AddCounterMutator addCounterMutator, StateSampler stateSampler,
        long outputCount) {
      super(outputName, new OutputReceiver[] {}, counterPrefix, addCounterMutator, stateSampler);
      addCounterMutator.addCounter(
          Counter.longs(outputName + "-ElementCount", SUM).resetToValue(outputCount));
    }

    @Override
    public void start() throws Exception {
      super.start();
      log.add(label + " started");
    }

    @Override
    public void finish() throws Exception {
      log.add(label + " finished");
      super.finish();
    }
  }

  // A mock ReadOperation fed to a MapTaskExecutor in test.
  static class TestReadOperation extends ReadOperation {
    private ApproximateProgress progress = null;

    TestReadOperation(OutputReceiver outputReceiver, String counterPrefix,
        AddCounterMutator addCounterMutator, StateSampler stateSampler) {
      super(new TestReader(), outputReceiver, counterPrefix, addCounterMutator, stateSampler);
    }

    @Override
    public Reader.Progress getProgress() {
      return cloudProgressToReaderProgress(progress);
    }

    @Override
    public Reader.ForkResult requestFork(Reader.ForkRequest forkRequest) {
      // Fakes the return with the same position as proposed.
      return new Reader.ForkResultWithPosition(cloudPositionToReaderPosition(
          forkRequestToApproximateProgress(forkRequest).getPosition()));
    }

    public void setProgress(ApproximateProgress progress) {
      this.progress = progress;
    }
  }

  @Test
  public void testExecuteMapTaskExecutor() throws Exception {
    List<String> log = new ArrayList<>();

    List<Operation> operations = Arrays.asList(new Operation[] {
        new TestOperation("o1", log), new TestOperation("o2", log), new TestOperation("o3", log)});

    CounterSet counters = new CounterSet();
    String counterPrefix = "test-";
    StateSampler stateSampler = new StateSampler(counterPrefix, counters.getAddCounterMutator());
    MapTaskExecutor executor = new MapTaskExecutor(operations, counters, stateSampler);

    executor.execute();

    Assert.assertThat(
        log,
        CoreMatchers.hasItems(
            "o3 started", "o2 started", "o1 started", "o1 finished", "o2 finished", "o3 finished"));

    executor.close();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGetOutputCounters() throws Exception {
    CounterSet counters = new CounterSet();
    String counterPrefix = "test-";
    StateSampler stateSampler = new StateSampler(counterPrefix, counters.getAddCounterMutator());
    List<Operation> operations = Arrays.asList(new Operation[] {
        new TestOperation("o1", counterPrefix, counters.getAddCounterMutator(), stateSampler, 1),
        new TestOperation("o2", counterPrefix, counters.getAddCounterMutator(), stateSampler, 2),
        new TestOperation("o3", counterPrefix, counters.getAddCounterMutator(), stateSampler, 3)});

    MapTaskExecutor executor = new MapTaskExecutor(operations, counters, stateSampler);

    CounterSet counterSet = executor.getOutputCounters();
    Assert.assertEquals(
        new CounterSet(Counter.longs("o1-ElementCount", SUM).resetToValue(1L),
            Counter.longs("test-o1-start-msecs", SUM)
                .resetToValue(
                    ((Counter<Long>)
                        counterSet.getExistingCounter("test-o1-start-msecs")).getAggregate(false)),
            Counter.longs("test-o1-process-msecs", SUM)
                .resetToValue(((Counter<Long>) counterSet.getExistingCounter(
                                   "test-o1-process-msecs")).getAggregate(false)),
            Counter.longs("test-o1-finish-msecs", SUM)
                .resetToValue(
                    ((Counter<Long>)
                        counterSet.getExistingCounter("test-o1-finish-msecs")).getAggregate(false)),
            Counter.longs("o2-ElementCount", SUM).resetToValue(2L),
            Counter.longs("test-o2-start-msecs", SUM)
                .resetToValue(
                    ((Counter<Long>)
                        counterSet.getExistingCounter("test-o2-start-msecs")).getAggregate(false)),
            Counter.longs("test-o2-process-msecs", SUM)
                .resetToValue(((Counter<Long>) counterSet.getExistingCounter(
                                   "test-o2-process-msecs")).getAggregate(false)),
            Counter.longs("test-o2-finish-msecs", SUM)
                .resetToValue(
                    ((Counter<Long>)
                        counterSet.getExistingCounter("test-o2-finish-msecs")).getAggregate(false)),
            Counter.longs("o3-ElementCount", SUM).resetToValue(3L),
            Counter.longs("test-o3-start-msecs", SUM)
                .resetToValue(
                    ((Counter<Long>)
                        counterSet.getExistingCounter("test-o3-start-msecs")).getAggregate(false)),
            Counter.longs("test-o3-process-msecs", SUM)
                .resetToValue(((Counter<Long>) counterSet.getExistingCounter(
                                   "test-o3-process-msecs")).getAggregate(false)),
            Counter.longs("test-o3-finish-msecs", SUM)
                .resetToValue(((Counter<Long>) counterSet.getExistingCounter(
                                   "test-o3-finish-msecs")).getAggregate(false))),
        counterSet);

    executor.close();
  }

  @Test
  public void testGetReadOperation() throws Exception {
    CounterSet counterSet = new CounterSet();
    String counterPrefix = "test-";
    StateSampler stateSampler = new StateSampler(counterPrefix, counterSet.getAddCounterMutator());
    // Test MapTaskExecutor without a single operation.
    MapTaskExecutor executor =
        new MapTaskExecutor(new ArrayList<Operation>(), counterSet, stateSampler);

    try {
      executor.getReadOperation();
      Assert.fail("Expected IllegalStateException.");
    } catch (IllegalStateException e) {
      // Exception expected
    }

    List<Operation> operations = Arrays.asList(new Operation[] {
        new TestOperation("o1", counterPrefix, counterSet.getAddCounterMutator(), stateSampler, 1),
        new TestOperation(
            "o2", counterPrefix, counterSet.getAddCounterMutator(), stateSampler, 2)});
    // Test MapTaskExecutor without ReadOperation.
    executor = new MapTaskExecutor(operations, counterSet, stateSampler);

    try {
      executor.getReadOperation();
      Assert.fail("Expected IllegalStateException.");
    } catch (IllegalStateException e) {
      // Exception expected
    }

    executor.close();

    TestReceiver receiver = new TestReceiver(counterSet, counterPrefix);
    operations = Arrays.asList(new Operation[] {new TestReadOperation(
        receiver, counterPrefix, counterSet.getAddCounterMutator(), stateSampler)});
    executor = new MapTaskExecutor(operations, counterSet, stateSampler);
    Assert.assertEquals(operations.get(0), executor.getReadOperation());
    executor.close();
  }

  @Test
  public void testGetProgressAndRequestSplit() throws Exception {
    CounterSet counterSet = new CounterSet();
    String counterPrefix = "test-";
    StateSampler stateSampler = new StateSampler(counterPrefix, counterSet.getAddCounterMutator());
    TestReceiver receiver = new TestReceiver(counterSet, counterPrefix);
    TestReadOperation operation = new TestReadOperation(
        receiver, counterPrefix, counterSet.getAddCounterMutator(), stateSampler);
    MapTaskExecutor executor =
        new MapTaskExecutor(Arrays.asList(new Operation[] {operation}), counterSet, stateSampler);

    operation.setProgress(approximateProgressAtIndex(1L));
    Assert.assertEquals(positionAtIndex(1L), positionFromProgress(executor.getWorkerProgress()));
    Assert.assertEquals(
        positionAtIndex(1L), positionFromForkResult(executor.requestFork(forkRequestAtIndex(1L))));

    executor.close();
  }
}
