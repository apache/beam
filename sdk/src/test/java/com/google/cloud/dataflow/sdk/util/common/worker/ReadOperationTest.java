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

import static com.google.cloud.dataflow.sdk.runners.worker.ReaderTestUtils.forkRequestAtIndex;
import static com.google.cloud.dataflow.sdk.runners.worker.ReaderTestUtils.positionAtIndex;
import static com.google.cloud.dataflow.sdk.runners.worker.SourceTranslationUtils.cloudPositionToReaderPosition;
import static com.google.cloud.dataflow.sdk.runners.worker.SourceTranslationUtils.cloudProgressToReaderProgress;
import static com.google.cloud.dataflow.sdk.runners.worker.SourceTranslationUtils.forkRequestToApproximateProgress;
import static com.google.cloud.dataflow.sdk.runners.worker.SourceTranslationUtils.readerProgressToCloudProgress;
import static com.google.cloud.dataflow.sdk.runners.worker.SourceTranslationUtils.toCloudPosition;
import static com.google.cloud.dataflow.sdk.util.common.Counter.AggregationKind.MEAN;
import static com.google.cloud.dataflow.sdk.util.common.Counter.AggregationKind.SUM;

import com.google.api.services.dataflow.model.ApproximateProgress;
import com.google.api.services.dataflow.model.Position;
import com.google.cloud.dataflow.sdk.util.common.Counter;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.util.common.worker.ExecutorTestUtils.TestReader;
import com.google.cloud.dataflow.sdk.util.common.worker.ExecutorTestUtils.TestReceiver;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.concurrent.Exchanger;

/**
 * Tests for ReadOperation.
 */
@RunWith(JUnit4.class)
public class ReadOperationTest {
  @Test
  @SuppressWarnings("unchecked")
  public void testRunReadOperation() throws Exception {
    TestReader reader = new TestReader();
    reader.addInput("hi", "there", "", "bob");

    CounterSet counterSet = new CounterSet();
    String counterPrefix = "test-";
    StateSampler stateSampler = new StateSampler(counterPrefix, counterSet.getAddCounterMutator());
    TestReceiver receiver = new TestReceiver(counterSet, counterPrefix);

    ReadOperation readOperation = new ReadOperation(
        reader, receiver, counterPrefix, counterSet.getAddCounterMutator(), stateSampler);

    readOperation.start();
    readOperation.finish();

    Assert.assertThat(
        receiver.outputElems, CoreMatchers.<Object>hasItems("hi", "there", "", "bob"));

    Assert
        .assertEquals(
            new CounterSet(
                Counter.longs("ReadOperation-ByteCount", SUM).resetToValue(2L + 5 + 0 + 3),
                Counter.longs("test_receiver_out-ElementCount", SUM).resetToValue(4L),
                Counter.longs("test_receiver_out-MeanByteCount", MEAN).resetToValue(4, 10L),
                Counter.longs("test-ReadOperation-start-msecs", SUM)
                    .resetToValue(((Counter<Long>) counterSet.getExistingCounter(
                                       "test-ReadOperation-start-msecs")).getAggregate(false)),
                Counter.longs("test-ReadOperation-read-msecs", SUM)
                    .resetToValue(((Counter<Long>) counterSet.getExistingCounter(
                                       "test-ReadOperation-read-msecs")).getAggregate(false)),
                Counter.longs("test-ReadOperation-process-msecs", SUM)
                    .resetToValue(((Counter<Long>) counterSet.getExistingCounter(
                                       "test-ReadOperation-process-msecs")).getAggregate(false)),
                Counter.longs("test-ReadOperation-finish-msecs", SUM)
                    .resetToValue(((Counter<Long>) counterSet.getExistingCounter(
                                       "test-ReadOperation-finish-msecs")).getAggregate(false))),
            counterSet);
  }

  @Test
  public void testGetProgress() throws Exception {
    MockReaderIterator iterator = new MockReaderIterator(0, 5);
    CounterSet counterSet = new CounterSet();
    String counterPrefix = "test-";
    final ReadOperation readOperation = new ReadOperation(new MockReader(iterator),
        new OutputReceiver("out", "test-", counterSet.getAddCounterMutator()), counterPrefix,
        counterSet.getAddCounterMutator(),
        new StateSampler(counterPrefix, counterSet.getAddCounterMutator()));
    // Update progress not continuously, but so that it's never more than 1 record stale.
    readOperation.setProgressUpdatePeriodMs(150);

    Thread thread = runReadLoopInThread(readOperation);
    for (int i = 0; i < 5; ++i) {
      Thread.sleep(300); // Wait for the operation to start and block.
      // Ensure that getProgress() doesn't block while the next() method is blocked.
      ApproximateProgress progress = readerProgressToCloudProgress(readOperation.getProgress());
      long observedIndex = progress.getPosition().getRecordIndex().longValue();
      Assert.assertTrue("Actual: " + observedIndex, i == observedIndex || i == observedIndex + 1);
      iterator.offerNext(i);
    }
    thread.join();
  }

  @Test
  public void testFork() throws Exception {
    MockReaderIterator iterator = new MockReaderIterator(0, 10);
    CounterSet counterSet = new CounterSet();
    MockOutputReceiver receiver = new MockOutputReceiver(counterSet.getAddCounterMutator());
    ReadOperation readOperation = new ReadOperation(new MockReader(iterator), receiver, "test-",
        counterSet.getAddCounterMutator(),
        new StateSampler("test-", counterSet.getAddCounterMutator()));
    // Update progress on every iteration of the read loop.
    readOperation.setProgressUpdatePeriodMs(0);

    // An unstarted ReadOperation refuses fork requests.
    Assert.assertNull(
        readOperation.requestFork(forkRequestAtIndex(7L)));

    Thread thread = runReadLoopInThread(readOperation);
    iterator.offerNext(0); // Await first next() and return 0 from it.
    // Read loop is now blocked in process() (not next()).
    Reader.ForkResultWithPosition fork = (Reader.ForkResultWithPosition) readOperation.requestFork(
        forkRequestAtIndex(7L));
    Assert.assertNotNull(fork);
    Assert.assertEquals(positionAtIndex(7L), toCloudPosition(fork.getAcceptedPosition()));
    receiver.unblockProcess();
    iterator.offerNext(1);
    receiver.unblockProcess();
    iterator.offerNext(2);

    // Should accept a fork at an earlier position than previously requested.
    // Should reject a fork at a later position than previously requested.
    // Note that here we're testing our own MockReaderIterator class, so it's kind of pointless,
    // but we're also testing that ReadOperation correctly relays the request to the iterator.
    fork = (Reader.ForkResultWithPosition) readOperation.requestFork(forkRequestAtIndex(5L));
    Assert.assertNotNull(fork);
    Assert.assertEquals(positionAtIndex(5L), toCloudPosition(fork.getAcceptedPosition()));
    fork = (Reader.ForkResultWithPosition) readOperation.requestFork(forkRequestAtIndex(5L));
    Assert.assertNull(fork);
    receiver.unblockProcess();

    iterator.offerNext(3);
    receiver.unblockProcess();
    iterator.offerNext(4);
    receiver.unblockProcess();

    // Should return false from hasNext() and exit read loop now.

    thread.join();

    // Operation is now finished. Check that it refuses a fork request.
    Assert.assertNull(readOperation.requestFork(forkRequestAtIndex(5L)));
  }

  private Thread runReadLoopInThread(final ReadOperation readOperation) {
    Thread thread = new Thread() {
      @Override
      public void run() {
        try {
          readOperation.start();
          readOperation.finish();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    };
    thread.start();
    return thread;
  }

  private static class MockReaderIterator extends Reader.AbstractReaderIterator<Integer> {
    private int to;
    private Exchanger<Integer> exchanger = new Exchanger<>();
    private int current;

    public MockReaderIterator(int from, int to) {
      this.current = from;
      this.to = to;
    }

    @Override
    public boolean hasNext() throws IOException {
      return current < to;
    }

    @Override
    public Integer next() throws IOException {
      ++current;
      try {
        return exchanger.exchange(current);
      } catch (InterruptedException e) {
        throw new NoSuchElementException("interrupted");
      }
    }

    @Override
    public Reader.Progress getProgress() {
      return cloudProgressToReaderProgress(
          new ApproximateProgress().setPosition(new Position().setRecordIndex((long) current)));
    }

    @Override
    public Reader.ForkResult requestFork(Reader.ForkRequest forkRequest) {
      ApproximateProgress progress = forkRequestToApproximateProgress(forkRequest);
      int index = progress.getPosition().getRecordIndex().intValue();
      if (index >= to) {
        return null;
      } else {
        this.to = index;
        return new Reader.ForkResultWithPosition(
            cloudPositionToReaderPosition(progress.getPosition()));
      }
    }

    public int offerNext(int next) {
      try {
        return exchanger.exchange(next);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static class MockReader extends Reader<Integer> {
    private ReaderIterator<Integer> iterator;

    private MockReader(ReaderIterator<Integer> iterator) {
      this.iterator = iterator;
    }

    @Override
    public ReaderIterator<Integer> iterator() throws IOException {
      return iterator;
    }
  }

  private static class MockOutputReceiver extends OutputReceiver {
    private Exchanger<Object> exchanger = new Exchanger<>();

    MockOutputReceiver(CounterSet.AddCounterMutator mutator) {
      super("out", "test-", mutator);
    }

    @Override
    public void process(Object elem) throws Exception {
      exchanger.exchange(null);
    }

    public void unblockProcess() {
      try {
        exchanger.exchange(null);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
