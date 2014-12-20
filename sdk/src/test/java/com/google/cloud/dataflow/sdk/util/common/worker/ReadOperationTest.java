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

import static com.google.cloud.dataflow.sdk.runners.worker.SourceTranslationUtils.cloudProgressToReaderProgress;
import static com.google.cloud.dataflow.sdk.runners.worker.SourceTranslationUtils.sourcePositionToCloudPosition;
import static com.google.cloud.dataflow.sdk.runners.worker.SourceTranslationUtils.sourceProgressToCloudProgress;
import static com.google.cloud.dataflow.sdk.util.common.Counter.AggregationKind.MEAN;
import static com.google.cloud.dataflow.sdk.util.common.Counter.AggregationKind.SUM;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.everyItem;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;

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
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Tests for ReadOperation.
 */
@RunWith(JUnit4.class)
public class ReadOperationTest {
  private static final long ITERATIONS = 3L;

  /**
   * The test Reader for testing updating stop position and progress report.
   * The number of read iterations is controlled by ITERATIONS.
   */
  static class TestTextReader extends Reader<String> {
    @Override
    public ReaderIterator<String> iterator() {
      return new TestTextReaderIterator();
    }

    class TestTextReaderIterator extends AbstractReaderIterator<String> {
      long offset = 0L;
      List<com.google.api.services.dataflow.model.Position> proposedPositions = new ArrayList<>();

      @Override
      public boolean hasNext() {
        return offset < ITERATIONS;
      }

      @Override
      public String next() {
        if (hasNext()) {
          offset++;
          return "hi";
        } else {
          throw new AssertionError("No next Element.");
        }
      }

      @Override
      public Progress getProgress() {
        com.google.api.services.dataflow.model.Position currentPosition =
            new com.google.api.services.dataflow.model.Position();
        currentPosition.setByteOffset(offset);

        ApproximateProgress progress = new ApproximateProgress();
        progress.setPosition(currentPosition);

        return cloudProgressToReaderProgress(progress);
      }

      @Override
      public Position updateStopPosition(Progress proposedStopPosition) {
        proposedPositions.add(sourceProgressToCloudProgress(proposedStopPosition).getPosition());
        // Actually no update happens, returns null.
        return null;
      }
    }
  }

  /**
   * The OutputReceiver for testing updating stop position and progress report.
   * The offset of the Reader (iterator) will be advanced each time this
   * Receiver processes a record.
   */
  static class TestTextReceiver extends OutputReceiver {
    ReadOperation readOperation = null;
    com.google.api.services.dataflow.model.Position proposedStopPosition = null;
    List<ApproximateProgress> progresses = new ArrayList<>();

    public TestTextReceiver(CounterSet counterSet, String counterPrefix) {
      super("test_receiver_out", counterPrefix, counterSet.getAddCounterMutator());
    }

    public void setReadOperation(ReadOperation readOp) {
      this.readOperation = readOp;
    }

    public void setProposedStopPosition(com.google.api.services.dataflow.model.Position position) {
      this.proposedStopPosition = position;
    }

    @Override
    public void process(Object outputElem) throws Exception {
      // Calls getProgress() and proposeStopPosition() in each iteration.
      progresses.add(sourceProgressToCloudProgress(readOperation.getProgress()));
      // We expect that call to proposeStopPosition is a no-op that does not
      // update the stop position for every iteration. We will verify it is
      // delegated to ReaderIterator after ReadOperation finishes.
      Assert.assertNull(readOperation.proposeStopPosition(
          cloudProgressToReaderProgress(makeApproximateProgress(proposedStopPosition))));
    }
  }

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
  public void testGetProgressAndProposeStopPosition() throws Exception {
    TestTextReader testTextReader = new TestTextReader();
    CounterSet counterSet = new CounterSet();
    String counterPrefix = "test-";
    StateSampler stateSampler = new StateSampler(counterPrefix, counterSet.getAddCounterMutator());
    TestTextReceiver receiver = new TestTextReceiver(counterSet, counterPrefix);
    ReadOperation readOperation = new ReadOperation(
        testTextReader, receiver, counterPrefix, counterSet.getAddCounterMutator(), stateSampler);
    readOperation.setProgressUpdatePeriodMs(0);
    receiver.setReadOperation(readOperation);

    Position proposedStopPosition = makePosition(3L);
    receiver.setProposedStopPosition(proposedStopPosition);

    Assert.assertNull(readOperation.getProgress());
    Assert.assertNull(readOperation.proposeStopPosition(
        cloudProgressToReaderProgress(makeApproximateProgress(proposedStopPosition))));

    readOperation.start();
    readOperation.finish();

    TestTextReader.TestTextReaderIterator testIterator =
        (TestTextReader.TestTextReaderIterator) readOperation.readerIterator;

    Assert.assertEquals(
        sourceProgressToCloudProgress(testIterator.getProgress()),
        sourceProgressToCloudProgress(readOperation.getProgress()));
    Assert.assertEquals(
        sourcePositionToCloudPosition(testIterator.updateStopPosition(
            cloudProgressToReaderProgress(makeApproximateProgress(proposedStopPosition)))),
        sourcePositionToCloudPosition(readOperation.proposeStopPosition(
            cloudProgressToReaderProgress(makeApproximateProgress(proposedStopPosition)))));

    // Verifies progress report and stop position updates.
    Assert.assertEquals(testIterator.proposedPositions.size(), ITERATIONS + 2);
    Assert.assertThat(testIterator.proposedPositions, everyItem(equalTo(makePosition(3L))));
    Assert.assertThat(
        receiver.progresses,
        contains(
            makeApproximateProgress(1L), makeApproximateProgress(2L), makeApproximateProgress(3L)));
  }

  @Test
  public void testGetProgressDoesNotBlock() throws Exception {
    final BlockingQueue<Integer> queue = new LinkedBlockingQueue<>();
    final Reader.ReaderIterator<Integer> iterator = new Reader.AbstractReaderIterator<Integer>() {
      private int itemsReturned = 0;

      @Override
      public boolean hasNext() throws IOException {
        return itemsReturned < 5;
      }

      @Override
      public Integer next() throws IOException {
        ++itemsReturned;
        try {
          return queue.take();
        } catch (InterruptedException e) {
          throw new NoSuchElementException("interrupted");
        }
      }

      @Override
      public Reader.Progress getProgress() {
        return cloudProgressToReaderProgress(new ApproximateProgress().setPosition(
            new Position().setRecordIndex((long) itemsReturned)));
      }
    };

    Reader<Integer> reader = new Reader<Integer>() {
      @Override
      public ReaderIterator<Integer> iterator() throws IOException {
        return iterator;
      }
    };

    CounterSet counterSet = new CounterSet();
    String counterPrefix = "test-";
    StateSampler stateSampler = new StateSampler(counterPrefix, counterSet.getAddCounterMutator());
    TestTextReceiver receiver = new TestTextReceiver(counterSet, counterPrefix);
    final ReadOperation readOperation = new ReadOperation(
        reader, receiver, counterPrefix, counterSet.getAddCounterMutator(), stateSampler);
    // Update progress not continuously, but so that it's never more than 1 record stale.
    readOperation.setProgressUpdatePeriodMs(150);
    receiver.setReadOperation(readOperation);

    new Thread() {
      @Override
      public void run() {
        try {
          readOperation.start();
          readOperation.finish();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }.start();

    for (int i = 0; i < 5; ++i) {
      Thread.sleep(100); // Wait for the operation to start and block.
      // Ensure that getProgress() doesn't block.
      ApproximateProgress progress = sourceProgressToCloudProgress(readOperation.getProgress());
      long observedIndex = progress.getPosition().getRecordIndex().longValue();
      Assert.assertTrue("Actual: " + observedIndex, i == observedIndex || i == observedIndex + 1);
      queue.offer(i);
    }
  }

  private static Position makePosition(long offset) {
    return new Position().setByteOffset(offset);
  }

  private static ApproximateProgress makeApproximateProgress(long offset) {
    return makeApproximateProgress(makePosition(offset));
  }

  private static ApproximateProgress makeApproximateProgress(
      com.google.api.services.dataflow.model.Position position) {
    return new ApproximateProgress().setPosition(position);
  }
}
