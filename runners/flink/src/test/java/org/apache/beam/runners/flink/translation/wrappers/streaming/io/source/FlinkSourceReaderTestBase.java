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
package org.apache.beam.runners.flink.translation.wrappers.streaming.io.source;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nullable;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.TestCountingSource;
import org.apache.beam.sdk.io.Source;
import org.apache.beam.sdk.values.KV;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.testutils.ManuallyTriggeredScheduledExecutorService;
import org.apache.flink.metrics.Counter;
import org.junit.Test;
import org.mockito.Mockito;

/** An abstract test base for {@link FlinkSourceReaderBase}. */
public abstract class FlinkSourceReaderTestBase<OutputT> {

  // -------------- test poll --------------
  @Test(timeout = 30000L)
  public void testPollBasic() throws Exception {
    testPoll(5, 10);
  }

  @Test(timeout = 30000L)
  public void testPollFromEmptySplit() throws Exception {
    testPoll(3, 0);
  }

  @Test
  public void testPollWithTimestampExtractor() throws Exception {
    testPoll(5, 10, record -> getKVPairs(record).getValue().longValue());
  }

  @Test
  public void testExceptionInExecutorThread() throws Exception {
    try (SourceReader<OutputT, FlinkSourceSplit<KV<Integer, Integer>>> reader = createReader()) {
      reader.start();
      ReaderOutput<OutputT> output = Mockito.mock(ReaderOutput.class);
      // The first poll should not throw any exception.
      reader.pollNext(output);

      RuntimeException expectedException = new RuntimeException();
      RuntimeException suppressedException = new RuntimeException();
      ((FlinkSourceReaderBase) reader)
          .execute(
              () -> {
                throw expectedException;
              });
      ((FlinkSourceReaderBase) reader)
          .execute(
              () -> {
                throw suppressedException;
              });
      CountDownLatch countDownLatch = new CountDownLatch(1);
      ((FlinkSourceReaderBase) reader).execute(countDownLatch::countDown);
      countDownLatch.await();

      try {
        reader.pollNext(output);
        fail("Should have thrown exception here.");
      } catch (Exception e) {
        Throwable actualException = e;
        while (actualException != expectedException && actualException.getCause() != null) {
          actualException = actualException.getCause();
        }
        assertEquals(expectedException, actualException);
        assertEquals(1, actualException.getSuppressed().length);
        assertEquals(suppressedException, actualException.getSuppressed()[0]);
      }
    }
  }

  private void testPoll(int numSplits, int numRecordsPerSplit) throws Exception {
    testPoll(numSplits, numRecordsPerSplit, null);
  }

  private void testPoll(
      int numSplits, int numRecordsPerSplit, @Nullable Function<OutputT, Long> timestampExtractor)
      throws Exception {
    List<FlinkSourceSplit<KV<Integer, Integer>>> splits =
        createSplits(numSplits, numRecordsPerSplit, 0);
    try (SourceReader<OutputT, FlinkSourceSplit<KV<Integer, Integer>>> reader =
        createReader(timestampExtractor)) {
      pollAndValidate(reader, splits, timestampExtractor != null);
    }
    verifyBeamReaderClosed(splits);
  }

  // This test may fail if the subclass of FlinkSourceReaderBase overrides
  // the isAvailable() method, which should have a good reason.
  @Test
  public void testIsAvailableOnNoMoreSplitsNotification() throws Exception {
    try (SourceReader<OutputT, FlinkSourceSplit<KV<Integer, Integer>>> reader = createReader()) {
      reader.start();

      // No splits assigned yet.
      CompletableFuture<Void> future1 = reader.isAvailable();
      assertFalse("No split assigned yet, should not be available.", future1.isDone());

      // Data available on split assigned.
      reader.notifyNoMoreSplits();
      assertTrue("Future1 should be completed upon no more splits notification", future1.isDone());
      assertTrue(
          "Completed future should be returned so pollNext can be invoked to get updated INPUT_STATUS",
          reader.isAvailable().isDone());
    }
  }

  // This test may fail if the subclass of FlinkSourceReaderBase overrides
  // the isAvailable() method, which should have a good reason.
  @Test
  public void testIsAvailableWithIdleTimeout() throws Exception {
    ManuallyTriggeredScheduledExecutorService executor =
        new ManuallyTriggeredScheduledExecutorService();
    try (SourceReader<OutputT, FlinkSourceSplit<KV<Integer, Integer>>> reader =
        createReader(executor, 1L)) {
      reader.start();

      CompletableFuture<Void> future1 = reader.isAvailable();
      assertFalse("Future1 should be uncompleted without live split.", future1.isDone());

      reader.notifyNoMoreSplits();
      assertTrue("Future1 should be completed upon no more splits notification.", future1.isDone());
      CompletableFuture<Void> future2 = reader.isAvailable();
      assertFalse("Future2 should be uncompleted when waiting for idle timeout", future2.isDone());

      executor.triggerScheduledTasks();
      assertTrue("Future2 should be completed after idle timeout.", future2.isDone());
      assertTrue(
          "The future should always be completed after idle timeout.",
          reader.isAvailable().isDone());
    }
  }

  // This test may fail if the subclass of FlinkSourceReaderBase overrides
  // the isAvailable() method, which should have a good reason.
  @Test
  public void testIsAvailableWithoutIdleTimeout() throws Exception {
    try (SourceReader<OutputT, FlinkSourceSplit<KV<Integer, Integer>>> reader = createReader()) {
      reader.start();

      CompletableFuture<Void> future1 = reader.isAvailable();
      assertFalse("Future1 should be uncompleted without live split.", future1.isDone());

      reader.notifyNoMoreSplits();
      assertTrue("Future1 should be completed upon no more splits notification.", future1.isDone());
      assertTrue(
          "The future should be completed without idle timeout.", reader.isAvailable().isDone());
    }
  }

  @Test
  public void testNumBytesInMetrics() throws Exception {
    final int numSplits = 2;
    final int numRecordsPerSplit = 10;
    List<FlinkSourceSplit<KV<Integer, Integer>>> splits =
        createSplits(numSplits, numRecordsPerSplit, 0);
    SourceTestMetrics.TestMetricGroup testMetricGroup = new SourceTestMetrics.TestMetricGroup();
    try (SourceReader<OutputT, FlinkSourceSplit<KV<Integer, Integer>>> reader =
        createReader(null, -1L, null, testMetricGroup)) {
      pollAndValidate(reader, splits, false);
    }
    assertEquals(numRecordsPerSplit * numSplits, testMetricGroup.numRecordsInCounter.getCount());
  }

  @Test
  public void testMetricsContainer() throws Exception {
    ManuallyTriggeredScheduledExecutorService executor =
        new ManuallyTriggeredScheduledExecutorService();
    SourceTestMetrics.TestMetricGroup testMetricGroup = new SourceTestMetrics.TestMetricGroup();
    try (SourceReader<OutputT, FlinkSourceSplit<KV<Integer, Integer>>> reader =
        createReader(executor, 0L, null, testMetricGroup)) {
      reader.start();

      List<FlinkSourceSplit<KV<Integer, Integer>>> splits = createSplits(2, 10, 0);
      reader.addSplits(splits);
      RecordsValidatingOutput validatingOutput = new RecordsValidatingOutput(splits);

      // Need to poll once to create all the readers.
      reader.pollNext(validatingOutput);
      Counter advanceCounter =
          testMetricGroup.registeredCounter.get(
              TestCountingSource.CountingSourceReader.ADVANCE_COUNTER_NAMESPACE
                  + "."
                  + TestCountingSource.CountingSourceReader.ADVANCE_COUNTER_NAME);
      assertNotNull(advanceCounter);
      assertTrue("The reader should have advanced.", advanceCounter.getCount() > 0);
    }
  }

  // --------------- abstract methods ---------------
  protected abstract KV<Integer, Integer> getKVPairs(OutputT record);

  protected abstract SourceReader<OutputT, FlinkSourceSplit<KV<Integer, Integer>>> createReader(
      ScheduledExecutorService executor,
      long idleTimeoutMs,
      @Nullable Function<OutputT, Long> timestampExtractor,
      SourceTestMetrics.TestMetricGroup testMetricGroup);

  protected abstract Source<KV<Integer, Integer>> createBeamSource(
      int splitIndex, int numRecordsPerSplit);

  // ------------------- protected helper methods ----------------------
  protected SourceReader<OutputT, FlinkSourceSplit<KV<Integer, Integer>>> createReader() {
    return createReader(null, -1L, null, new SourceTestMetrics.TestMetricGroup());
  }

  protected SourceReader<OutputT, FlinkSourceSplit<KV<Integer, Integer>>> createReader(
      Function<OutputT, Long> timestampExtractor) {
    return createReader(null, -1L, timestampExtractor, new SourceTestMetrics.TestMetricGroup());
  }

  protected SourceReader<OutputT, FlinkSourceSplit<KV<Integer, Integer>>> createReader(
      ScheduledExecutorService executor, long idleTimeoutMs) {
    return createReader(executor, idleTimeoutMs, null, new SourceTestMetrics.TestMetricGroup());
  }

  protected SourceReader<OutputT, FlinkSourceSplit<KV<Integer, Integer>>> createReader(
      ScheduledExecutorService executor,
      long idleTimeoutMs,
      Function<OutputT, Long> timestampExtractor) {
    return createReader(
        executor, idleTimeoutMs, timestampExtractor, new SourceTestMetrics.TestMetricGroup());
  }

  protected void pollAndValidate(
      SourceReader<OutputT, FlinkSourceSplit<KV<Integer, Integer>>> reader,
      List<FlinkSourceSplit<KV<Integer, Integer>>> splits,
      boolean validateReceivedTimestamp)
      throws Exception {
    RecordsValidatingOutput validatingOutput = new RecordsValidatingOutput(splits);
    pollAndValidate(reader, splits, validatingOutput, Integer.MAX_VALUE);
    if (validateReceivedTimestamp) {
      assertTrue(validatingOutput.allTimestampReceived());
    }
  }

  protected void pollAndValidate(
      SourceReader<OutputT, FlinkSourceSplit<KV<Integer, Integer>>> reader,
      List<FlinkSourceSplit<KV<Integer, Integer>>> splits,
      RecordsValidatingOutput validatingOutput,
      int numRecordsToConsume)
      throws Exception {
    reader.addSplits(splits);
    reader.notifyNoMoreSplits();

    do {
      reader.pollNext(validatingOutput);
    } while (!validatingOutput.allRecordsConsumed()
        && validatingOutput.numCollectedRecords() < numRecordsToConsume);
  }

  protected List<FlinkSourceSplit<KV<Integer, Integer>>> createSplits(
      int numSplits, int numRecordsPerSplit, int startingIndex) {
    List<FlinkSourceSplit<KV<Integer, Integer>>> splitList = new ArrayList<>();
    for (int i = startingIndex; i < numSplits; i++) {
      Source<KV<Integer, Integer>> testingSource = createBeamSource(i, numRecordsPerSplit);
      splitList.add(new FlinkSourceSplit<>(i, testingSource));
    }
    return splitList;
  }

  protected <T> List<FlinkSourceSplit<T>> createEmptySplits(int numSplits) {
    return IntStream.range(0, numSplits)
        .mapToObj(i -> new FlinkSourceSplit<>(i, new EmptyUnboundedSource<T>()))
        .collect(Collectors.toList());
  }

  protected void verifyBeamReaderClosed(List<FlinkSourceSplit<KV<Integer, Integer>>> splits) {
    splits.forEach(
        split -> {
          TestSource source = (TestSource) split.getBeamSplitSource();
          assertEquals(
              "Should have only one beam BoundedReader created", 1, source.createdReaders().size());
          assertTrue(
              "The beam BoundedReader should have been closed",
              source.createdReaders().get(0).isClosed());
        });
  }

  protected static SourceReaderContext createSourceReaderContext(
      SourceTestMetrics.TestMetricGroup metricGroup) {
    SourceReaderContext mockContext = Mockito.mock(SourceReaderContext.class);
    when(mockContext.metricGroup()).thenReturn(metricGroup);
    return mockContext;
  }

  // -------------------- protected helper class for fetch result validation ---------------------
  protected class RecordsValidatingOutput implements SourceTestMetrics.ReaderOutputCompat<OutputT> {
    private final List<Source<KV<Integer, Integer>>> sources;
    private final Map<String, TestSourceOutput> sourceOutputs;
    private int numCollectedRecords = 0;

    public RecordsValidatingOutput(List<FlinkSourceSplit<KV<Integer, Integer>>> splits) {
      this.sources = new ArrayList<>();
      this.sourceOutputs = new HashMap<>();
      splits.forEach(split -> sources.add(split.getBeamSplitSource()));
    }

    @Override
    public void collect(OutputT record) {
      KV<Integer, Integer> kv = getKVPairs(record);
      ((TestSource) sources.get(kv.getKey())).validateNextValue(kv.getValue());
      numCollectedRecords++;
    }

    @Override
    public void collect(OutputT record, long timestamp) {
      KV<Integer, Integer> kv = getKVPairs(record);
      TestSource testSource = ((TestSource) sources.get(kv.getKey()));
      testSource.validateNextValue(kv.getValue());
      testSource.validateNextTimestamp(timestamp);
      numCollectedRecords++;
    }

    @Override
    public void emitWatermark(Watermark watermark) {}

    @Override
    public void markIdle() {}

    @Override
    public void markActive() {}

    @Override
    public SourceOutput<OutputT> createOutputForSplit(String splitId) {
      return sourceOutputs.computeIfAbsent(splitId, ignored -> new TestSourceOutput(this));
    }

    @Override
    public void releaseOutputForSplit(String splitId) {}

    public int numCollectedRecords() {
      return numCollectedRecords;
    }

    public boolean allRecordsConsumed() {
      return sources.stream()
          .map(TestSource.class::cast)
          .allMatch(TestSource::isConsumptionCompleted);
    }

    public boolean allTimestampReceived() {
      boolean allTimestampReceived = true;
      for (Source<?> source : sources) {
        allTimestampReceived =
            allTimestampReceived && ((TestSource) source).allTimestampsReceived();
      }
      return allTimestampReceived;
    }

    public Map<String, TestSourceOutput> createdSourceOutputs() {
      return sourceOutputs;
    }
  }

  protected class TestSourceOutput implements SourceTestMetrics.SourceOutputCompat<OutputT> {
    private final ReaderOutput<OutputT> output;
    private @Nullable Watermark watermark;
    private boolean isIdle;

    private TestSourceOutput(RecordsValidatingOutput output) {
      this.output = output;
      this.watermark = null;
      this.isIdle = false;
    }

    @Override
    public void collect(OutputT record) {
      output.collect(record);
    }

    @Override
    public void collect(OutputT record, long timestamp) {
      output.collect(record, timestamp);
    }

    @Override
    public void emitWatermark(Watermark watermark) {
      this.watermark = watermark;
    }

    @Override
    public void markIdle() {
      isIdle = true;
    }

    @Override
    public void markActive() {
      isIdle = false;
    }

    public @Nullable Watermark watermark() {
      return watermark;
    }

    public boolean isIdle() {
      return isIdle;
    }
  }
}
