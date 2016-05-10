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
package org.apache.beam.runners.flink.streaming;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.beam.runners.flink.translation.wrappers.streaming.io.UnboundedSourceWrapper;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Tests for {@link UnboundedSourceWrapper}.
 */
public class UnboundedSourceWrapperTest {

  /**
   * Creates a {@link UnboundedSourceWrapper} that has exactly one reader per source, since we
   * specify a parallelism of 1 and also at runtime tell the source that it has 1 parallel subtask.
   */
  @Test
  public void testWithOneReader() throws Exception {
    final int NUM_ELEMENTS = 20;
    final Object checkpointLock = new Object();
    PipelineOptions options = PipelineOptionsFactory.create();

    // this source will emit exactly NUM_ELEMENTS across all parallel readers,
    // afterwards it will stall. We check whether we also receive NUM_ELEMENTS
    // elements later.
    TestCountingSource source = new TestCountingSource(NUM_ELEMENTS);
    UnboundedSourceWrapper<KV<Integer, Integer>, TestCountingSource.CounterMark> flinkWrapper =
        new UnboundedSourceWrapper<>(options, source, 1);

    assertEquals(1, flinkWrapper.getSplitSources().size());

    StreamSource<
        WindowedValue<KV<Integer, Integer>>,
        UnboundedSourceWrapper<
            KV<Integer, Integer>,
            TestCountingSource.CounterMark>> sourceOperator = new StreamSource<>(flinkWrapper);

    setupSourceOperator(sourceOperator);


    try {
      sourceOperator.run(checkpointLock,
          new Output<StreamRecord<WindowedValue<KV<Integer, Integer>>>>() {
            private int count = 0;

            @Override
            public void emitWatermark(Watermark watermark) {
            }

            @Override
            public void collect(
                StreamRecord<WindowedValue<KV<Integer,Integer>>> windowedValueStreamRecord) {

              count++;
              if (count >= NUM_ELEMENTS) {
                throw new SuccessException();
              }
            }

            @Override
            public void close() {

            }
          });
    } catch (SuccessException e) {
      // success
    } catch (Exception e) {
      fail("We caught " + e);
    }
  }

  /**
   * Creates a {@link UnboundedSourceWrapper} that has multiple readers per source, since we
   * specify a parallelism higher than 1 and at runtime tell the source that it has 1 parallel
   * this means that one source will manage multiple readers.
   */
  @Test
  public void testWithMultipleReaders() throws Exception {
    final int NUM_ELEMENTS = 20;
    final Object checkpointLock = new Object();
    PipelineOptions options = PipelineOptionsFactory.create();

    // this source will emit exactly NUM_ELEMENTS across all parallel readers,
    // afterwards it will stall. We check whether we also receive NUM_ELEMENTS
    // elements later.
    TestCountingSource source = new TestCountingSource(NUM_ELEMENTS);
    UnboundedSourceWrapper<KV<Integer, Integer>, TestCountingSource.CounterMark> flinkWrapper =
        new UnboundedSourceWrapper<>(options, source, 4);

    assertEquals(4, flinkWrapper.getSplitSources().size());

    StreamSource<WindowedValue<
        KV<Integer, Integer>>,
        UnboundedSourceWrapper<
            KV<Integer, Integer>,
            TestCountingSource.CounterMark>> sourceOperator = new StreamSource<>(flinkWrapper);

    setupSourceOperator(sourceOperator);


    try {
      sourceOperator.run(checkpointLock,
          new Output<StreamRecord<WindowedValue<KV<Integer, Integer>>>>() {
            private int count = 0;

            @Override
            public void emitWatermark(Watermark watermark) {
            }

            @Override
            public void collect(
                StreamRecord<WindowedValue<KV<Integer,Integer>>> windowedValueStreamRecord) {

              count++;
              if (count >= NUM_ELEMENTS) {
                throw new SuccessException();
              }
            }

            @Override
            public void close() {

            }
          });
    } catch (SuccessException e) {
      // success
      return;
    }
    fail("Read terminated without producing expected number of outputs");
  }

  /**
   * Verify that snapshot/restore work as expected. We bring up a source and cancel
   * after seeing a certain number of elements. Then we snapshot that source,
   * bring up a completely new source that we restore from the snapshot and verify
   * that we see all expected elements in the end.
   */
  @Test
  public void testRestore() throws Exception {
    final int NUM_ELEMENTS = 20;
    final Object checkpointLock = new Object();
    PipelineOptions options = PipelineOptionsFactory.create();

    // this source will emit exactly NUM_ELEMENTS across all parallel readers,
    // afterwards it will stall. We check whether we also receive NUM_ELEMENTS
    // elements later.
    TestCountingSource source = new TestCountingSource(NUM_ELEMENTS);
    UnboundedSourceWrapper<KV<Integer, Integer>, TestCountingSource.CounterMark> flinkWrapper =
        new UnboundedSourceWrapper<>(options, source, 1);

    assertEquals(1, flinkWrapper.getSplitSources().size());

    StreamSource<
        WindowedValue<KV<Integer, Integer>>,
        UnboundedSourceWrapper<
            KV<Integer, Integer>,
            TestCountingSource.CounterMark>> sourceOperator = new StreamSource<>(flinkWrapper);

    setupSourceOperator(sourceOperator);

    final Set<KV<Integer, Integer>> emittedElements = new HashSet<>();

    boolean readFirstBatchOfElements = false;

    try {
      sourceOperator.run(checkpointLock,
          new Output<StreamRecord<WindowedValue<KV<Integer, Integer>>>>() {
            private int count = 0;

            @Override
            public void emitWatermark(Watermark watermark) {
            }

            @Override
            public void collect(
                StreamRecord<WindowedValue<KV<Integer,Integer>>> windowedValueStreamRecord) {

              emittedElements.add(windowedValueStreamRecord.getValue().getValue());
              count++;
              if (count >= NUM_ELEMENTS / 2) {
                throw new SuccessException();
              }
            }

            @Override
            public void close() {

            }
          });
    } catch (SuccessException e) {
      // success
      readFirstBatchOfElements = true;
    }

    assertTrue("Did not successfully read first batch of elements.", readFirstBatchOfElements);

    // draw a snapshot
    byte[] snapshot = flinkWrapper.snapshotState(0, 0);

    // create a completely new source but restore from the snapshot
    TestCountingSource restoredSource = new TestCountingSource(NUM_ELEMENTS);
    UnboundedSourceWrapper<
        KV<Integer, Integer>, TestCountingSource.CounterMark> restoredFlinkWrapper =
        new UnboundedSourceWrapper<>(options, restoredSource, 1);

    assertEquals(1, restoredFlinkWrapper.getSplitSources().size());

    StreamSource<
        WindowedValue<KV<Integer, Integer>>,
        UnboundedSourceWrapper<
            KV<Integer, Integer>,
            TestCountingSource.CounterMark>> restoredSourceOperator =
        new StreamSource<>(restoredFlinkWrapper);

    setupSourceOperator(restoredSourceOperator);

    // restore snapshot
    restoredFlinkWrapper.restoreState(snapshot);

    boolean readSecondBatchOfElements = false;

    // run again and verify that we see the other elements
    try {
      restoredSourceOperator.run(checkpointLock,
          new Output<StreamRecord<WindowedValue<KV<Integer, Integer>>>>() {
            private int count = 0;

            @Override
            public void emitWatermark(Watermark watermark) {
            }

            @Override
            public void collect(
                StreamRecord<WindowedValue<KV<Integer,Integer>>> windowedValueStreamRecord) {
              emittedElements.add(windowedValueStreamRecord.getValue().getValue());
              count++;
              if (count >= NUM_ELEMENTS / 2) {
                throw new SuccessException();
              }
            }

            @Override
            public void close() {

            }
          });
    } catch (SuccessException e) {
      // success
      readSecondBatchOfElements = true;
    }

    assertTrue("Did not successfully read second batch of elements.", readSecondBatchOfElements);

    // verify that we saw all NUM_ELEMENTS elements
    assertTrue(emittedElements.size() == NUM_ELEMENTS);
  }

  @SuppressWarnings("unchecked")
  private static <T> void setupSourceOperator(StreamSource<T, ?> operator) {
    ExecutionConfig executionConfig = new ExecutionConfig();
    StreamConfig cfg = new StreamConfig(new Configuration());

    cfg.setTimeCharacteristic(TimeCharacteristic.EventTime);

    Environment env = new DummyEnvironment("MockTwoInputTask", 1, 0);

    StreamTask<?, ?> mockTask = mock(StreamTask.class);
    when(mockTask.getName()).thenReturn("Mock Task");
    when(mockTask.getCheckpointLock()).thenReturn(new Object());
    when(mockTask.getConfiguration()).thenReturn(cfg);
    when(mockTask.getEnvironment()).thenReturn(env);
    when(mockTask.getExecutionConfig()).thenReturn(executionConfig);
    when(mockTask.getAccumulatorMap()).thenReturn(Collections.<String, Accumulator<?, ?>>emptyMap());

    operator.setup(mockTask, cfg, (Output< StreamRecord<T>>) mock(Output.class));
  }

  /**
   * A special {@link RuntimeException} that we throw to signal that the test was successful.
   */
  private static class SuccessException extends RuntimeException {}
}
