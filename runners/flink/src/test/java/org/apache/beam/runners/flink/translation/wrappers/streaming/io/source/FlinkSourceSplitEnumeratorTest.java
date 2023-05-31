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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.List;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.TestBoundedCountingSource;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.TestCountingSource;
import org.apache.beam.sdk.io.Source;
import org.apache.beam.sdk.values.KV;
import org.apache.flink.connector.testutils.source.reader.TestingSplitEnumeratorContext;
import org.junit.Test;

/** Unit tests for {@link FlinkSourceSplitEnumerator}. */
public class FlinkSourceSplitEnumeratorTest {

  @Test
  public void testAssignSplitsWithBoundedSource() throws IOException {
    final int numSubtasks = 2;
    final int numSplits = 10;
    final int totalNumRecords = 10;
    TestingSplitEnumeratorContext<FlinkSourceSplit<KV<Integer, Integer>>> testContext =
        new TestingSplitEnumeratorContext<>(numSubtasks);
    TestBoundedCountingSource testSource =
        new TestBoundedCountingSource(numSplits, totalNumRecords);

    assignSplits(testContext, testSource, numSplits);
    assertEquals(numSubtasks, testContext.getSplitAssignments().size());

    testContext
        .getSplitAssignments()
        .forEach(
            (subtaskId, state) -> {
              int expectedNumSplitsPerSubtask = numSplits / numSubtasks;
              assertEquals(
                  "Each subtask should have " + expectedNumSplitsPerSubtask + " assigned splits",
                  expectedNumSplitsPerSubtask,
                  state.getAssignedSplits().size());
              assertTrue(
                  "Each subtask should have received NoMoreSplits",
                  state.hasReceivedNoMoreSplitsSignal());
              state
                  .getAssignedSplits()
                  .forEach(
                      split -> {
                        TestBoundedCountingSource source =
                            (TestBoundedCountingSource) split.getBeamSplitSource();
                        try {
                          int expectedSplitSize = totalNumRecords / numSplits;
                          assertEquals(
                              expectedSplitSize,
                              source.getEstimatedSizeBytes(FlinkPipelineOptions.defaults()));
                        } catch (Exception e) {
                          fail("Received exception" + e);
                        }
                      });
            });
  }

  @Test
  public void testAssignSplitsWithUnboundedSource() throws IOException {
    final int numSplits = 10;
    final int numSubtasks = 5;
    final int numRecordsPerSplit = 10;
    TestingSplitEnumeratorContext<FlinkSourceSplit<KV<Integer, Integer>>> testContext =
        new TestingSplitEnumeratorContext<>(numSubtasks);
    TestCountingSource testSource = new TestCountingSource(numRecordsPerSplit);

    assignSplits(testContext, testSource, numSplits);

    testContext
        .getSplitAssignments()
        .forEach(
            (subtaskId, state) -> {
              int expectedNumSplitsPerSubtask = numSplits / numSubtasks;
              assertEquals(
                  "Each subtask should have " + expectedNumSplitsPerSubtask + " assigned splits",
                  expectedNumSplitsPerSubtask,
                  state.getAssignedSplits().size());
              assertTrue(
                  "Each subtask should have received NoMoreSplits",
                  state.hasReceivedNoMoreSplitsSignal());
            });
  }

  @Test
  public void testAddSplitsBack() throws IOException {
    final int numSubtasks = 2;
    final int numSplits = 10;
    final int totalNumRecords = 10;
    TestingSplitEnumeratorContext<FlinkSourceSplit<KV<Integer, Integer>>> testContext =
        new TestingSplitEnumeratorContext<>(numSubtasks);
    TestBoundedCountingSource testSource =
        new TestBoundedCountingSource(numSplits, totalNumRecords);
    try (FlinkSourceSplitEnumerator<KV<Integer, Integer>> splitEnumerator =
        new FlinkSourceSplitEnumerator<>(
            testContext, testSource, FlinkPipelineOptions.defaults(), numSplits)) {
      splitEnumerator.start();
      testContext.registerReader(0, "0");
      splitEnumerator.addReader(0);
      testContext.getExecutorService().triggerAll();

      List<FlinkSourceSplit<KV<Integer, Integer>>> splitsForReader =
          testContext.getSplitAssignments().get(0).getAssignedSplits();
      assertEquals(numSplits / numSubtasks, splitsForReader.size());

      splitEnumerator.addSplitsBack(splitsForReader, 0);
      splitEnumerator.addReader(0);
      assertEquals(2 * numSplits / numSubtasks, splitsForReader.size());
    }
  }

  private void assignSplits(
      TestingSplitEnumeratorContext<FlinkSourceSplit<KV<Integer, Integer>>> context,
      Source<KV<Integer, Integer>> source,
      int numSplits)
      throws IOException {
    try (FlinkSourceSplitEnumerator<KV<Integer, Integer>> splitEnumerator =
        new FlinkSourceSplitEnumerator<>(
            context, source, FlinkPipelineOptions.defaults(), numSplits)) {
      splitEnumerator.start();
      // Add a reader before splitting the beam source.
      context.registerReader(0, "0");
      splitEnumerator.addReader(0);
      context.getExecutorService().triggerAll();
      context.registerReader(1, "1");
      // Add another reader after splitting the beam source.
      splitEnumerator.addReader(1);
    }
  }
}
