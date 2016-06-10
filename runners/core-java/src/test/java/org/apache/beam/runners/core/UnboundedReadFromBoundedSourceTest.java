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

import static org.junit.Assert.assertEquals;

import org.apache.beam.runners.core.UnboundedReadFromBoundedSource.BoundedToUnboundedSourceAdapter;
import org.apache.beam.runners.core.UnboundedReadFromBoundedSource.BoundedToUnboundedSourceAdapter.Checkpoint;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.CountingSource;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.RunnableOnService;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Max;
import org.apache.beam.sdk.transforms.Min;
import org.apache.beam.sdk.transforms.RemoveDuplicates;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.PCollection;

import com.google.api.client.util.Lists;
import com.google.common.collect.Sets;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.List;
import java.util.Set;

/**
 * Unit tests for {@link UnboundedReadFromBoundedSource}.
 */
@RunWith(JUnit4.class)
public class UnboundedReadFromBoundedSourceTest {

  @Test
  @Category(RunnableOnService.class)
  public void testBoundedToUnboundedSourceAdapter() throws Exception {
    long numElements = 100;
    BoundedSource<Long> boundedSource = CountingSource.upTo(numElements);
    UnboundedSource<Long, Checkpoint<Long>> unboundedSource =
        new BoundedToUnboundedSourceAdapter<>(boundedSource);

    Pipeline p = TestPipeline.create();

    PCollection<Long> output =
        p.apply(Read.from(unboundedSource).withMaxNumRecords(numElements));

    // Count == numElements
    PAssert
      .thatSingleton(output.apply("Count", Count.<Long>globally()))
      .isEqualTo(numElements);
    // Unique count == numElements
    PAssert
      .thatSingleton(output.apply(RemoveDuplicates.<Long>create())
                          .apply("UniqueCount", Count.<Long>globally()))
      .isEqualTo(numElements);
    // Min == 0
    PAssert
      .thatSingleton(output.apply("Min", Min.<Long>globally()))
      .isEqualTo(0L);
    // Max == numElements-1
    PAssert
      .thatSingleton(output.apply("Max", Max.<Long>globally()))
      .isEqualTo(numElements - 1);
    p.run();
  }

  @Test
  public void testBoundedToUnboundedSourceAdapterCheckpoint() throws Exception {
    long numElements = 100;
    BoundedSource<Long> boundedSource = CountingSource.upTo(numElements);
    BoundedToUnboundedSourceAdapter<Long> unboundedSource =
        new BoundedToUnboundedSourceAdapter<>(boundedSource);

    PipelineOptions options = PipelineOptionsFactory.create();
    BoundedToUnboundedSourceAdapter<Long>.Reader reader =
        unboundedSource.createReader(options, null);

    Set<Long> expected = Sets.newHashSet();
    for (long i = 0; i < numElements; ++i) {
      expected.add(i);
    }

    List<Long> actual = Lists.newArrayList();
    for (boolean hasNext = reader.start(); hasNext;) {
      actual.add(reader.getCurrent());
      // checkpoint every 9 elements
      if (actual.size() % 9 == 0) {
        Checkpoint<Long> checkpoint = reader.getCheckpointMark();
        Coder<Checkpoint<Long>> checkpointCoder = unboundedSource.getCheckpointMarkCoder();
        Checkpoint<Long> decodedCheckpoint = CoderUtils.decodeFromByteArray(
            checkpointCoder,
            CoderUtils.encodeToByteArray(checkpointCoder, checkpoint));
        reader.close();
        checkpoint.finalizeCheckpoint();

        BoundedToUnboundedSourceAdapter<Long>.Reader restarted =
            unboundedSource.createReader(options, decodedCheckpoint);
        reader = restarted;
        hasNext = reader.start();
      } else {
        hasNext = reader.advance();
      }
    }
    assertEquals(numElements, actual.size());
    assertEquals(expected, Sets.newHashSet(actual));
  }
}
