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
package com.google.cloud.dataflow.sdk.io;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.dataflow.sdk.runners.dataflow.TestCountingSource;
import com.google.cloud.dataflow.sdk.testing.PAssert;
import com.google.cloud.dataflow.sdk.testing.RunnableOnService;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

import org.joda.time.Duration;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Unit tests for {@link BoundedReadFromUnboundedSource}. */
@RunWith(JUnit4.class)
public class BoundedReadFromUnboundedSourceTest {
  private static final int NUM_RECORDS = 100;
  private static List<Integer> finalizeTracker = null;

  @Test
  @Category(RunnableOnService.class)
  public void testNoDedup() throws Exception {
    test(false, false);
  }

  @Test
  @Category(RunnableOnService.class)
  public void testDedup() throws Exception {
    test(true, false);
  }

  @Test
  @Category(RunnableOnService.class)
  public void testTimeBound() throws Exception {
    test(false, true);
  }

  private static class Checker
      implements SerializableFunction<Iterable<KV<Integer, Integer>>, Void> {
    private final boolean dedup;
    private final boolean timeBound;

    Checker(boolean dedup, boolean timeBound) {
      this.dedup = dedup;
      this.timeBound = timeBound;
    }

    @Override
    public Void apply(Iterable<KV<Integer, Integer>> input) {
      List<Integer> values = new ArrayList<>();
      for (KV<Integer, Integer> kv : input) {
        assertEquals(0, (int) kv.getKey());
        values.add(kv.getValue());
      }
      if (timeBound) {
        assertTrue(values.size() > 2);
      } else if (dedup) {
        // Verify that at least some data came through.  The chance of 90% of the input
        // being duplicates is essentially zero.
        assertTrue(values.size() > NUM_RECORDS / 10 && values.size() <= NUM_RECORDS);
      } else {
        assertEquals(NUM_RECORDS, values.size());
      }
      Collections.sort(values);
      for (int i = 0; i < values.size(); i++) {
        assertEquals(i, (int) values.get(i));
      }
      if (finalizeTracker != null) {
        assertThat(finalizeTracker, containsInAnyOrder(values.size() - 1));
      }
      return null;
    }
  }

  private void test(boolean dedup, boolean timeBound) throws Exception {
    Pipeline p = TestPipeline.create();

    if (p.getOptions().getRunner() == DirectPipelineRunner.class) {
      finalizeTracker = new ArrayList<>();
      TestCountingSource.setFinalizeTracker(finalizeTracker);
    }
    TestCountingSource source = new TestCountingSource(Integer.MAX_VALUE).withoutSplitting();
    if (dedup) {
      source = source.withDedup();
    }
    PCollection<KV<Integer, Integer>> output =
        timeBound
        ? p.apply(Read.from(source).withMaxReadTime(Duration.millis(200)))
        : p.apply(Read.from(source).withMaxNumRecords(NUM_RECORDS));

    List<KV<Integer, Integer>> expectedOutput = new ArrayList<>();
    for (int i = 0; i < NUM_RECORDS; i++) {
      expectedOutput.add(KV.of(0, i));
    }

    // Because some of the NUM_RECORDS elements read are dupes, the final output
    // will only have output from 0 to n where n < NUM_RECORDS.
    PAssert.that(output).satisfies(new Checker(dedup, timeBound));

    p.run();
  }
}
