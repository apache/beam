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
package org.apache.beam.sdk.io;

import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.includesDisplayDataFor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.runners.dataflow.TestCountingSource;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link BoundedReadFromUnboundedSource}. */
@RunWith(JUnit4.class)
public class BoundedReadFromUnboundedSourceTest implements Serializable{
  private static final int NUM_RECORDS = 100;

  @Rule
  public transient TestPipeline p = TestPipeline.create();

  @Test
  @Category(ValidatesRunner.class)
  public void testNoDedup() throws Exception {
    test(false, false);
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testDedup() throws Exception {
    test(true, false);
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testTimeBound() throws Exception {
    test(false, true);
  }

  @Test
  public void testForwardsDisplayData() {
    TestCountingSource src = new TestCountingSource(1234) {
      @Override
      public void populateDisplayData(DisplayData.Builder builder) {
        builder.add(DisplayData.item("foo", "bar"));
      }
    };

    BoundedReadFromUnboundedSource<KV<Integer, Integer>> read = Read.from(src).withMaxNumRecords(5);
    assertThat(DisplayData.from(read), includesDisplayDataFor("source", src));
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
        assertTrue(values.size() >= 1);
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
      return null;
    }
  }

  private void test(boolean dedup, boolean timeBound) throws Exception {

    TestCountingSource source = new TestCountingSource(Integer.MAX_VALUE).withoutSplitting();
    if (dedup) {
      source = source.withDedup();
    }
    PCollection<KV<Integer, Integer>> output =
        timeBound
        ? p.apply(Read.from(source).withMaxReadTime(Duration.millis(200)))
        : p.apply(Read.from(source).withMaxNumRecords(NUM_RECORDS));

    // Because some of the NUM_RECORDS elements read are dupes, the final output
    // will only have output from 0 to n where n < NUM_RECORDS.
    PAssert.that(output).satisfies(new Checker(dedup, timeBound));

    p.run();
  }
}
