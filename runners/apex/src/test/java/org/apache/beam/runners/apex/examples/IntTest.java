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

package org.apache.beam.runners.apex.examples;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.beam.runners.apex.ApexPipelineOptions;
import org.apache.beam.runners.apex.TestApexRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.runners.dataflow.TestCountingSource;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * For debugging only.
 */
@Ignore
@RunWith(JUnit4.class)
public class IntTest implements java.io.Serializable
{

  @Test
  public void test()
  {
    ApexPipelineOptions options = PipelineOptionsFactory.as(ApexPipelineOptions.class);
    options.setTupleTracingEnabled(true);
    options.setRunner(TestApexRunner.class);
    Pipeline p = Pipeline.create(options);
boolean timeBound = false;


  TestCountingSource source = new TestCountingSource(Integer.MAX_VALUE).withoutSplitting();
//List<KV<Integer,Integer>> values = Lists.newArrayList(
//    KV.of(0, 99),KV.of(0, 99),KV.of(0, 98));

//UnboundedSource<KV<Integer,Integer>, ?> source = new ValuesSource<>(values,
//   KvCoder.of(VarIntCoder.of(), VarIntCoder.of()));

  if (true) {
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
    PAssert.that(output).satisfies(new Checker(true, timeBound));


    p.run();
    return;
  }

  private static final int NUM_RECORDS = 10;
  private static class Checker implements SerializableFunction<Iterable<KV<Integer, Integer>>, Void>
  {
    private final boolean dedup;
    private final boolean timeBound;

    Checker(boolean dedup, boolean timeBound)
    {
      this.dedup = dedup;
      this.timeBound = timeBound;
    }

    @Override
    public Void apply(Iterable<KV<Integer, Integer>> input)
    {
      List<Integer> values = new ArrayList<>();
      for (KV<Integer, Integer> kv : input) {
        assertEquals(0, (int)kv.getKey());
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
        assertEquals(i, (int)values.get(i));
      }
      //if (finalizeTracker != null) {
      //  assertThat(finalizeTracker, containsInAnyOrder(values.size() - 1));
      //}
      return null;
    }
  }


}
