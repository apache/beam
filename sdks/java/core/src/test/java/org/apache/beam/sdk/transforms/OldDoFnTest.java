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
package org.apache.beam.sdk.transforms;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.isA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;

import java.io.Serializable;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for OldDoFn.
 */
@RunWith(JUnit4.class)
public class OldDoFnTest implements Serializable {

  @Rule
  public transient ExpectedException thrown = ExpectedException.none();

  @Test
  public void testCreateAggregatorWithCombinerSucceeds() {
    String name = "testAggregator";
    Combine.BinaryCombineLongFn combiner = Sum.ofLongs();

    OldDoFn<Void, Void> doFn = new NoOpOldDoFn<>();

    Aggregator<Long, Long> aggregator = doFn.createAggregator(name, combiner);

    assertEquals(name, aggregator.getName());
    assertEquals(combiner, aggregator.getCombineFn());
  }

  @Test
  public void testCreateAggregatorWithNullNameThrowsException() {
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("name cannot be null");

    OldDoFn<Void, Void> doFn = new NoOpOldDoFn<>();

    doFn.createAggregator(null, Sum.ofLongs());
  }

  @Test
  public void testCreateAggregatorWithNullCombineFnThrowsException() {
    CombineFn<Object, Object, Object> combiner = null;

    thrown.expect(NullPointerException.class);
    thrown.expectMessage("combiner cannot be null");

    OldDoFn<Void, Void> doFn = new NoOpOldDoFn<>();

    doFn.createAggregator("testAggregator", combiner);
  }

  @Test
  public void testCreateAggregatorWithNullSerializableFnThrowsException() {
    SerializableFunction<Iterable<Object>, Object> combiner = null;

    thrown.expect(NullPointerException.class);
    thrown.expectMessage("combiner cannot be null");

    OldDoFn<Void, Void> doFn = new NoOpOldDoFn<>();

    doFn.createAggregator("testAggregator", combiner);
  }

  @Test
  public void testCreateAggregatorWithSameNameThrowsException() {
    String name = "testAggregator";
    CombineFn<Double, ?, Double> combiner = Max.ofDoubles();

    OldDoFn<Void, Void> doFn = new NoOpOldDoFn<>();

    doFn.createAggregator(name, combiner);

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Cannot create");
    thrown.expectMessage(name);
    thrown.expectMessage("already exists");

    doFn.createAggregator(name, combiner);
  }

  @Test
  public void testCreateAggregatorsWithDifferentNamesSucceeds() {
    String nameOne = "testAggregator";
    String nameTwo = "aggregatorPrime";
    CombineFn<Double, ?, Double> combiner = Max.ofDoubles();

    OldDoFn<Void, Void> doFn = new NoOpOldDoFn<>();

    Aggregator<Double, Double> aggregatorOne =
        doFn.createAggregator(nameOne, combiner);
    Aggregator<Double, Double> aggregatorTwo =
        doFn.createAggregator(nameTwo, combiner);

    assertNotEquals(aggregatorOne, aggregatorTwo);
  }

  @Test
  public void testCreateAggregatorThrowsWhenAggregatorsAreFinal() throws Exception {
    OldDoFn<String, String> fn = new OldDoFn<String, String>() {
      @Override
      public void processElement(ProcessContext c) throws Exception { }
    };
    OldDoFn<String, String>.Context context = createContext(fn);
    context.setupDelegateAggregators();

    thrown.expect(isA(IllegalStateException.class));
    fn.createAggregator("anyAggregate", Max.ofIntegers());
  }

  private OldDoFn<String, String>.Context createContext(OldDoFn<String, String> fn) {
    return fn.new Context() {
      @Override
      public PipelineOptions getPipelineOptions() {
        throw new UnsupportedOperationException();
      }

      @Override
      public void output(String output) {
        throw new UnsupportedOperationException();
      }

      @Override
      public void outputWithTimestamp(String output, Instant timestamp) {
        throw new UnsupportedOperationException();
      }

      @Override
      public <T> void sideOutput(TupleTag<T> tag, T output) {
        throw new UnsupportedOperationException();
      }

      @Override
      public <T> void sideOutputWithTimestamp(TupleTag<T> tag, T output, Instant timestamp) {
        throw new UnsupportedOperationException();
      }

      @Override
      public <AggInputT, AggOutputT>
      Aggregator<AggInputT, AggOutputT> createAggregatorInternal(
              String name, CombineFn<AggInputT, ?, AggOutputT> combiner) {
        throw new UnsupportedOperationException();
      }
    };
  }

  @Test
  public void testPopulateDisplayDataDefaultBehavior() {
    OldDoFn<String, String> usesDefault =
        new OldDoFn<String, String>() {
          @Override
          public void processElement(ProcessContext c) throws Exception {}
        };

    DisplayData data = DisplayData.from(usesDefault);
    assertThat(data.items(), empty());
  }
}
