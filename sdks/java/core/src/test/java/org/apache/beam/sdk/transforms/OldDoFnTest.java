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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.isA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.Pipeline.PipelineExecutionException;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.runners.AggregatorValues;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.Max.MaxIntegerFn;
import org.apache.beam.sdk.transforms.Sum.SumIntegerFn;
import org.apache.beam.sdk.transforms.display.DisplayData;

import com.google.common.collect.ImmutableMap;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.Serializable;
import java.util.Map;

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
    Sum.SumLongFn combiner = new Sum.SumLongFn();

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

    doFn.createAggregator(null, new Sum.SumLongFn());
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
    CombineFn<Double, ?, Double> combiner = new Max.MaxDoubleFn();

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
    CombineFn<Double, ?, Double> combiner = new Max.MaxDoubleFn();

    OldDoFn<Void, Void> doFn = new NoOpOldDoFn<>();

    Aggregator<Double, Double> aggregatorOne =
        doFn.createAggregator(nameOne, combiner);
    Aggregator<Double, Double> aggregatorTwo =
        doFn.createAggregator(nameTwo, combiner);

    assertNotEquals(aggregatorOne, aggregatorTwo);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testCreateAggregatorInStartBundleThrows() {
    TestPipeline p = createTestPipeline(new OldDoFn<String, String>() {
      @Override
      public void startBundle(OldDoFn<String, String>.Context c) throws Exception {
        createAggregator("anyAggregate", new MaxIntegerFn());
      }

      @Override
      public void processElement(OldDoFn<String, String>.ProcessContext c) throws Exception {}
    });

    thrown.expect(PipelineExecutionException.class);
    thrown.expectCause(isA(IllegalStateException.class));

    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testCreateAggregatorInProcessElementThrows() {
    TestPipeline p = createTestPipeline(new OldDoFn<String, String>() {
      @Override
      public void processElement(ProcessContext c) throws Exception {
        createAggregator("anyAggregate", new MaxIntegerFn());
      }
    });

    thrown.expect(PipelineExecutionException.class);
    thrown.expectCause(isA(IllegalStateException.class));

    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testCreateAggregatorInFinishBundleThrows() {
    TestPipeline p = createTestPipeline(new OldDoFn<String, String>() {
      @Override
      public void finishBundle(OldDoFn<String, String>.Context c) throws Exception {
        createAggregator("anyAggregate", new MaxIntegerFn());
      }

      @Override
      public void processElement(OldDoFn<String, String>.ProcessContext c) throws Exception {}
    });

    thrown.expect(PipelineExecutionException.class);
    thrown.expectCause(isA(IllegalStateException.class));

    p.run();
  }

  /**
   * Initialize a test pipeline with the specified {@link OldDoFn}.
   */
  private <InputT, OutputT> TestPipeline createTestPipeline(OldDoFn<InputT, OutputT> fn) {
    TestPipeline pipeline = TestPipeline.create();
    pipeline.apply(Create.of((InputT) null))
     .apply(ParDo.of(fn));

    return pipeline;
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

  @Test
  @Category(NeedsRunner.class)
  public void testAggregators() throws Exception {
    Pipeline pipeline = TestPipeline.create();

    CountOddsFn countOdds = new CountOddsFn();
    pipeline
        .apply(Create.of(1, 3, 5, 7, 2, 4, 6, 8, 10, 12, 14, 20, 42, 68, 100))
        .apply(ParDo.of(countOdds));
    PipelineResult result = pipeline.run();

    AggregatorValues<Integer> values = result.getAggregatorValues(countOdds.aggregator);
    assertThat(values.getValuesAtSteps(),
        equalTo((Map<String, Integer>) ImmutableMap.<String, Integer>of("ParDo(CountOdds)", 4)));
  }

  private static class CountOddsFn extends OldDoFn<Integer, Void> {
    @Override
    public void processElement(ProcessContext c) throws Exception {
      if (c.element() % 2 == 1) {
        aggregator.addValue(1);
      }
    }

    Aggregator<Integer, Integer> aggregator =
        createAggregator("odds", new SumIntegerFn());
  }
}
