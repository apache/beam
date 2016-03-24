/*
 * Copyright (C) 2015 Google Inc.
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

package com.google.cloud.dataflow.sdk.transforms;

import static org.hamcrest.CoreMatchers.isA;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;

import com.google.cloud.dataflow.sdk.Pipeline.PipelineExecutionException;
import com.google.cloud.dataflow.sdk.testing.RunnableOnService;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Combine.CombineFn;
import com.google.cloud.dataflow.sdk.transforms.Max.MaxIntegerFn;
import com.google.cloud.dataflow.sdk.transforms.display.DisplayData;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.Serializable;

/**
 * Tests for DoFn.
 */
@RunWith(JUnit4.class)
public class DoFnTest implements Serializable {

  @Rule
  public transient ExpectedException thrown = ExpectedException.none();

  @Test
  public void testCreateAggregatorWithCombinerSucceeds() {
    String name = "testAggregator";
    Sum.SumLongFn combiner = new Sum.SumLongFn();

    DoFn<Void, Void> doFn = new NoOpDoFn<>();

    Aggregator<Long, Long> aggregator = doFn.createAggregator(name, combiner);

    assertEquals(name, aggregator.getName());
    assertEquals(combiner, aggregator.getCombineFn());
  }

  @Test
  public void testCreateAggregatorWithNullNameThrowsException() {
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("name cannot be null");

    DoFn<Void, Void> doFn = new NoOpDoFn<>();

    doFn.createAggregator(null, new Sum.SumLongFn());
  }

  @Test
  public void testCreateAggregatorWithNullCombineFnThrowsException() {
    CombineFn<Object, Object, Object> combiner = null;

    thrown.expect(NullPointerException.class);
    thrown.expectMessage("combiner cannot be null");

    DoFn<Void, Void> doFn = new NoOpDoFn<>();

    doFn.createAggregator("testAggregator", combiner);
  }

  @Test
  public void testCreateAggregatorWithNullSerializableFnThrowsException() {
    SerializableFunction<Iterable<Object>, Object> combiner = null;

    thrown.expect(NullPointerException.class);
    thrown.expectMessage("combiner cannot be null");

    DoFn<Void, Void> doFn = new NoOpDoFn<>();

    doFn.createAggregator("testAggregator", combiner);
  }

  @Test
  public void testCreateAggregatorWithSameNameThrowsException() {
    String name = "testAggregator";
    CombineFn<Double, ?, Double> combiner = new Max.MaxDoubleFn();

    DoFn<Void, Void> doFn = new NoOpDoFn<>();

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

    DoFn<Void, Void> doFn = new NoOpDoFn<>();

    Aggregator<Double, Double> aggregatorOne =
        doFn.createAggregator(nameOne, combiner);
    Aggregator<Double, Double> aggregatorTwo =
        doFn.createAggregator(nameTwo, combiner);

    assertNotEquals(aggregatorOne, aggregatorTwo);
  }

  @Test
  @Category(RunnableOnService.class)
  public void testCreateAggregatorInStartBundleThrows() {
    TestPipeline p = createTestPipeline(new DoFn<String, String>() {
      @Override
      public void startBundle(DoFn<String, String>.Context c) throws Exception {
        createAggregator("anyAggregate", new MaxIntegerFn());
      }

      @Override
      public void processElement(DoFn<String, String>.ProcessContext c) throws Exception {}
    });

    thrown.expect(PipelineExecutionException.class);
    thrown.expectCause(isA(IllegalStateException.class));

    p.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testCreateAggregatorInProcessElementThrows() {
    TestPipeline p = createTestPipeline(new DoFn<String, String>() {
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
  @Category(RunnableOnService.class)
  public void testCreateAggregatorInFinishBundleThrows() {
    TestPipeline p = createTestPipeline(new DoFn<String, String>() {
      @Override
      public void finishBundle(DoFn<String, String>.Context c) throws Exception {
        createAggregator("anyAggregate", new MaxIntegerFn());
      }

      @Override
      public void processElement(DoFn<String, String>.ProcessContext c) throws Exception {}
    });

    thrown.expect(PipelineExecutionException.class);
    thrown.expectCause(isA(IllegalStateException.class));

    p.run();
  }

  /**
   * Initialize a test pipeline with the specified {@link DoFn}.
   */
  private <InputT, OutputT> TestPipeline createTestPipeline(DoFn<InputT, OutputT> fn) {
    TestPipeline pipeline = TestPipeline.create();
    pipeline.apply(Create.of((InputT) null))
     .apply(ParDo.of(fn));

    return pipeline;
  }

  @Test
  public void testPopulateDisplayDataDefaultBehavior() {
    DoFn<String, String> usesDefault =
        new DoFn<String, String>() {
          @Override
          public void processElement(ProcessContext c) throws Exception {}
        };

    DisplayData data = DisplayData.from(usesDefault);
    assertThat(data.items(), empty());
  }
}
