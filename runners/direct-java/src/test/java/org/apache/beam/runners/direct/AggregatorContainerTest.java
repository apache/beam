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
package org.apache.beam.runners.direct;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.beam.runners.core.ExecutionContext.StepContext;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.Sum;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Tests for {@link AggregatorContainer}.
 */
@RunWith(JUnit4.class)
public class AggregatorContainerTest {

  @Rule
  public final ExpectedException thrown = ExpectedException.none();
  private final AggregatorContainer container = AggregatorContainer.create();

  private static final String STEP_NAME = "step";
  private final Class<?> fn = getClass();

  @Mock
  private StepContext stepContext;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    when(stepContext.getStepName()).thenReturn(STEP_NAME);
  }

  @Test
  public void addsAggregatorsOnCommit() {
    AggregatorContainer.Mutator mutator = container.createMutator();
    mutator.createAggregatorForDoFn(fn, stepContext, "sum_int", Sum.ofIntegers()).addValue(5);
    mutator.commit();

    assertThat((Integer) container.getAggregate(STEP_NAME, "sum_int"), equalTo(5));

    mutator = container.createMutator();
    mutator.createAggregatorForDoFn(fn, stepContext, "sum_int", Sum.ofIntegers()).addValue(8);

    assertThat("Shouldn't update value until commit",
        (Integer) container.getAggregate(STEP_NAME, "sum_int"), equalTo(5));
    mutator.commit();
    assertThat((Integer) container.getAggregate(STEP_NAME, "sum_int"), equalTo(13));
  }

  @Test
  public void failToCreateAfterCommit() {
    AggregatorContainer.Mutator mutator = container.createMutator();
    mutator.commit();

    thrown.expect(IllegalStateException.class);
    mutator.createAggregatorForDoFn(fn, stepContext, "sum_int", Sum.ofIntegers()).addValue(5);
  }

  @Test
  public void failToAddValueAfterCommit() {
    AggregatorContainer.Mutator mutator = container.createMutator();
    Aggregator<Integer, ?> aggregator =
        mutator.createAggregatorForDoFn(fn, stepContext, "sum_int", Sum.ofIntegers());
    mutator.commit();

    thrown.expect(IllegalStateException.class);
    aggregator.addValue(5);
  }

  @Test
  public void failToAddValueAfterCommitWithPrevious() {
    AggregatorContainer.Mutator mutator = container.createMutator();
    mutator.createAggregatorForDoFn(
        fn, stepContext, "sum_int", Sum.ofIntegers()).addValue(5);
    mutator.commit();

    mutator = container.createMutator();
    Aggregator<Integer, ?> aggregator = mutator.createAggregatorForDoFn(
        fn, stepContext, "sum_int", Sum.ofIntegers());
    mutator.commit();

    thrown.expect(IllegalStateException.class);
    aggregator.addValue(5);
  }

  @Test
  public void concurrentWrites() throws InterruptedException {
    ExecutorService executor = Executors.newFixedThreadPool(20);
    int sum = 0;
    for (int i = 0; i < 100; i++) {
      sum += i;
      final int value = i;
      final AggregatorContainer.Mutator mutator = container.createMutator();
      executor.submit(new Runnable() {
        @Override
        public void run() {
          mutator.createAggregatorForDoFn(
              fn, stepContext, "sum_int", Sum.ofIntegers()).addValue(value);
          mutator.commit();
        }
      });
    }
    executor.shutdown();
    assertThat("Expected all threads to complete after 5 seconds",
        executor.awaitTermination(5, TimeUnit.SECONDS), equalTo(true));

    assertThat((Integer) container.getAggregate(STEP_NAME, "sum_int"), equalTo(sum));
  }
}
