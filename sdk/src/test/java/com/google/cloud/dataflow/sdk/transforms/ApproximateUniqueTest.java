/*
 * Copyright (C) 2014 Google Inc.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.TestUtils;
import com.google.cloud.dataflow.sdk.coders.BigEndianIntegerCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderRegistry;
import com.google.cloud.dataflow.sdk.coders.DoubleCoder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.coders.SerializableCoder;
import com.google.cloud.dataflow.sdk.runners.DirectPipeline;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner.EvaluationResults;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Combine.CombineFn;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Tests for the ApproximateUnique aggregator transform.
 */
@RunWith(JUnit4.class)
@SuppressWarnings("serial")
public class ApproximateUniqueTest {

  @Test
  public void testEstimationErrorToSampleSize() {
    assertEquals(40000, ApproximateUnique.sampleSizeFromEstimationError(0.01));
    assertEquals(10000, ApproximateUnique.sampleSizeFromEstimationError(0.02));
    assertEquals(2500, ApproximateUnique.sampleSizeFromEstimationError(0.04));
    assertEquals(1600, ApproximateUnique.sampleSizeFromEstimationError(0.05));
    assertEquals(400, ApproximateUnique.sampleSizeFromEstimationError(0.1));
    assertEquals(100, ApproximateUnique.sampleSizeFromEstimationError(0.2));
    assertEquals(25, ApproximateUnique.sampleSizeFromEstimationError(0.4));
    assertEquals(16, ApproximateUnique.sampleSizeFromEstimationError(0.5));
  }

  public <T> PCollection<T> createInput(Pipeline p, Iterable<T> input,
      Coder<T> coder) {
    return p.apply(Create.of(input)).setCoder(coder);
  }

  @Test
  @Category(com.google.cloud.dataflow.sdk.testing.RunnableOnService.class)
  public void testApproximateUniqueWithSmallInput() {
    Pipeline p = TestPipeline.create();

    PCollection<Integer> input =
        createInput(p, Arrays.asList(1, 2, 3, 3), BigEndianIntegerCoder.of());

    PCollection<Long> estimate = input
        .apply(ApproximateUnique.<Integer>globally(1000));

    DataflowAssert.that(estimate).containsInAnyOrder(3L);

    p.run();
  }

  @Test
  public void testApproximateUniqueWithDuplicates() {
    runApproximateUniqueWithDuplicates(100, 100, 100);
    runApproximateUniqueWithDuplicates(1000, 1000, 100);
    runApproximateUniqueWithDuplicates(1500, 1000, 100);
    runApproximateUniqueWithDuplicates(10000, 1000, 100);
  }

  private void runApproximateUniqueWithDuplicates(int elementCount,
      int uniqueCount, int sampleSize) {

    assert elementCount >= uniqueCount;
    List<Double> elements = Lists.newArrayList();
    for (int i = 0; i < elementCount; i++) {
      elements.add(1.0 / (i % uniqueCount + 1));
    }
    Collections.shuffle(elements);

    DirectPipeline p = DirectPipeline.createForTest();
    PCollection<Double> input = createInput(p, elements, DoubleCoder.of());
    PCollection<Long> estimate =
        input.apply(ApproximateUnique.<Double>globally(sampleSize));

    EvaluationResults results = p.run();

    verifyEstimate(uniqueCount, sampleSize,
        results.getPCollection(estimate).get(0));
  }

  @Test
  public void testApproximateUniqueWithSkewedDistributions() {
    runApproximateUniqueWithSkewedDistributions(100, 100, 100);
    runApproximateUniqueWithSkewedDistributions(10000, 10000, 100);
    runApproximateUniqueWithSkewedDistributions(10000, 1000, 100);
    runApproximateUniqueWithSkewedDistributions(10000, 200, 100);
  }

  @Test
  public void testApproximateUniqueWithSkewedDistributionsAndLargeSampleSize() {
    runApproximateUniqueWithSkewedDistributions(10000, 2000, 1000);
  }

  private void runApproximateUniqueWithSkewedDistributions(int elementCount,
      final int uniqueCount, final int sampleSize) {
    List<Integer> elements = Lists.newArrayList();
    // Zipf distribution with approximately elementCount items.
    double s = 1 - 1.0 * uniqueCount / elementCount;
    double maxCount = Math.pow(uniqueCount, s);
    for (int k = 0; k < uniqueCount; k++) {
      int count = Math.max(1, (int) Math.round(maxCount * Math.pow(k, -s)));
      // Element k occurs count times.
      for (int c = 0; c < count; c++) {
        elements.add(k);
      }
    }

    DirectPipeline p = DirectPipeline.createForTest();
    PCollection<Integer> input =
        createInput(p, elements, BigEndianIntegerCoder.of());
    PCollection<Long> estimate =
        input.apply(ApproximateUnique.<Integer>globally(sampleSize));

    EvaluationResults results = p.run();

    verifyEstimate(uniqueCount, sampleSize,
        results.getPCollection(estimate).get(0).longValue());
  }

  @Test
  public void testApproximateUniquePerKey() {
    List<KV<Integer, Integer>> elements = Lists.newArrayList();
    List<Integer> keys = ImmutableList.of(20, 50, 100);
    int elementCount = 1000;
    int sampleSize = 100;
    // Use the key as the number of unique values.
    for (int uniqueCount : keys) {
      for (int value = 0; value < elementCount; value++) {
        elements.add(KV.of(uniqueCount, value % uniqueCount));
      }
    }

    DirectPipeline p = DirectPipeline.createForTest();
    PCollection<KV<Integer, Integer>> input = createInput(p, elements,
        KvCoder.of(BigEndianIntegerCoder.of(), BigEndianIntegerCoder.of()));
    PCollection<KV<Integer, Long>> counts =
        input.apply(ApproximateUnique.<Integer, Integer>perKey(sampleSize));

    EvaluationResults results = p.run();

    for (KV<Integer, Long> result : results.getPCollection(counts)) {
      verifyEstimate(result.getKey(), sampleSize, result.getValue());
    }
  }

  /**
   * Applies {@link ApproximateUnique} for different sample sizes and verifies
   * that the estimation error falls within the maximum allowed error of
   * {@code 2 / sqrt(sampleSize)}.
   */
  @Test
  public void testApproximateUniqueWithDifferentSampleSizes() {
    runApproximateUniquePipeline(16);
    runApproximateUniquePipeline(64);
    runApproximateUniquePipeline(128);
    runApproximateUniquePipeline(256);
    runApproximateUniquePipeline(512);
    runApproximateUniquePipeline(1000);
    runApproximateUniquePipeline(1024);
    try {
      runApproximateUniquePipeline(15);
      fail("Accepted sampleSize < 16");
    } catch (IllegalArgumentException e) {
      assertTrue("Expected an exception due to sampleSize < 16", e.getMessage()
          .startsWith("ApproximateUnique needs a sampleSize >= 16"));
    }
  }

  /**
   * Applies {@code ApproximateUnique(sampleSize)} verifying that the estimation
   * error falls within the maximum allowed error of {@code 2/sqrt(sampleSize)}.
   */
  private void runApproximateUniquePipeline(int sampleSize) {
    DirectPipeline p = DirectPipeline.createForTest();
    PCollection<String> collection = readPCollection(p);

    PCollection<Long> exact = collection.apply(RemoveDuplicates.<String>create())
        .apply(Combine.globally(new CountElements<String>()));

    PCollection<Long> approximate =
        collection.apply(ApproximateUnique.<String>globally(sampleSize));

    EvaluationResults results = p.run();

    verifyEstimate(results.getPCollection(exact).get(0).longValue(), sampleSize,
        results.getPCollection(approximate).get(0).longValue());
  }

  /**
   * Reads a large {@code PCollection<String>}.
   */
  private PCollection<String> readPCollection(Pipeline p) {
    // TODO: Read PCollection from a set of text files.
    List<String> page = TestUtils.LINES;
    final int pages = 1000;
    ArrayList<String> file = new ArrayList<>(pages * page.size());
    for (int i = 0; i < pages; i++) {
      file.addAll(page);
    }
    assert file.size() == pages * page.size();
    PCollection<String> words = TestUtils.createStrings(p, file);
    return words;
  }

  /**
   * Checks that the estimation error, i.e., the difference between
   * {@code uniqueCount} and {@code estimate} is less than
   * {@code 2 / sqrt(sampleSize}).
   */
  private static void verifyEstimate(long uniqueCount, int sampleSize,
      long estimate) {
    if (uniqueCount < sampleSize) {
      assertEquals("Number of hashes is less than the sample size. "
          + "Estimate should be exact", uniqueCount, estimate);
    }

    double error = 100.0 * Math.abs(estimate - uniqueCount) / uniqueCount;
    double maxError = 100.0 * 2 / Math.sqrt(sampleSize);

    assertTrue("Estimate= " + estimate + " Actual=" + uniqueCount + " Error="
        + error + "%, MaxError=" + maxError + "%.", error < maxError);
  }

  /**
   * Combiner function counting the number of elements in an input PCollection.
   *
   * @param <E> the type of elements in the input PCollection.
   */
  private static class CountElements<E> extends CombineFn<E, Long[], Long> {

    @Override
    public Long[] createAccumulator() {
      Long[] accumulator = new Long[1];
      accumulator[0] = 0L;
      return accumulator;
    }

    @Override
    public void addInput(Long[] accumulator, E input) {
      accumulator[0]++;
    }

    @Override
    public Long[] mergeAccumulators(Iterable<Long[]> accumulators) {
      Long[] sum = new Long[1];
      sum[0] = 0L;
      for (Long[] accumulator : accumulators) {
        sum[0] += accumulator[0];
      }
      return sum;
    }

    @Override
    public Long extractOutput(Long[] accumulator) {
      return accumulator[0];
    }

    @Override
    public Coder<Long[]> getAccumulatorCoder(CoderRegistry registry,
        Coder<E> inputCoder) {
      return SerializableCoder.of(Long[].class);
    }
  }
}
