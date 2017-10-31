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
package org.apache.beam.sdk.extensions.sketching.cardinality;

import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.hamcrest.MatcherAssert.assertThat;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for {@link ApproximateDistinct}.
 */
public class ApproximateDistinctTest implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(ApproximateDistinctTest.class);

  @Rule
  public final transient TestPipeline tp = TestPipeline.create();

  @Test
  public void smallCardinality() {
    final int smallCard = 1000;
    List<Integer> small = new ArrayList<>();
    for (int i = 0; i < smallCard; i++) {
      small.add(i);
    }

    final int p = 6;
    final Double expectedErr = 1.104 / Math.sqrt(p);

    PCollection<Long> cardinality = tp.apply(Create.<Integer> of(small))
            .apply(ApproximateDistinct.<Integer>globally(p))
            .apply(ParDo.of(new RetrieveDistinct()));

    PAssert.thatSingleton("Not Accurate Enough", cardinality)
            .satisfies(new SerializableFunction<Long, Void>() {
              @Override
              public Void apply(Long input) {
                boolean isAccurate = Math.abs(input - smallCard) / smallCard < expectedErr;
                Assert.assertTrue("not accurate enough : \nExpected Cardinality : "
                                + smallCard + "\nComputed Cardinality : " + input,
                        isAccurate);
                return null;
              }
            });
    tp.run();
    }

  @Test
  public void createSparse4BigCardinality() {
    final int cardinality = 15000;
    final int p = 15;
    final int sp = 20;
    final Double expectedErr = 1.04 / Math.sqrt(p);

    List<Integer> stream = new ArrayList<>();
    for (int i = 1; i <= cardinality; i++) {
      stream.addAll(Collections.nCopies(2, i));
    }
    Collections.shuffle(stream);

    PCollection<Long> res = tp.apply(Create.<Integer>of(stream))
            .apply(Combine.globally(ApproximateDistinct.ApproximateDistinctFn.<Integer>create(p)
                    .withSparseRepresentation(sp)))
            .apply(ParDo.of(new RetrieveDistinct()));

    PAssert.that("Verify Accuracy", res)
            .satisfies(new VerifyAccuracy(cardinality, expectedErr));

    tp.run();
  }

  @Test
  public void perKey() {
    final int cardinality = 1000;
    final int p = 15;
    final Double expectedErr = 1.04 / Math.sqrt(p);
    List<Integer> stream = new ArrayList<>();
    for (int i = 1; i <= cardinality; i++) {
      stream.addAll(Collections.nCopies(2, i));
    }
    Collections.shuffle(stream);

    PCollection<Long> results = tp.apply(Create.of(stream))
            .apply(WithKeys.<Integer, Integer>of(1))
            .apply(ApproximateDistinct.<Integer, Integer>perKey(p))
            .apply(Values.<HyperLogLogPlus>create())
            .apply(ParDo.of(new RetrieveDistinct()));

    PAssert.that("Verify Accuracy", results)
            .satisfies(new VerifyAccuracy(cardinality, expectedErr));
    tp.run();
  }

  static class RetrieveDistinct extends DoFn<HyperLogLogPlus, Long> {
    @ProcessElement
    public void apply(ProcessContext c) {
      Long card = c.element().cardinality();
      LOG.debug("Number of distinct Elements : " + card);
      c.output(card);
    }
  }

  @Test
  public void testCoder() throws Exception {
    HyperLogLogPlus hllp = new HyperLogLogPlus(12, 18);
    for (int i = 0; i < 10; i++) {
      hllp.offer(i);
    }
    CoderProperties.<HyperLogLogPlus>coderDecodeEncodeEqual(
            ApproximateDistinct.HyperLogLogPlusCoder.of(), hllp);
  }

  @Test
  public void testDisplayData() {
    final Combine.Globally<Integer, HyperLogLogPlus> specifiedPrecision =
            ApproximateDistinct.globally(23);

    assertThat(DisplayData.from(specifiedPrecision), hasDisplayItem("p", 23));
    assertThat(DisplayData.from(specifiedPrecision), hasDisplayItem("sp", 0));

  }

  class VerifyAccuracy implements SerializableFunction<Iterable<Long>, Void> {

    private final int expectedCard;

    private final double expectedError;

    VerifyAccuracy(int expectedCard, double expectedError) {
      this.expectedCard = expectedCard;
      this.expectedError = expectedError;
    }

    @Override
    public Void apply(Iterable<Long> input) {
      for (Long estimate : input) {
        boolean isAccurate = Math.abs(estimate - expectedCard) / expectedCard < expectedError;
        Assert.assertTrue(
                "not accurate enough : \nExpected Cardinality : " + expectedCard
                        + "\nComputed Cardinality : " + estimate,
                isAccurate);
      }
      return null;
    }
  }
}
