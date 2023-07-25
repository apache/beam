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
package org.apache.beam.sdk.extensions.sketching;

import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.hamcrest.MatcherAssert.assertThat;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.extensions.sketching.ApproximateDistinct.ApproximateDistinctFn;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link ApproximateDistinct}. */
@RunWith(JUnit4.class)
public class ApproximateDistinctTest implements Serializable {

  @Rule public final transient TestPipeline tp = TestPipeline.create();

  @Test
  public void smallCardinality() {
    final int smallCard = 1000;
    final int p = 6;
    final double expectedErr = 1.104 / Math.sqrt(p);

    List<Integer> small = new ArrayList<>();
    for (int i = 0; i < smallCard; i++) {
      small.add(i);
    }

    PCollection<Long> cardinality =
        tp.apply("small stream", Create.of(small))
            .apply("small cardinality", ApproximateDistinct.<Integer>globally().withPrecision(p));

    PAssert.that("Not Accurate Enough", cardinality)
        .satisfies(new VerifyAccuracy(smallCard, expectedErr));

    tp.run();
  }

  @Test
  public void bigCardinality() {
    final int cardinality = 15000;
    final int p = 15;
    final int sp = 20;
    final double expectedErr = 1.04 / Math.sqrt(p);

    List<Integer> stream = new ArrayList<>();
    for (int i = 1; i <= cardinality; i++) {
      stream.addAll(Collections.nCopies(2, i));
    }
    Collections.shuffle(stream);

    PCollection<Long> res =
        tp.apply("big stream", Create.of(stream))
            .apply(
                "big cardinality",
                ApproximateDistinct.<Integer>globally().withPrecision(p).withSparsePrecision(sp));

    PAssert.that("Verify Accuracy for big cardinality", res)
        .satisfies(new VerifyAccuracy(cardinality, expectedErr));

    tp.run();
  }

  @Test
  public void perKey() {
    final int cardinality = 1000;
    final int p = 15;
    final double expectedErr = 1.04 / Math.sqrt(p);

    List<Integer> stream = new ArrayList<>();
    for (int i = 1; i <= cardinality; i++) {
      stream.addAll(Collections.nCopies(2, i));
    }
    Collections.shuffle(stream);

    PCollection<Long> results =
        tp.apply("per key stream", Create.of(stream))
            .apply("create keys", WithKeys.of(1))
            .apply(
                "per key cardinality",
                ApproximateDistinct.<Integer, Integer>perKey().withPrecision(p))
            .apply("extract values", Values.create());

    PAssert.that("Verify Accuracy for cardinality per key", results)
        .satisfies(new VerifyAccuracy(cardinality, expectedErr));

    tp.run();
  }

  @Test
  public void customObject() {
    final int cardinality = 500;
    final int p = 15;
    final double expectedErr = 1.04 / Math.sqrt(p);

    Schema schema =
        SchemaBuilder.record("User")
            .fields()
            .requiredString("Pseudo")
            .requiredInt("Age")
            .endRecord();
    List<GenericRecord> users = new ArrayList<>();
    for (int i = 1; i <= cardinality; i++) {
      GenericData.Record newRecord = new GenericData.Record(schema);
      newRecord.put("Pseudo", "User" + i);
      newRecord.put("Age", i);
      users.add(newRecord);
    }
    PCollection<Long> results =
        tp.apply("Create stream", Create.of(users).withCoder(AvroCoder.of(schema)))
            .apply(
                "Test custom object",
                ApproximateDistinct.<GenericRecord>globally().withPrecision(p));

    PAssert.that("Verify Accuracy for custom object", results)
        .satisfies(new VerifyAccuracy(cardinality, expectedErr));

    tp.run();
  }

  @Test
  public void testCoder() throws Exception {
    HyperLogLogPlus hllp = new HyperLogLogPlus(12, 18);
    for (int i = 0; i < 10; i++) {
      hllp.offer(i);
    }
    CoderProperties.coderDecodeEncodeEqual(ApproximateDistinct.HyperLogLogPlusCoder.of(), hllp);
  }

  @Test
  public void testDisplayData() {
    final ApproximateDistinctFn<Integer> fnWithPrecision =
        ApproximateDistinctFn.create(BigEndianIntegerCoder.of()).withPrecision(23);

    assertThat(DisplayData.from(fnWithPrecision), hasDisplayItem("p", 23));
    assertThat(DisplayData.from(fnWithPrecision), hasDisplayItem("sp", 0));
  }

  private static class VerifyAccuracy implements SerializableFunction<Iterable<Long>, Void> {

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
            "not accurate enough : \nExpected Cardinality : "
                + expectedCard
                + "\nComputed Cardinality : "
                + estimate,
            isAccurate);
      }
      return null;
    }
  }
}
