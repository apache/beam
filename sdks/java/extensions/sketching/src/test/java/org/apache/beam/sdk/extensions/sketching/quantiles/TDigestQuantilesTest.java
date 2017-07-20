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
package org.apache.beam.sdk.extensions.sketching.quantiles;

import com.tdunning.math.stats.Centroid;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.beam.sdk.extensions.sketching.quantiles.TDigestQuantiles.SerializableTDigest;
import org.apache.beam.sdk.extensions.sketching.quantiles.TDigestQuantiles.SerializableTDigestCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Tests for {@link TDigestQuantiles}.
 */
public class TDigestQuantilesTest {

  @Rule public final transient TestPipeline tp = TestPipeline.create();

  private static final List<Double> stream = generateStream();

  private static final int size = 999;

  private static final int compression = 100;

  private static List<Double> generateStream() {
    List<Double> li = new ArrayList<>();
    for (double i = 1D; i <= size; i++) {
      li.add(i);
    }
    Collections.shuffle(li);
    return li;
  }

  @Test
  public void globally() {
    PCollection<KV<Double, Double>> col = tp.apply(Create.of(stream))
            .apply(TDigestQuantiles.globally(compression))
            .apply(ParDo.of(new RetrieveQuantiles(0.25, 0.5, 0.75, 0.99)));

    PAssert.that("Verify Accuracy", col).satisfies(new VerifyAccuracy());
    tp.run();
  }

  @Test
  public void perKey() {
    PCollection<KV<Double, Double>> col = tp.apply(Create.of(stream))
            .apply(WithKeys.<Integer, Double>of(1))
            .apply(TDigestQuantiles.<Integer>perKey(compression))
            .apply(Values.<SerializableTDigest>create())
            .apply(ParDo.of(new RetrieveQuantiles(0.25,  0.5, 0.75, 0.99)));

    PAssert.that("Verify Accuracy", col).satisfies(new VerifyAccuracy());

    tp.run();
  }

  @Test
  public void testCoder() throws Exception {
    SerializableTDigest tDigest = new SerializableTDigest(1000);
    for (int i = 0; i < 10; i++) {
      tDigest.getSketch().add((float) (2.4 + i));
    }
    Assert.assertTrue("Encode and Decode", encodeDecode(tDigest));
  }

  static class VerifyAccuracy implements SerializableFunction<Iterable<KV<Double, Double>>, Void> {

    public Void apply(Iterable<KV<Double, Double>> input) {
      for (KV<Double, Double> pair : input) {
        double expectedError = 3D / compression;
        double expectedValue = pair.getKey() * size;
        boolean isAccurate = Math.abs(pair.getValue() - expectedValue)
                / size <= expectedError;
        Assert.assertTrue("not accurate enough : \nQuantile " + pair.getKey()
                        + " is " + expectedValue + " and not " + pair.getValue(),
                isAccurate);
      }
      return null;
    }
  }

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testNaN() throws IllegalArgumentException {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Cannot add NaN to t-digest");

    SerializableTDigest std1 = new SerializableTDigest(10);
    SerializableTDigest std2 = new SerializableTDigest(10);

    std1.getSketch().add(std2.getSketch());
  }

  @Test
  public void testMergeAccum() {
      Random rd = new Random(1234);
      List<SerializableTDigest> accums = new ArrayList<>();
      for (int i = 0; i < 3; i++) {
          SerializableTDigest std = new SerializableTDigest(100);
          for (int j = 0; j < 1000; j++) {
              std.add(rd.nextDouble());
          }
          accums.add(std);
      }
      TDigestQuantiles.QuantileFn fn = new TDigestQuantiles.QuantileFn(100);
      SerializableTDigest res = fn.mergeAccumulators(accums);
  }

  private <T> boolean encodeDecode(SerializableTDigest tDigest) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    SerializableTDigestCoder tDigestCoder = new SerializableTDigestCoder();

    tDigestCoder.encode(tDigest, baos);
    byte[] bytes = baos.toByteArray();

    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
    SerializableTDigest decoded = tDigestCoder.decode(bais);

    boolean equal = true;
    // the only way to compare the two sketches is to compare them centroid by centroid.
    // Indeed, the means are doubles but are encoded as float and cast during decoding.
    // This entails a small approximation that makes the centroids different after decoding.
    Iterator<Centroid> it1 = decoded.getSketch().centroids().iterator();
    Iterator<Centroid> it2 = tDigest.getSketch().centroids().iterator();

    for (int i = 0; i < decoded.getSketch().centroids().size(); i++) {
      Centroid c1 = it1.next();
      Centroid c2 = it2.next();
      if ((float) c1.mean() != (float) c2.mean() || c1.count() != c2.count()) {
        equal = false;
        break;
      }
    }
    return equal;
  }

  static class RetrieveQuantiles extends DoFn<SerializableTDigest, KV<Double, Double>> {
    private final double quantile;
    private final double[] otherQ;

    public RetrieveQuantiles(double q, double... otherQ) {
      this.quantile = q;
      this.otherQ = otherQ;
    }

    @ProcessElement public void processElement(ProcessContext c) {
      c.output(KV.of(quantile, c.element().getSketch().quantile(quantile)));
      for (Double q : otherQ) {
        c.output(KV.of(q, c.element().getSketch().quantile(q)));
      }
    }
  }
}
