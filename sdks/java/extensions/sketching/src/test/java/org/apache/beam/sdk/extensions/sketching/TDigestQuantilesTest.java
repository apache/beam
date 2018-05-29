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
import static org.junit.Assert.assertThat;

import com.tdunning.math.stats.Centroid;
import com.tdunning.math.stats.MergingDigest;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import org.apache.beam.sdk.extensions.sketching.TDigestQuantiles.MergingDigestCoder;
import org.apache.beam.sdk.extensions.sketching.TDigestQuantiles.TDigestQuantilesFn;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Tests for {@link TDigestQuantiles}. */
public class TDigestQuantilesTest {

  @Rule public final transient TestPipeline tp = TestPipeline.create();

  private static final List<Double> stream = generateStream();

  private static final int size = 999;

  private static final int compression = 100;

  private static final double[] quantiles = {0.25, 0.5, 0.75, 0.99};

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
            .apply(TDigestQuantiles.globally().withCompression(compression))
            .apply(ParDo.of(new RetrieveQuantiles(quantiles)));

    PAssert.that("Verify Accuracy", col).satisfies(new VerifyAccuracy());
    tp.run();
  }

  @Test
  public void perKey() {
    PCollection<KV<Double, Double>> col = tp.apply(Create.of(stream))
            .apply(WithKeys.<Integer, Double>of(1))
            .apply(TDigestQuantiles.<Integer>perKey().withCompression(compression))
            .apply(Values.<MergingDigest>create())
            .apply(ParDo.of(new RetrieveQuantiles(quantiles)));

    PAssert.that("Verify Accuracy", col).satisfies(new VerifyAccuracy());

    tp.run();
  }

  @Test
  public void testCoder() throws Exception {
    MergingDigest tDigest = new MergingDigest(1000);
    for (int i = 0; i < 10; i++) {
      tDigest.add(2.4 + i);
    }

    Assert.assertTrue("Encode and Decode", encodeDecodeEquals(tDigest));
  }

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testMergeAccum() {
      Random rd = new Random(1234);
      List<MergingDigest> accums = new ArrayList<>();
      for (int i = 0; i < 3; i++) {
          MergingDigest std = new MergingDigest(100);
          for (int j = 0; j < 1000; j++) {
              std.add(rd.nextDouble());
          }
          accums.add(std);
      }
      TDigestQuantilesFn fn = TDigestQuantilesFn.create(100);
      MergingDigest res = fn.mergeAccumulators(accums);
  }

  private <T> boolean encodeDecodeEquals(MergingDigest tDigest) throws IOException {
    MergingDigest decoded = CoderUtils.clone(new MergingDigestCoder(), tDigest);

    boolean equal = true;
    // the only way to compare the two sketches is to compare them centroid by centroid.
    // Indeed, the means are doubles but are encoded as float and cast during decoding.
    // This entails a small approximation that makes the centroids different after decoding.
    Iterator<Centroid> it1 = decoded.centroids().iterator();
    Iterator<Centroid> it2 = tDigest.centroids().iterator();

    for (int i = 0; i < decoded.centroids().size(); i++) {
      Centroid c1 = it1.next();
      Centroid c2 = it2.next();
      if ((float) c1.mean() != (float) c2.mean() || c1.count() != c2.count()) {
        equal = false;
        break;
      }
    }
    return equal;
  }

  @Test
  public void testDisplayData() {
    final TDigestQuantilesFn fn = TDigestQuantilesFn.create(155D);
    assertThat(DisplayData.from(fn), hasDisplayItem("compression", 155D));
  }

  static class RetrieveQuantiles extends DoFn<MergingDigest, KV<Double, Double>> {
    private final double[] quantiles;

    public RetrieveQuantiles(double[] quantiles) {
      this.quantiles = quantiles;
    }

    @ProcessElement public void processElement(ProcessContext c) {
      for (double q : quantiles) {
        c.output(KV.of(q, c.element().quantile(q)));
      }
    }
  }

  static class VerifyAccuracy implements SerializableFunction<Iterable<KV<Double, Double>>, Void> {

    double expectedError = 3D / compression;

    public Void apply(Iterable<KV<Double, Double>> input) {
      for (KV<Double, Double> pair : input) {
        double expectedValue = pair.getKey() * (size + 1);
        boolean isAccurate = Math.abs(pair.getValue() - expectedValue)
                / size <= expectedError;
        Assert.assertTrue("not accurate enough : \nQuantile " + pair.getKey()
                        + " is " + pair.getValue() + " and not " + expectedValue,
                isAccurate);
      }
      return null;
    }
  }
}
