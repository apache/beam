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
package org.apache.beam.sdk.io.synthetic;

import static org.apache.beam.sdk.io.synthetic.SyntheticOptions.fromRealDistribution;
import static org.junit.Assert.assertEquals;

import java.util.List;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.commons.math3.distribution.ConstantRealDistribution;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link BundleSplitter}. */
@RunWith(JUnit4.class)
public class BundleSplitterTest {

  private SyntheticSourceOptions options;

  private BundleSplitter splitter;

  @Before
  public void setUp() throws Exception {
    options = new SyntheticSourceOptions();
    options.numRecords = 8;
    options.keySizeBytes = 1;
    options.valueSizeBytes = 1;

    splitter = new BundleSplitter(options);
  }

  @Test
  public void shouldCreateAsManyBundlesAsItsDesired() {
    splitter = new BundleSplitter(options);
    int expectedBundleCount = 4;

    List bundleSizes = splitter.getBundleSizes(expectedBundleCount, 0, options.numRecords);

    assertEquals(expectedBundleCount, bundleSizes.size());
  }

  @Test
  public void bundlesShouldBeEvenForConstDistribution() {
    long expectedBundleSize = 2;
    options.bundleSizeDistribution = fromRealDistribution(new ConstantRealDistribution(2));
    splitter = new BundleSplitter(options);

    List<OffsetRange> bundleSizes = splitter.getBundleSizes(4, 0, options.numRecords);

    bundleSizes.stream()
        .map(range -> range.getTo() - range.getFrom())
        .forEach(size -> assertEquals(expectedBundleSize, size.intValue()));
  }

  @Test
  public void bundleSizesShouldBeProportionalToTheOneSuggestedInBundleSizeDistribution() {
    long expectedBundleSize = 4;
    options.bundleSizeDistribution = fromRealDistribution(new ConstantRealDistribution(2));
    options.numRecords = 16;
    splitter = new BundleSplitter(options);

    List<OffsetRange> bundleSizes = splitter.getBundleSizes(4, 0, options.numRecords);

    bundleSizes.stream()
        .map(range -> range.getTo() - range.getFrom())
        .forEach(size -> assertEquals(expectedBundleSize, size.intValue()));
  }

  @Test
  public void consequentBundlesShouldHaveTheSameRangeEndAndStart() {
    int desiredNumberOfBundles = 2;
    options.bundleSizeDistribution = fromRealDistribution(new ConstantRealDistribution(2));
    splitter = new BundleSplitter(options);

    List<OffsetRange> bundleSizes =
        splitter.getBundleSizes(desiredNumberOfBundles, 0, options.numRecords);

    assertEquals(bundleSizes.get(0).getTo(), bundleSizes.get(1).getFrom());
    assertEquals(bundleSizes.get(0).getTo(), bundleSizes.get(1).getFrom());
    assertEquals(desiredNumberOfBundles, bundleSizes.size());
  }
}
