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

import static org.apache.commons.math3.stat.StatUtils.sum;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.io.range.OffsetRange;

/** Determines bundle sizes based on @{link SyntheticSourceOptions}. */
public class BundleSplitter implements Serializable {

  private SyntheticSourceOptions options;

  public BundleSplitter(SyntheticSourceOptions options) {
    this.options = options;
  }

  List<OffsetRange> getBundleSizes(int desiredNumBundles, long start, long end) {
    List<OffsetRange> result = new ArrayList<>();

    double[] relativeSizes = getRelativeBundleSizes(desiredNumBundles);

    // Generate offset ranges proportional to the relative sizes.
    double s = sum(relativeSizes);
    long startOffset = start;
    double sizeSoFar = 0;
    for (int i = 0; i < relativeSizes.length; ++i) {
      sizeSoFar += relativeSizes[i];

      long endOffset =
          (i == relativeSizes.length - 1) ? end : (long) (start + sizeSoFar * (end - start) / s);

      if (startOffset != endOffset) {
        result.add(new OffsetRange(startOffset, endOffset));
      }
      startOffset = endOffset;
    }
    return result;
  }

  private double[] getRelativeBundleSizes(int desiredNumBundles) {
    double[] relativeSizes = new double[desiredNumBundles];
    for (int i = 0; i < relativeSizes.length; ++i) {
      relativeSizes[i] =
          options.bundleSizeDistribution.sample(options.hashFunction().hashInt(i).asLong());
    }
    return relativeSizes;
  }
}
