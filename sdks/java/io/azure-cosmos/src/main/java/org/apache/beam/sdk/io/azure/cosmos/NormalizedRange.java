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
package org.apache.beam.sdk.io.azure.cosmos;

import com.azure.cosmos.implementation.feedranges.FeedRangeEpkImpl;
import com.azure.cosmos.implementation.routing.PartitionKeyInternalHelper;
import com.azure.cosmos.implementation.routing.Range;
import com.azure.cosmos.models.FeedRange;
import java.io.Serializable;
import java.math.BigInteger;

public class NormalizedRange implements Serializable {

  public static final NormalizedRange FULL_RANGE =
      new NormalizedRange(
          PartitionKeyInternalHelper.MinimumInclusiveEffectivePartitionKey,
          PartitionKeyInternalHelper.MaximumExclusiveEffectivePartitionKey);

  private final String minInclusive;
  private final String maxExclusive;

  public static NormalizedRange fromFeedRange(FeedRange feedRange) {
    if (feedRange instanceof FeedRangeEpkImpl) {
      FeedRangeEpkImpl ekp = (FeedRangeEpkImpl) feedRange;
      Range<String> range = ekp.getRange();
      String min = range.getMin();
      String max = range.getMax();

      if (!range.isMinInclusive()) {
        min = new BigInteger(min, 16).add(BigInteger.ONE).toString(16);
      }

      if (range.isMaxInclusive()) {
        max = new BigInteger(max, 16).subtract(BigInteger.ONE).toString(16);
      }

      return new NormalizedRange(min, max);
    } else {
      throw new IllegalArgumentException("Only FeedRangeEpkImpl are supported. got: " + feedRange);
    }
  }

  public NormalizedRange(String minInclusive, String maxExclusive) {
    this.minInclusive = minInclusive;
    this.maxExclusive = maxExclusive;
  }

  public FeedRange toFeedRange() {
    return new FeedRangeEpkImpl(new Range<>(minInclusive, maxExclusive, true, false));
  }
}
