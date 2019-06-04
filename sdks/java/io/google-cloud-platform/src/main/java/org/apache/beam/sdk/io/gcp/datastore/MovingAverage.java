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
package org.apache.beam.sdk.io.gcp.datastore;

import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.util.MovingFunction;

class MovingAverage {
  private final MovingFunction sum;
  private final MovingFunction count;

  public MovingAverage(
      long samplePeriodMs,
      long sampleUpdateMs,
      int numSignificantBuckets,
      int numSignificantSamples) {
    sum =
        new MovingFunction(
            samplePeriodMs,
            sampleUpdateMs,
            numSignificantBuckets,
            numSignificantSamples,
            Sum.ofLongs());
    count =
        new MovingFunction(
            samplePeriodMs,
            sampleUpdateMs,
            numSignificantBuckets,
            numSignificantSamples,
            Sum.ofLongs());
  }

  public void add(long nowMsSinceEpoch, long value) {
    sum.add(nowMsSinceEpoch, value);
    count.add(nowMsSinceEpoch, 1);
  }

  public long get(long nowMsSinceEpoch) {
    return sum.get(nowMsSinceEpoch) / count.get(nowMsSinceEpoch);
  }

  public boolean hasValue(long nowMsSinceEpoch) {
    return sum.isSignificant() && count.isSignificant() && count.get(nowMsSinceEpoch) > 0;
  }
}
