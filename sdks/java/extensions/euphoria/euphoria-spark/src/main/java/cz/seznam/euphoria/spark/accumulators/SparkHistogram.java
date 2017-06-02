/**
 * Copyright 2016-2017 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.spark.accumulators;

import cz.seznam.euphoria.core.client.accumulators.Histogram;

import java.util.HashMap;
import java.util.Map;

class SparkHistogram implements Histogram, SparkAccumulator<Map<Long, Long>> {

  private final Map<Long, Long> buckets = new HashMap<>();

  @Override
  public void add(long value) {
    this.add(value, 1);
  }

  @Override
  public void add(long value, long times) {
    buckets.compute(value, (key, count) -> count == null ? times : (count + times));
  }

  @Override
  public Map<Long, Long> value() {
    return buckets;
  }

  @Override
  public SparkAccumulator<Map<Long, Long>> merge(SparkAccumulator<Map<Long, Long>> other) {
    Map<Long, Long> otherMap = other.value();
    otherMap.forEach((k, v) -> buckets.merge(k, v, Long::sum));
    return this;
  }

  @Override
  public String toString() {
    return buckets.toString();
  }
}
