/*
 * Copyright 2016-2018 Seznam.cz, a.s.
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

import cz.seznam.euphoria.core.client.accumulators.Timer;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

class SparkTimer implements Timer, SparkAccumulator<Map<Duration, Long>> {

  private final SparkHistogram hist = new SparkHistogram();

  @Override
  public Map<Duration, Long> value() {
    // transform Map[Long, Long] to Map[Duration, Long]
    return hist.value().entrySet().stream()
            .collect(Collectors.toMap(e -> Duration.ofNanos(e.getKey()), Map.Entry::getValue));
  }

  @Override
  public void add(long duration, TimeUnit unit) {
    hist.add(unit.toNanos(duration), 1);
  }

  @Override
  public void add(Duration duration) {
    this.add(duration.toNanos(), TimeUnit.NANOSECONDS);
  }

  @Override
  public SparkAccumulator<Map<Duration, Long>> merge(SparkAccumulator<Map<Duration, Long>> other) {
    Map<Duration, Long> otherMap = other.value();
    otherMap.forEach((duration, times) -> hist.add(duration.toNanos(), times));
    return this;
  }

  @Override
  public String toString() {
    return hist.toString();
  }
}
