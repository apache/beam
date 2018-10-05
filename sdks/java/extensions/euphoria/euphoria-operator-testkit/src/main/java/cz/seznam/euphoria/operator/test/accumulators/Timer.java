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
package cz.seznam.euphoria.operator.test.accumulators;

import cz.seznam.euphoria.shaded.guava.com.google.common.collect.Maps;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

final class Timer implements
    cz.seznam.euphoria.core.client.accumulators.Timer, Snapshotable<Map<Duration, Long>> {

  private final Histogram hist = new Histogram();

  Timer() {}

  @Override
  public void add(long duration, TimeUnit unit) {
    hist.add(unit.toNanos(duration), 1);
  }

  @Override
  public void add(Duration duration) {
    add(duration.toNanos(), TimeUnit.NANOSECONDS);
  }

  public Map<Duration, Long> getSnapshot() {
    Map<Duration, Long> m = Maps.newHashMapWithExpectedSize(hist.buckets.size());
    hist.buckets.forEach((key, count) -> m.put(Duration.ofNanos(key), count));
    return m;
  }
}
