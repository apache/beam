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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

final class Histogram implements
    cz.seznam.euphoria.core.client.accumulators.Histogram, GetSnapshot<Map<Long, Integer>> {

  final Map<Long, Integer> buckets = new ConcurrentHashMap<>();

  Histogram() {}

  @Override
  public void add(long value, long times) {
    buckets.compute(value, (key, count) -> count == null ? 1 : (count + 1));
  }

  @Override
  public void add(long value) {
    add(value, 1);
  }

  public Map<Long, Integer> getSnapshot() {
    return new HashMap<>(buckets);
  }
}
