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

import java.util.concurrent.atomic.AtomicLong;

final class Counter implements
    cz.seznam.euphoria.core.client.accumulators.Counter, Snapshotable<Long> {

  private AtomicLong value = new AtomicLong();

  Counter() {}

  @Override
  public void increment(long value) {
    this.value.addAndGet(value);
  }

  @Override
  public void increment() {
    increment(1);
  }

  @Override
  public Long getSnapshot() {
    return value.get();
  }

  @Override
  public String toString() {
    return "Counter{" +
        "value=" + value +
        '}';
  }
}
