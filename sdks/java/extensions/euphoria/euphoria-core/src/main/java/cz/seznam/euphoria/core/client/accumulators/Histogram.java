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
package cz.seznam.euphoria.core.client.accumulators;

/**
 * Histogram is a type of accumulator recording a distribution of different values.
 */
public interface Histogram extends Accumulator {

  /**
   * Add specified value.
   * @param value Value to be added.
   */
  void add(long value);

  /**
   * Add specified value multiple times.
   * @param value Value to be added.
   * @param times Number of occurrences to add.
   */
  void add(long value, long times);
}
