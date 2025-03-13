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
package org.apache.beam.it.truthmatchers;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Provides a wrapper around a List which can be used to accumulate results, useful for assertions
 * that consume inputs and need to keep state across calls.
 */
public class ListAccumulator<T> {

  private List<T> backingList;

  /** Initialize the accumulator. */
  public ListAccumulator() {
    this.backingList = new ArrayList<>();
  }

  /**
   * Add the partial results to the backingList, and return the accumulated count.
   *
   * @param partial Partial results to accumulate.
   * @return Accumulated count.
   */
  public int accumulate(Collection<T> partial) {
    this.backingList.addAll(partial);
    return this.backingList.size();
  }

  /**
   * Return the count of objects accumulated so far.
   *
   * @return Count, which is the size of the backingList.
   */
  public int count() {
    return this.backingList.size();
  }

  /** @return the internal backing list being accumulated. */
  public List<T> getBackingList() {
    return this.backingList;
  }
}
