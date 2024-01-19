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
package org.apache.beam.runners.dataflow.worker.windmill.state;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterators;

class ConcatIterables<T> implements Iterable<T> {
  // List of component iterables.  Should only be appended to in order to support snapshot().
  private final List<Iterable<T>> iterables;

  public ConcatIterables() {
    this.iterables = new ArrayList<>();
  }

  public void extendWith(Iterable<T> iterable) {
    iterables.add(iterable);
  }

  @Override
  public Iterator<T> iterator() {
    return Iterators.concat(Iterables.transform(iterables, Iterable::iterator).iterator());
  }

  /**
   * Returns a view of the current state of this iterable. Remembers the current length of iterables
   * so that the returned value Will not change due to future extendWith() calls.
   */
  public Iterable<T> snapshot() {
    final int limit = iterables.size();
    final List<Iterable<T>> iterablesList = iterables;
    return () ->
        Iterators.concat(
            Iterators.transform(
                Iterators.limit(iterablesList.iterator(), limit), Iterable::iterator));
  }
}
