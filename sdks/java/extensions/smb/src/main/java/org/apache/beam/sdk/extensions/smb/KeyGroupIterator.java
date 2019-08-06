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
package org.apache.beam.sdk.extensions.smb;

import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Function;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterators;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.PeekingIterator;

/**
 * Converts an {@code Iterator<V>} to an {@code Iterator<KV<K, Iterator<V>>}, grouping values of the
 * same key. Values in the input iterator must be sorted by key, and values in the output {@code
 * KV<K, Iterator<V>>} must be exhausted before proceeding to the next key group.
 */
class KeyGroupIterator<K, V> implements Iterator<KV<K, Iterator<V>>> {
  private final PeekingIterator<V> iterator;
  private final Function<V, K> keyFn;
  private final Comparator<K> keyComparator;

  private Iterator<V> currentGroup = null;

  KeyGroupIterator(Iterator<V> iterator, Function<V, K> keyFn, Comparator<K> keyComparator) {
    this.iterator = Iterators.peekingIterator(iterator);
    this.keyFn = keyFn;
    this.keyComparator = keyComparator;
  }

  @Override
  public boolean hasNext() {
    checkState();
    return iterator.hasNext();
  }

  @Override
  public KV<K, Iterator<V>> next() {
    checkState();
    K k = keyFn.apply(iterator.peek());

    Iterator<V> vi =
        new Iterator<V>() {
          @Override
          public boolean hasNext() {
            boolean r;
            if (iterator.hasNext()) {
              V nextV = iterator.peek();
              K nextK = keyFn.apply(nextV);
              r = keyComparator.compare(k, nextK) == 0;
            } else {
              r = false;
            }
            if (!r) {
              currentGroup = null;
            }
            return r;
          }

          @Override
          public V next() {
            V nextV = iterator.peek();
            K nextK = keyFn.apply(nextV);
            if (keyComparator.compare(k, nextK) != 0) {
              currentGroup = null;
              throw new NoSuchElementException();
            }
            iterator.next();
            return nextV;
          }
        };

    currentGroup = vi;
    return KV.of(k, vi);
  }

  private void checkState() {
    Preconditions.checkState(currentGroup == null, "Previous Iterator<V> not fully iterated");
  }
}
