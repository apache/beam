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

// FIXME: current limitation: must exhaust Iterator<ValueT> before starting the next key group
class KeyGroupIterator<KeyT, ValueT> implements Iterator<KV<KeyT, Iterator<ValueT>>> {
  private final PeekingIterator<ValueT> iterator;
  private final Function<ValueT, KeyT> keyFn;
  private final Comparator<KeyT> keyComparator;

  private Iterator<ValueT> currentGroup = null;

  KeyGroupIterator(
      Iterator<ValueT> iterator, Function<ValueT, KeyT> keyFn, Comparator<KeyT> keyComparator) {
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
  public KV<KeyT, Iterator<ValueT>> next() {
    checkState();
    KeyT k = keyFn.apply(iterator.peek());

    Iterator<ValueT> vi =
        new Iterator<ValueT>() {
          @Override
          public boolean hasNext() {
            boolean r;
            if (iterator.hasNext()) {
              ValueT nextV = iterator.peek();
              KeyT nextK = keyFn.apply(nextV);
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
          public ValueT next() {
            ValueT nextV = iterator.peek();
            KeyT nextK = keyFn.apply(nextV);
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
    Preconditions.checkState(currentGroup == null, "Previous Iterator<ValueT> not fully iterated");
  }
}
