/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataflow.sdk.util.state;

import com.google.common.collect.Iterables;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Implementation of {@link BagState} reads from all the sources and writes to the specified result.
 *
 * @param <T> the type of elements in the bag
 */
class MergedBag<T> implements BagState<T> {

  private final Collection<BagState<T>> sources;
  private final BagState<T> result;

  public MergedBag(Collection<BagState<T>> sources, BagState<T> result) {
    this.sources = sources;
    this.result = result;
  }

  @Override
  public void clear() {
    for (State source : sources) {
      source.clear();
    }
    result.clear();
  }

  @Override
  public void add(T input) {
    result.add(input);
  }

  @Override
  public StateContents<Iterable<T>> get() {
    // Initiate the get's right away
    final List<StateContents<Iterable<T>>> futures = new ArrayList<>(sources.size());
    for (BagState<T> source : sources) {
      futures.add(source.get());
    }

    // But defer the actual reads until later.
    return new StateContents<Iterable<T>>() {
      @Override
      public Iterable<T> read() {
        // Can't use FluentIterables#toList because some values may be legitimately null.
        List<T> result = new ArrayList<>();
        for (StateContents<Iterable<T>> future : futures) {
          Iterables.addAll(result, future.read());
        }
        return result;
      }
    };
  }

  @Override
  public StateContents<Boolean> isEmpty() {
    // Initiate the get's right away
    final List<StateContents<Boolean>> futures = new ArrayList<>(sources.size());
    for (BagState<T> source : sources) {
      futures.add(source.isEmpty());
    }

    // But defer the actual reads until later.
    return new StateContents<Boolean>() {
      @Override
      public Boolean read() {
        for (StateContents<Boolean> future : futures) {
          if (!future.read()) {
            return false;
          }
        }
        return true;
      }
    };
  }
}
