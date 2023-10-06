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
package org.apache.beam.runners.samza.runtime;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.runners.samza.util.FutureUtils;
import org.apache.beam.sdk.util.WindowedValue;

class FutureCollectorImpl<OutT> implements FutureCollector<OutT> {
  private final AtomicBoolean collectorSealed;
  private CompletionStage<Collection<WindowedValue<OutT>>> outputFuture;

  FutureCollectorImpl() {
    outputFuture = CompletableFuture.completedFuture(new ArrayList<>());
    collectorSealed = new AtomicBoolean(true);
  }

  @Override
  public void add(CompletionStage<WindowedValue<OutT>> element) {
    checkState(
        !collectorSealed.get(),
        "Cannot add element to an unprepared collector. Make sure prepare() is invoked before adding elements.");

    // We need synchronize guard against scenarios when watermark/finish bundle trigger outputs.
    synchronized (this) {
      outputFuture =
          outputFuture.thenCombine(
              element,
              (collection, event) -> {
                collection.add(event);
                return collection;
              });
    }
  }

  @Override
  public void addAll(CompletionStage<Collection<WindowedValue<OutT>>> elements) {
    checkState(
        !collectorSealed.get(),
        "Cannot add elements to an unprepared collector. Make sure prepare() is invoked before adding elements.");

    synchronized (this) {
      outputFuture = FutureUtils.combineFutures(outputFuture, elements);
    }
  }

  @Override
  public void discard() {
    collectorSealed.compareAndSet(false, true);

    synchronized (this) {
      outputFuture = CompletableFuture.completedFuture(new ArrayList<>());
    }
  }

  @Override
  public CompletionStage<Collection<WindowedValue<OutT>>> finish() {
    /*
     * We can ignore the results here because its okay to call finish without invoking prepare. It will be a no-op
     * and an empty collection will be returned.
     */
    collectorSealed.compareAndSet(false, true);

    synchronized (this) {
      final CompletionStage<Collection<WindowedValue<OutT>>> sealedOutputFuture = outputFuture;
      outputFuture = CompletableFuture.completedFuture(new ArrayList<>());
      return sealedOutputFuture;
    }
  }

  @Override
  public void prepare() {
    boolean isCollectorSealed = collectorSealed.compareAndSet(true, false);
    checkState(
        isCollectorSealed,
        "Failed to prepare the collector. Collector needs to be sealed before prepare() is invoked.");
  }
}
