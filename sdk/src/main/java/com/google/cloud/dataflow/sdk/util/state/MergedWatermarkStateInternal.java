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


import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.OutputTimeFn;
import com.google.common.collect.Lists;

import org.joda.time.Instant;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Implementation of {@link WatermarkStateInternal} reading from multiple sources and writing to a
 * single result.
 */
class MergedWatermarkStateInternal<W extends BoundedWindow> implements WatermarkStateInternal {

  private final Collection<WatermarkStateInternal> sources;
  private final WatermarkStateInternal result;
  private final OutputTimeFn<? super W> outputTimeFn;
  private final W resultWindow;

  public MergedWatermarkStateInternal(
      Collection<WatermarkStateInternal> sources,
      WatermarkStateInternal result,
      W resultWindow,
      OutputTimeFn<? super W> outputTimeFn) {
    this.sources = sources;
    this.result = result;
    this.resultWindow = resultWindow;
    this.outputTimeFn = outputTimeFn;
  }

  @Override
  public void clear() {
    for (State source : sources) {
      source.clear();
    }
    result.clear();
  }

  @Override
  public void add(Instant outputTimestamp) {
    result.add(outputTimestamp);
  }

  @Override
  public StateContents<Instant> get() {
    // Short circuit if output times depend only on the window, hence are all equal.
    if (outputTimeFn.dependsOnlyOnWindow()) {
      return result.get();
    }

    // Get the underlying StateContents's right away.
    final List<StateContents<Instant>> reads = new ArrayList<>(sources.size());
    for (WatermarkStateInternal source : sources) {
      reads.add(source.get());
    }

    // But defer actually reading them.
    return new StateContents<Instant>() {
      @Override
      public Instant read() {
        List<Instant> outputTimesToMerge = Lists.newArrayListWithCapacity(sources.size());
        for (StateContents<Instant> read : reads) {
          Instant sourceOutputTime = read.read();
          if (sourceOutputTime != null) {
            outputTimesToMerge.add(sourceOutputTime);
          }
        }

        if (outputTimesToMerge.isEmpty()) {
          return null;
        } else {
          // Also, compact the state
          clear();
          Instant mergedOutputTime = outputTimeFn.merge(resultWindow, outputTimesToMerge);
          add(mergedOutputTime);
          return mergedOutputTime;
        }
      }
    };
  }

  @Override
  public StateContents<Boolean> isEmpty() {
    // Initiate the get's right away
    final List<StateContents<Boolean>> futures = new ArrayList<>(sources.size());
    for (WatermarkStateInternal source : sources) {
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

  @Override
  public void releaseExtraneousHolds() {
    if (outputTimeFn.dependsOnlyOnEarliestInputTimestamp()) {
      // The backend is implicitly already holding the output watermark to
      // the minimum of all holds in all merged windows. Therefore, we don't need to
      // explicitly change it.
      // When the final (post merged) session window fires, we will collect all holds
      // over all intermediate (pre merged) windows, take their min, and clear them.
      // Therefore we also don't need to garbage collect any state here.
    } else {
      // The output time function may be able to move the hold forward. get() implements
      // the necessary combining logic, and as a side effect will compact the hold
      // in Windmill state. This ensures Windmill's output watermark can progress, and
      // there is no stale hold left behind.
      get().read();
    }
  }
}
