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

import org.joda.time.Instant;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Implementation of {@link WatermarkStateInternal} reading from multiple sources and writing to a
 * single result.
 */
class MergedWatermarkStateInternal implements WatermarkStateInternal {

  private final Collection<WatermarkStateInternal> sources;
  private final WatermarkStateInternal result;

  public MergedWatermarkStateInternal(
      Collection<WatermarkStateInternal> sources, WatermarkStateInternal result) {
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
  public void add(Instant watermarkHold) {
    result.add(watermarkHold);
  }

  @Override
  public StateContents<Instant> get() {
    // Get the underlying StateContents's right away.
    final List<StateContents<Instant>> reads = new ArrayList<>(sources.size());
    for (WatermarkStateInternal source : sources) {
      reads.add(source.get());
    }

    // But defer actually reading them.
    return new StateContents<Instant>() {
      @Override
      public Instant read() {
        Instant minimum = null;
        for (StateContents<Instant> read : reads) {
          Instant input = read.read();
          if (minimum == null || (input != null && minimum.isAfter(input))) {
            minimum = input;
          }
        }

        // Also, compact the state
        if (minimum != null) {
          clear();
          add(minimum);
        }

        return minimum;
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
}
