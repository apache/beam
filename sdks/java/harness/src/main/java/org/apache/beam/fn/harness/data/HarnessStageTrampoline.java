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
package org.apache.beam.fn.harness.data;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.values.WindowedValue;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Single-threaded segmented trampoline for the Portable Java SDK Harness that bounds fused
 * PTransform call-stack depth to {@code O(1)} on the bundle processing thread.
 *
 * <p>Cuts sit immediately upstream of {@link PCollectionConsumerRegistry}'s metric-tracking
 * receivers so {@code SampleByteSizeDistribution.tryUpdate -> consumer -> finishLazyUpdate}
 * executes atomically during {@link #drain()}, and downstream user exceptions are wrapped via
 * {@link UserCodeException#wrap(Throwable)} to preserve stack-trace truncation.
 */
@SuppressWarnings({"rawtypes", "unchecked", "nullness"})
public final class HarnessStageTrampoline {
  public static final int DEFAULT_TRAMPOLINE_FUSION_THRESHOLD = 72;
  private static final String TRAMPOLINE_THRESHOLD_PREFIX = "trampoline_fusion_threshold=";
  private static final String DISABLE_TRAMPOLINE_EXPERIMENT = "disable_fusion_trampoline";
  private static final int INITIAL_BUFFER_CAPACITY = 256;
  private static final int SHRINK_THRESHOLD = 4096;

  private final List<CutReceiver<?>> postOrderCuts = new ArrayList<>();
  private CutReceiver<?>[] cuts = new CutReceiver<?>[0];
  private int numCuts = 0;
  private long activeMask = 0L;
  private long @Nullable [] extraMasks = null;
  private int activeCutIndex = -1;

  public static int resolveThreshold(@Nullable PipelineOptions options) {
    if (options == null) {
      return DEFAULT_TRAMPOLINE_FUSION_THRESHOLD;
    }
    List<String> experiments = options.as(ExperimentalOptions.class).getExperiments();
    if (experiments != null) {
      if (experiments.contains(DISABLE_TRAMPOLINE_EXPERIMENT)) {
        return 0;
      }
      for (String exp : experiments) {
        if (exp != null && exp.startsWith(TRAMPOLINE_THRESHOLD_PREFIX)) {
          return Integer.parseInt(exp.substring(TRAMPOLINE_THRESHOLD_PREFIX.length()));
        }
      }
    }
    return DEFAULT_TRAMPOLINE_FUSION_THRESHOLD;
  }

  /**
   * Registers a trampoline cut in post-order (sink-to-root) during bundle processor construction.
   * Call {@link #freezeReverseTopologicalCuts()} once all transforms are wired.
   */
  public <T> FnDataReceiver<WindowedValue<T>> createCut(
      FnDataReceiver<WindowedValue<T>> downstream) {
    CutReceiver<T> cut = new CutReceiver<>(this, downstream);
    postOrderCuts.add(cut);
    return cut;
  }

  /** Assigns final topological indices ({@code 0 .. numCuts - 1}) from post-order registration. */
  public void freezeReverseTopologicalCuts() {
    List<CutReceiver<?>> ordered = new ArrayList<>(postOrderCuts);
    Collections.reverse(ordered);
    this.numCuts = ordered.size();
    this.cuts = ordered.toArray(new CutReceiver<?>[0]);
    this.extraMasks = numCuts > 64 ? new long[(numCuts + 63) >>> 6] : null;
    for (int i = 0; i < numCuts; i++) {
      this.cuts[i].cutIndex = i;
    }
  }

  public int cutCount() {
    return numCuts > 0 ? numCuts : postOrderCuts.size();
  }

  void markActive(int cutIndex) {
    if (extraMasks == null) {
      activeMask |= (1L << cutIndex);
    } else {
      extraMasks[cutIndex >>> 6] |= (1L << (cutIndex & 63));
    }
  }

  private void clearActive(int cutIndex) {
    if (extraMasks == null) {
      activeMask &= ~(1L << cutIndex);
    } else {
      extraMasks[cutIndex >>> 6] &= ~(1L << (cutIndex & 63));
    }
  }

  boolean hasActiveAbove(int cutIndex) {
    if (extraMasks == null) {
      if (cutIndex >= 63) {
        return false;
      }
      return (activeMask & (-1L << (cutIndex + 1))) != 0L;
    }
    int wordIdx = (cutIndex + 1) >>> 6;
    if (wordIdx >= extraMasks.length) {
      return false;
    }
    int bitOffset = (cutIndex + 1) & 63;
    if ((extraMasks[wordIdx] & (-1L << bitOffset)) != 0L) {
      return true;
    }
    for (int w = wordIdx + 1; w < extraMasks.length; w++) {
      if (extraMasks[w] != 0L) {
        return true;
      }
    }
    return false;
  }

  private int highestActiveCut() {
    if (extraMasks == null) {
      long m = activeMask;
      return m == 0L ? -1 : 63 - Long.numberOfLeadingZeros(m);
    }
    for (int w = extraMasks.length - 1; w >= 0; w--) {
      long m = extraMasks[w];
      if (m != 0L) {
        return (w << 6) + (63 - Long.numberOfLeadingZeros(m));
      }
    }
    return -1;
  }

  public boolean hasPending() {
    if (extraMasks == null) {
      return activeMask != 0L;
    }
    for (long m : extraMasks) {
      if (m != 0L) {
        return true;
      }
    }
    return false;
  }

  private int highestActiveCutAtOrAbove(int minCutIndex) {
    int idx = highestActiveCut();
    return idx >= minCutIndex ? idx : -1;
  }

  /**
   * Drains all buffered elements across all topological cuts strictly downstream of the currently
   * executing cut (or all cuts when called at the top level) in iterative depth-first order.
   */
  public void drain() {
    int minCutIndex = activeCutIndex + 1;
    int idx = highestActiveCutAtOrAbove(minCutIndex);
    if (idx < 0) {
      return;
    }
    int prevActive = activeCutIndex;
    try {
      do {
        clearActive(idx);
        activeCutIndex = idx;
        cuts[idx].drainSegment();
      } while ((idx = highestActiveCutAtOrAbove(minCutIndex)) >= 0);
    } catch (OutOfMemoryError oom) {
      if (prevActive < 0) {
        reset();
      } else {
        clearCutsFrom(minCutIndex);
      }
      throw oom;
    } catch (Throwable t) {
      if (prevActive < 0) {
        reset();
      } else {
        clearCutsFrom(minCutIndex);
      }
      throw UserCodeException.wrap(t);
    } finally {
      activeCutIndex = prevActive;
    }
  }

  private void clearCutsFrom(int minCutIndex) {
    for (int i = minCutIndex; i < numCuts; i++) {
      clearActive(i);
      cuts[i].clear();
    }
  }

  /** Resets all cut queues and active bitmasks (invoked on bundle reset/failure). */
  public void reset() {
    activeCutIndex = -1;
    activeMask = 0L;
    if (extraMasks != null) {
      Arrays.fill(extraMasks, 0L);
    }
    for (int i = 0; i < numCuts; i++) {
      cuts[i].clear();
    }
  }

  private static final class CutReceiver<T> implements FnDataReceiver<WindowedValue<T>> {
    private final HarnessStageTrampoline trampoline;
    private final FnDataReceiver<WindowedValue<T>> downstream;
    int cutIndex = 0;
    private WindowedValue<T>[] buffer = new WindowedValue[INITIAL_BUFFER_CAPACITY];
    private int mask = INITIAL_BUFFER_CAPACITY - 1;
    private int head = 0;
    private int tail = 0;
    private int size = 0;

    CutReceiver(HarnessStageTrampoline trampoline, FnDataReceiver<WindowedValue<T>> downstream) {
      this.trampoline = trampoline;
      this.downstream = downstream;
    }

    @Override
    public void accept(WindowedValue<T> elem) {
      if (size == buffer.length) {
        if (cutIndex > trampoline.activeCutIndex) {
          trampoline.drain();
        }
        if (size == buffer.length) {
          grow();
        }
      }
      buffer[tail] = elem;
      tail = (tail + 1) & mask;
      int s = size + 1;
      size = s;
      trampoline.markActive(cutIndex);
      if (s == INITIAL_BUFFER_CAPACITY && cutIndex > trampoline.activeCutIndex) {
        trampoline.drain();
      }
    }

    void drainSegment() throws Exception {
      while (size > 0) {
        int h = head;
        WindowedValue<T> elem = buffer[h];
        buffer[h] = null;
        head = (h + 1) & mask;
        size--;
        downstream.accept(elem);
        if (size > 0 && trampoline.hasActiveAbove(cutIndex)) {
          trampoline.markActive(cutIndex);
          return;
        }
      }
      head = 0;
      tail = 0;
      if (buffer.length > SHRINK_THRESHOLD) {
        buffer = new WindowedValue[INITIAL_BUFFER_CAPACITY];
        mask = INITIAL_BUFFER_CAPACITY - 1;
      }
    }

    private void grow() {
      int oldCap = buffer.length;
      int newCap = oldCap << 1;
      if (newCap < 0) {
        throw new IllegalStateException("HarnessStageTrampoline cut buffer overflow");
      }
      WindowedValue<T>[] next = new WindowedValue[newCap];
      int firstPart = oldCap - head;
      System.arraycopy(buffer, head, next, 0, firstPart);
      if (head > 0) {
        System.arraycopy(buffer, 0, next, firstPart, head);
      }
      buffer = next;
      mask = newCap - 1;
      head = 0;
      tail = size;
    }

    void clear() {
      if (size > 0) {
        Arrays.fill(buffer, null);
        size = 0;
      }
      head = 0;
      tail = 0;
      if (buffer.length > SHRINK_THRESHOLD) {
        buffer = new WindowedValue[INITIAL_BUFFER_CAPACITY];
        mask = INITIAL_BUFFER_CAPACITY - 1;
      }
    }
  }
}
