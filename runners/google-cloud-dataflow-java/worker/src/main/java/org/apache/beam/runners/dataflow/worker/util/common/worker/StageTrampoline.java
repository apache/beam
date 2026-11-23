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
package org.apache.beam.runners.dataflow.worker.util.common.worker;

import java.util.Arrays;
import java.util.List;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Single-threaded segmented trampoline that bounds fusion call-stack depth to {@code O(1)} on the
 * bundle execution thread.
 *
 * <p>Cuts are ordered topologically ({@code cutIndex = 0 .. numCuts - 1}) so that if cut {@code i}
 * can reach cut {@code j} downstream, {@code i < j}. Draining always selects the highest-index
 * active cut first (depth-first order), yielding back to the outer loop whenever a downstream cut
 * becomes non-empty so segments execute iteratively at constant stack height rather than nesting.
 */
@SuppressWarnings("nullness")
public final class StageTrampoline {
  static final int INITIAL_BUFFER_CAPACITY = 256;
  private static final int SHRINK_THRESHOLD = 4096;

  private final Cut[] cuts;
  private final int numCuts;
  private long activeMask = 0L;
  private final long @Nullable [] extraMasks;
  private int activeCutIndex = -1;

  public StageTrampoline(List<OutputReceiver> cutReceivers) {
    this.numCuts = cutReceivers.size();
    this.cuts = new Cut[numCuts];
    this.extraMasks = numCuts > 64 ? new long[(numCuts + 63) >>> 6] : null;
    for (int i = 0; i < numCuts; i++) {
      OutputReceiver receiver = cutReceivers.get(i);
      Cut cut = new Cut(this, i, receiver);
      this.cuts[i] = cut;
      receiver.setTrampolineCut(cut);
    }
  }

  public int cutCount() {
    return numCuts;
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
      long aboveMask = -1L << (cutIndex + 1);
      return (activeMask & aboveMask) != 0L;
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
  public void drain() throws Exception {
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
    } catch (Throwable t) {
      if (prevActive < 0) {
        reset();
      } else {
        clearCutsFrom(minCutIndex);
      }
      if (t instanceof Exception) {
        throw (Exception) t;
      }
      throw (Error) t;
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

  /** Clears all buffered elements and active masks (invoked on bundle abort/close). */
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

  /** Per-edge FIFO ring buffer sitting immediately upstream of an {@link OutputReceiver}. */
  static final class Cut {
    private final StageTrampoline trampoline;
    private final int cutIndex;
    private final OutputReceiver target;
    private Object[] buffer = new Object[INITIAL_BUFFER_CAPACITY];
    private int mask = INITIAL_BUFFER_CAPACITY - 1;
    private int head = 0;
    private int tail = 0;
    private int size = 0;

    Cut(StageTrampoline trampoline, int cutIndex, OutputReceiver target) {
      this.trampoline = trampoline;
      this.cutIndex = cutIndex;
      this.target = target;
    }

    void enqueue(Object elem) throws Exception {
      if (size == buffer.length) {
        if (cutIndex > trampoline.activeCutIndex) {
          // A segment emitted 256 elements in a single call (e.g. exploding a hot GBK key or
          // 1-to-many FlatMap downstream of an upstream cut): drain strictly downstream cuts first
          // to bound memory to O(256).
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
        Object elem = buffer[h];
        buffer[h] = null;
        head = (h + 1) & mask;
        size--;
        target.processDirect(elem);
        if (size > 0 && trampoline.hasActiveAbove(cutIndex)) {
          // A downstream cut now has elements; yield to the outer trampoline loop so the
          // downstream segment runs at constant stack depth before we advance this cut.
          trampoline.markActive(cutIndex);
          return;
        }
      }
      head = 0;
      tail = 0;
      if (buffer.length > SHRINK_THRESHOLD) {
        buffer = new Object[INITIAL_BUFFER_CAPACITY];
        mask = INITIAL_BUFFER_CAPACITY - 1;
      }
    }

    private void grow() {
      int oldCap = buffer.length;
      int newCap = oldCap << 1;
      if (newCap < 0) {
        throw new IllegalStateException("StageTrampoline cut buffer overflow");
      }
      Object[] next = new Object[newCap];
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
        buffer = new Object[INITIAL_BUFFER_CAPACITY];
        mask = INITIAL_BUFFER_CAPACITY - 1;
      }
    }
  }
}
