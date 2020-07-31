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
package org.apache.beam.sdk.transforms.windowing;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.annotations.Internal;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * <b>For internal use only; no backwards compatibility guarantees.</b>
 *
 * <p>A utility function for merging overlapping {@link IntervalWindow IntervalWindows}.
 */
@Internal
public class MergeOverlappingIntervalWindows {

  /** Merge overlapping {@link IntervalWindow}s. */
  public static void mergeWindows(WindowFn<?, IntervalWindow>.MergeContext c) throws Exception {
    // Merge any overlapping windows into a single window.
    // Sort the list of existing windows so we only have to
    // traverse the list once rather than considering all
    // O(n^2) window pairs.
    List<IntervalWindow> sortedWindows = new ArrayList<>();
    for (IntervalWindow window : c.windows()) {
      sortedWindows.add(window);
    }
    Collections.sort(sortedWindows);
    List<MergeCandidate> merges = new ArrayList<>();
    MergeCandidate current = new MergeCandidate();
    for (IntervalWindow window : sortedWindows) {
      if (current.intersects(window)) {
        current.add(window);
      } else {
        merges.add(current);
        current = new MergeCandidate(window);
      }
    }
    merges.add(current);
    for (MergeCandidate merge : merges) {
      merge.apply(c);
    }
  }

  private static class MergeCandidate {
    private @Nullable IntervalWindow union;
    private final List<IntervalWindow> parts;

    public MergeCandidate() {
      union = null;
      parts = new ArrayList<>();
    }

    public MergeCandidate(IntervalWindow window) {
      union = window;
      parts = new ArrayList<>(Arrays.asList(window));
    }

    public boolean intersects(IntervalWindow window) {
      return union == null || union.intersects(window);
    }

    public void add(IntervalWindow window) {
      union = union == null ? window : union.span(window);
      parts.add(window);
    }

    public void apply(WindowFn<?, IntervalWindow>.MergeContext c) throws Exception {
      if (parts.size() > 1) {
        c.merge(parts, union);
      }
    }

    @Override
    public String toString() {
      return "MergeCandidate[union=" + union + ", parts=" + parts + "]";
    }
  }
}
