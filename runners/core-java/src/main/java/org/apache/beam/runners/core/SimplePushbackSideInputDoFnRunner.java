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
package org.apache.beam.runners.core;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * A {@link DoFnRunner} that can refuse to process elements that are not ready, instead returning
 * them via the {@link #processElementInReadyWindows(WindowedValue)}.
 */
public class SimplePushbackSideInputDoFnRunner<InputT, OutputT>
    implements PushbackSideInputDoFnRunner<InputT, OutputT> {
  private final DoFnRunner<InputT, OutputT> underlying;
  private final Collection<PCollectionView<?>> views;
  private final ReadyCheckingSideInputReader sideInputReader;

  // Initialized in startBundle()
  private @Nullable Set<BoundedWindow> notReadyWindows;

  public static <InputT, OutputT> SimplePushbackSideInputDoFnRunner<InputT, OutputT> create(
      DoFnRunner<InputT, OutputT> underlying,
      Collection<PCollectionView<?>> views,
      ReadyCheckingSideInputReader sideInputReader) {
    return new SimplePushbackSideInputDoFnRunner<>(underlying, views, sideInputReader);
  }

  private SimplePushbackSideInputDoFnRunner(
      DoFnRunner<InputT, OutputT> underlying,
      Collection<PCollectionView<?>> views,
      ReadyCheckingSideInputReader sideInputReader) {
    this.underlying = underlying;
    this.views = views;
    this.sideInputReader = sideInputReader;
  }

  @Override
  public DoFn<InputT, OutputT> getFn() {
    return underlying.getFn();
  }

  @Override
  public void startBundle() {
    notReadyWindows = new HashSet<>();
    underlying.startBundle();
  }

  @Override
  public Iterable<WindowedValue<InputT>> processElementInReadyWindows(WindowedValue<InputT> elem) {
    if (views.isEmpty()) {
      // When there are no side inputs, we can preserve the compressed representation.
      underlying.processElement(elem);
      return Collections.emptyList();
    }
    ImmutableList.Builder<WindowedValue<InputT>> pushedBack = ImmutableList.builder();
    for (WindowedValue<InputT> windowElem : elem.explodeWindows()) {
      BoundedWindow mainInputWindow = Iterables.getOnlyElement(windowElem.getWindows());
      if (isReady(mainInputWindow)) {
        // When there are any side inputs, we have to process the element in each window
        // individually, to disambiguate access to per-window side inputs.
        underlying.processElement(windowElem);
      } else {
        notReadyWindows.add(mainInputWindow);
        pushedBack.add(windowElem);
      }
    }
    return pushedBack.build();
  }

  private boolean isReady(BoundedWindow mainInputWindow) {
    if (notReadyWindows.contains(mainInputWindow)) {
      return false;
    }
    for (PCollectionView<?> view : views) {
      BoundedWindow sideInputWindow = view.getWindowMappingFn().getSideInputWindow(mainInputWindow);
      if (!sideInputReader.isReady(view, sideInputWindow)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public <KeyT> void onTimer(
      String timerId,
      String timerFamilyId,
      KeyT key,
      BoundedWindow window,
      Instant timestamp,
      Instant outputTimestamp,
      TimeDomain timeDomain) {
    underlying.onTimer(timerId, timerFamilyId, key, window, timestamp, outputTimestamp, timeDomain);
  }

  @Override
  public void finishBundle() {
    notReadyWindows = null;
    underlying.finishBundle();
  }
}
