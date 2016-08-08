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
package org.apache.beam.sdk.util;

import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PCollectionView;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * A {@link DoFnRunner} that can refuse to process elements that are not ready, instead returning
 * them via the {@link #processElementInReadyWindows(WindowedValue)}.
 */
public class PushbackSideInputDoFnRunner<InputT, OutputT> implements DoFnRunner<InputT, OutputT> {
  private final DoFnRunner<InputT, OutputT> underlying;
  private final Collection<PCollectionView<?>> views;
  private final ReadyCheckingSideInputReader sideInputReader;

  private Set<BoundedWindow> notReadyWindows;

  public static <InputT, OutputT> PushbackSideInputDoFnRunner<InputT, OutputT> create(
      DoFnRunner<InputT, OutputT> underlying,
      Collection<PCollectionView<?>> views,
      ReadyCheckingSideInputReader sideInputReader) {
    return new PushbackSideInputDoFnRunner<>(underlying, views, sideInputReader);
  }

  private PushbackSideInputDoFnRunner(
      DoFnRunner<InputT, OutputT> underlying,
      Collection<PCollectionView<?>> views,
      ReadyCheckingSideInputReader sideInputReader) {
    this.underlying = underlying;
    this.views = views;
    this.sideInputReader = sideInputReader;
  }

  @Override
  public void startBundle() {
    notReadyWindows = new HashSet<>();
    underlying.startBundle();
  }

  /**
   * Call the underlying {@link DoFnRunner#processElement(WindowedValue)} for the provided element
   * for each window the element is in that is ready.
   *
   * @param elem the element to process in all ready windows
   * @return each element that could not be processed because it requires a side input window
   * that is not ready.
   */
  public Iterable<WindowedValue<InputT>> processElementInReadyWindows(WindowedValue<InputT> elem) {
    if (views.isEmpty()) {
      processElement(elem);
      return Collections.emptyList();
    }
    ImmutableList.Builder<WindowedValue<InputT>> pushedBack = ImmutableList.builder();
    for (WindowedValue<InputT> windowElem : elem.explodeWindows()) {
      BoundedWindow mainInputWindow = Iterables.getOnlyElement(windowElem.getWindows());
      boolean isReady = !notReadyWindows.contains(mainInputWindow);
      for (PCollectionView<?> view : views) {
        BoundedWindow sideInputWindow =
            view.getWindowingStrategyInternal()
                .getWindowFn()
                .getSideInputWindow(mainInputWindow);
        if (!sideInputReader.isReady(view, sideInputWindow)) {
          isReady = false;
          break;
        }
      }
      if (isReady) {
        processElement(windowElem);
      } else {
        notReadyWindows.add(mainInputWindow);
        pushedBack.add(windowElem);
      }
    }
    return pushedBack.build();
  }

  @Override
  public void processElement(WindowedValue<InputT> elem) {
    underlying.processElement(elem);
  }

  /**
   * Call the underlying {@link DoFnRunner#finishBundle()}.
   */
  @Override
  public void finishBundle() {
    notReadyWindows = null;
    underlying.finishBundle();
  }
}

