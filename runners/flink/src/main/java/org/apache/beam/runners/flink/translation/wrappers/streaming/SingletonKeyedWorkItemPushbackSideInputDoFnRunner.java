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
package org.apache.beam.runners.flink.translation.wrappers.streaming;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.PushbackSideInputDoFnRunner;
import org.apache.beam.runners.core.ReadyCheckingSideInputReader;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

@SuppressWarnings({"nullness", "keyfor"}) // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
public class SingletonKeyedWorkItemPushbackSideInputDoFnRunner<KeyT, InputT, OutputT>
    implements PushbackSideInputDoFnRunner<KeyedWorkItem<KeyT, InputT>, KV<KeyT, OutputT>> {

  private final DoFnRunner<KeyedWorkItem<KeyT, InputT>, KV<KeyT, OutputT>> underlying;
  private final Collection<PCollectionView<?>> views;
  private final ReadyCheckingSideInputReader sideInputReader;

  private @Nullable Set<BoundedWindow> notReadyWindows = new HashSet<>();

  public SingletonKeyedWorkItemPushbackSideInputDoFnRunner(
      DoFnRunner<KeyedWorkItem<KeyT, InputT>, KV<KeyT, OutputT>> underlying,
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

  @Override
  public Iterable<WindowedValue<KeyedWorkItem<KeyT, InputT>>> processElementInReadyWindows(
      WindowedValue<KeyedWorkItem<KeyT, InputT>> elem) {
    if (views.isEmpty()) {
      underlying.processElement(elem);
      return Collections.emptyList();
    }
    final SingletonKeyedWorkItem<KeyT, InputT> kwi =
        (SingletonKeyedWorkItem<KeyT, InputT>) elem.getValue();
    Preconditions.checkState(
        Iterables.isEmpty(kwi.timersIterable()), "Timers are not supported in side inputs.");
    final List<WindowedValue<KeyedWorkItem<KeyT, InputT>>> pushedBack = new ArrayList<>();
    for (WindowedValue<InputT> windowedValue : kwi.value().explodeWindows()) {
      final WindowedValue<KeyedWorkItem<KeyT, InputT>> outputValue =
          windowedValue.withValue(new SingletonKeyedWorkItem<>(kwi.key(), windowedValue));
      final BoundedWindow mainInputWindow = Iterables.getOnlyElement(windowedValue.getWindows());
      if (isReady(mainInputWindow)) {
        underlying.processElement(outputValue);
      } else {
        Objects.requireNonNull(notReadyWindows).add(mainInputWindow);
        pushedBack.add(outputValue);
      }
    }
    if (pushedBack.isEmpty()) {
      return Collections.emptyList();
    }
    return Collections.unmodifiableList(pushedBack);
  }

  @Override
  public <TimerKeyT> void onTimer(
      String timerId,
      String timerFamilyId,
      TimerKeyT key,
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

  @Override
  public DoFn<KeyedWorkItem<KeyT, InputT>, KV<KeyT, OutputT>> getFn() {
    return underlying.getFn();
  }

  private boolean isReady(BoundedWindow mainInputWindow) {
    if (Objects.requireNonNull(notReadyWindows).contains(mainInputWindow)) {
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
}
