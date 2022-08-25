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

import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Instant;

/** This class wraps a DoFnRunner with keyed StateInternals and TimerInternals access. */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class DoFnRunnerWithKeyedInternals<InputT, OutputT> implements DoFnRunner<InputT, OutputT> {
  private final DoFnRunner<InputT, OutputT> underlying;
  private final KeyedInternals keyedInternals;

  DoFnRunnerWithKeyedInternals(
      DoFnRunner<InputT, OutputT> doFnRunner, KeyedInternals keyedInternals) {
    this.underlying = doFnRunner;
    this.keyedInternals = keyedInternals;
  }

  @Override
  public void startBundle() {
    underlying.startBundle();
  }

  @Override
  public void processElement(WindowedValue<InputT> elem) {
    // NOTE: this is thread-safe if we only allow concurrency on the per-key basis.
    setKeyedInternals(elem.getValue());

    try {
      underlying.processElement(elem);
    } finally {
      clearKeyedInternals();
    }
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
    // Note: wrap with KV.of(key, null) as a special use case of setKeyedInternals() to set key
    // directly.
    setKeyedInternals(KV.of(key, null));

    try {
      underlying.onTimer(
          timerId, timerFamilyId, key, window, timestamp, outputTimestamp, timeDomain);
    } finally {
      clearKeyedInternals();
    }
  }

  @Override
  public void finishBundle() {
    underlying.finishBundle();
  }

  @Override
  public <KeyT> void onWindowExpiration(BoundedWindow window, Instant timestamp, KeyT key) {
    underlying.onWindowExpiration(window, timestamp, key);
  }

  @Override
  public DoFn<InputT, OutputT> getFn() {
    return underlying.getFn();
  }

  private void setKeyedInternals(Object value) {
    if (value instanceof KeyedWorkItem) {
      keyedInternals.setKey(((KeyedWorkItem<?, ?>) value).key());
    } else if (value instanceof KeyedTimerData) {
      final Object key = ((KeyedTimerData) value).getKey();
      if (key != null) {
        keyedInternals.setKey(key);
      }
    } else if (value instanceof KV) {
      keyedInternals.setKey(((KV<?, ?>) value).getKey());
    } else {
      throw new UnsupportedOperationException(
          String.format(
              "%s is not supported in %s", value.getClass(), DoFnRunnerWithKeyedInternals.class));
    }
  }

  private void clearKeyedInternals() {
    keyedInternals.clearKey();
  }
}
