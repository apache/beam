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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateInternalsFactory;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.TimerInternalsFactory;
import org.apache.beam.sdk.state.State;
import org.apache.beam.sdk.state.StateContext;
import org.apache.beam.sdk.state.TimeDomain;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/** Provides access to the keyed StateInternals and TimerInternals. */
@ThreadSafe
class KeyedInternals<K> {

  private static class KeyedStates<K> {
    private final K key;
    private final List<State> states;

    private KeyedStates(K key) {
      this.key = key;
      this.states = new ArrayList<>();
    }
  }

  private static final ThreadLocal<KeyedStates> threadLocalKeyedStates = new ThreadLocal<>();
  private final StateInternalsFactory<K> stateFactory;
  private final TimerInternalsFactory<K> timerFactory;

  KeyedInternals(StateInternalsFactory<K> stateFactory, TimerInternalsFactory<K> timerFactory) {
    this.stateFactory = stateFactory;
    this.timerFactory = timerFactory;
  }

  StateInternals stateInternals() {
    return new KeyedStateInternals();
  }

  TimerInternals timerInternals() {
    return new KeyedTimerInternals();
  }

  void setKey(K key) {
    checkState(
        threadLocalKeyedStates.get() == null,
        "States for key %s is not cleared before processing",
        key);

    threadLocalKeyedStates.set(new KeyedStates<K>(key));
  }

  K getKey() {
    KeyedStates<K> keyedStates = threadLocalKeyedStates.get();
    return keyedStates == null ? null : keyedStates.key;
  }

  void clearKey() {
    final List<State> states = threadLocalKeyedStates.get().states;
    states.forEach(
        state -> {
          if (state instanceof SamzaStoreStateInternals.KeyValueIteratorState) {
            ((SamzaStoreStateInternals.KeyValueIteratorState) state).closeIterators();
          }
        });
    states.clear();

    threadLocalKeyedStates.remove();
  }

  private class KeyedStateInternals implements StateInternals {

    @Override
    public K getKey() {
      return KeyedInternals.this.getKey();
    }

    @Override
    public <T extends State> T state(
        StateNamespace namespace, StateTag<T> address, StateContext<?> c) {
      checkState(getKey() != null, "Key is not set before state access in Stateful ParDo.");

      final T state = stateFactory.stateInternalsForKey(getKey()).state(namespace, address, c);
      threadLocalKeyedStates.get().states.add(state);
      return state;
    }
  }

  private class KeyedTimerInternals implements TimerInternals {

    private TimerInternals getInternals() {
      return timerFactory.timerInternalsForKey(getKey());
    }

    @Override
    public void setTimer(
        StateNamespace namespace,
        String timerId,
        String timerFamilyId,
        Instant target,
        Instant outputTimestamp,
        TimeDomain timeDomain) {
      getInternals()
          .setTimer(namespace, timerId, timerFamilyId, target, outputTimestamp, timeDomain);
    }

    @Override
    public void setTimer(TimerData timerData) {
      getInternals().setTimer(timerData);
    }

    @Override
    public void deleteTimer(StateNamespace namespace, String timerId, TimeDomain timeDomain) {
      getInternals().deleteTimer(namespace, timerId, timeDomain);
    }

    @Override
    public void deleteTimer(StateNamespace namespace, String timerId, String timerFamilyId) {
      getInternals().deleteTimer(namespace, timerId, timerFamilyId);
    }

    @Override
    public void deleteTimer(TimerData timerKey) {
      getInternals().deleteTimer(timerKey);
    }

    @Override
    public Instant currentProcessingTime() {
      return getInternals().currentProcessingTime();
    }

    @Nullable
    @Override
    public Instant currentSynchronizedProcessingTime() {
      return getInternals().currentSynchronizedProcessingTime();
    }

    @Override
    public Instant currentInputWatermarkTime() {
      return getInternals().currentInputWatermarkTime();
    }

    @Nullable
    @Override
    public Instant currentOutputWatermarkTime() {
      return getInternals().currentOutputWatermarkTime();
    }
  }
}
