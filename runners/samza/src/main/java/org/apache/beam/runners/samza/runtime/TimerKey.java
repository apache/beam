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

import org.apache.beam.runners.core.StateNamespace;

/** Timer key which is used to register and delete timers. */
class TimerKey<K> {
  private final K key;
  private final byte[] keyBytes;
  private final StateNamespace stateNamespace;
  private final String timerId;

  TimerKey(K key, byte[] keyBytes, StateNamespace stateNamespace, String timerId) {
    this.key = key;
    this.keyBytes = keyBytes;
    this.stateNamespace = stateNamespace;
    this.timerId = timerId;
  }

  public K getKey() {
    return key;
  }

  public byte[] getKeyBytes() {
    return keyBytes;
  }

  public StateNamespace getStateNamespace() {
    return stateNamespace;
  }

  public String getTimerId() {
    return timerId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    TimerKey<?> timerKey = (TimerKey<?>) o;

    if (key != null ? !key.equals(timerKey.key) : timerKey.key != null) {
      return false;
    }
    if (!stateNamespace.equals(timerKey.stateNamespace)) {
      return false;
    }

    return timerId.equals(timerKey.timerId);
  }

  @Override
  public int hashCode() {
    int result = key != null ? key.hashCode() : 0;
    result = 31 * result + stateNamespace.hashCode();
    result = 31 * result + timerId.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "TimerKey{"
        + "key="
        + key
        + ", stateNamespace="
        + stateNamespace
        + ", timerId='"
        + timerId
        + '\''
        + '}';
  }
}
