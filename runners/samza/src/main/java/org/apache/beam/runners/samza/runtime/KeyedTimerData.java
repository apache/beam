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

import java.util.Arrays;
import org.apache.beam.runners.core.TimerInternals;

/**
 * {@link TimerInternals.TimerData} with key, used by {@link SamzaTimerInternalsFactory}.
 * Implements {@link Comparable} by first comparing the wrapped TimerData then the key.
 */
public class KeyedTimerData<K> implements Comparable<KeyedTimerData<K>> {
  private final byte[] keyBytes;
  private final K key;
  private final TimerInternals.TimerData timerData;

  public KeyedTimerData(byte[] keyBytes, K key, TimerInternals.TimerData timerData) {
    this.keyBytes = keyBytes;
    this.key = key;
    this.timerData = timerData;
  }

  public K getKey() {
    return key;
  }

  public TimerInternals.TimerData getTimerData() {
    return timerData;
  }


  @Override
  public int compareTo(KeyedTimerData<K> other) {
    final int timerCompare = getTimerData().compareTo(other.getTimerData());
    if (timerCompare != 0) {
      return timerCompare;
    }

    if (keyBytes == null) {
      return other.keyBytes == null ? 0 : -1;
    }

    if (other.keyBytes == null) {
      return 1;
    }

    if (keyBytes.length < other.keyBytes.length) {
      return -1;
    }

    if (keyBytes.length > other.keyBytes.length) {
      return 1;
    }

    for (int i = 0; i < keyBytes.length; ++i) {
      final char b1 = (char) keyBytes[i];
      final char b2 = (char) other.keyBytes[i];
      if (b1 != b2) {
        return b1 - b2;
      }
    }

    return 0;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final KeyedTimerData<?> that = (KeyedTimerData<?>) o;

    return Arrays.equals(keyBytes, that.keyBytes)
        && timerData.equals(that.timerData);
  }

  @Override
  public int hashCode() {
    int result = Arrays.hashCode(keyBytes);
    result = 31 * result + timerData.hashCode();
    return result;
  }
}
