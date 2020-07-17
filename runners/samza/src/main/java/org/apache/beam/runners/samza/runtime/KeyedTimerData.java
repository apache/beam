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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * {@link TimerInternals.TimerData} with key, used by {@link SamzaTimerInternalsFactory}. Implements
 * {@link Comparable} by first comparing the wrapped TimerData then the key.
 */
public class KeyedTimerData<K> implements Comparable<KeyedTimerData<K>> {
  private final byte[] keyBytes;
  private final K key;
  private final TimerInternals.TimerData timerData;

  public KeyedTimerData(byte[] keyBytes, K key, TimerData timerData) {
    this.keyBytes = keyBytes;
    this.key = key;
    this.timerData = timerData;
  }

  public K getKey() {
    return key;
  }

  public byte[] getKeyBytes() {
    return keyBytes;
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
  public String toString() {
    return "KeyedTimerData{"
        + "key="
        + key
        + ", keyBytes="
        + Arrays.toString(keyBytes)
        + ", timerData="
        + timerData
        + '}';
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final KeyedTimerData<?> that = (KeyedTimerData<?>) o;

    return Arrays.equals(keyBytes, that.keyBytes) && timerData.equals(that.timerData);
  }

  @Override
  public int hashCode() {
    int result = Arrays.hashCode(keyBytes);
    result = 31 * result + timerData.hashCode();
    return result;
  }

  /**
   * Coder for {@link KeyedTimerData}. Note we don't use the {@link TimerInternals.TimerDataCoderV2}
   * here directly since we want to en/decode timestamp first so the timers will be sorted in the
   * state.
   */
  public static class KeyedTimerDataCoder<K> extends StructuredCoder<KeyedTimerData<K>> {
    private static final StringUtf8Coder STRING_CODER = StringUtf8Coder.of();
    private static final InstantCoder INSTANT_CODER = InstantCoder.of();

    private final Coder<K> keyCoder;
    private final Coder<? extends BoundedWindow> windowCoder;

    KeyedTimerDataCoder(Coder<K> keyCoder, Coder<? extends BoundedWindow> windowCoder) {
      this.keyCoder = keyCoder;
      this.windowCoder = windowCoder;
    }

    @Override
    public void encode(KeyedTimerData<K> value, OutputStream outStream)
        throws CoderException, IOException {

      final TimerData timer = value.getTimerData();
      // encode the timestamp first
      INSTANT_CODER.encode(timer.getTimestamp(), outStream);
      STRING_CODER.encode(timer.getTimerId(), outStream);
      STRING_CODER.encode(timer.getNamespace().stringKey(), outStream);
      STRING_CODER.encode(timer.getDomain().name(), outStream);

      if (keyCoder != null) {
        keyCoder.encode(value.key, outStream);
      }
    }

    @Override
    public KeyedTimerData<K> decode(InputStream inStream) throws CoderException, IOException {
      // decode the timestamp first
      final Instant timestamp = INSTANT_CODER.decode(inStream);
      final String timerId = STRING_CODER.decode(inStream);
      final StateNamespace namespace =
          StateNamespaces.fromString(STRING_CODER.decode(inStream), windowCoder);
      final TimeDomain domain = TimeDomain.valueOf(STRING_CODER.decode(inStream));
      final TimerData timer = TimerData.of(timerId, namespace, timestamp, timestamp, domain);

      byte[] keyBytes = null;
      K key = null;
      if (keyCoder != null) {
        key = keyCoder.decode(inStream);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
          keyCoder.encode(key, baos);
        } catch (IOException e) {
          throw new RuntimeException("Could not encode key: " + key, e);
        }
        keyBytes = baos.toByteArray();
      }

      return new KeyedTimerData(keyBytes, key, timer);
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return Arrays.asList(keyCoder, windowCoder);
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {}
  }
}
