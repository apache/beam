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
package org.apache.beam.runners.core.construction;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.coders.BooleanCoder;
import org.apache.beam.sdk.coders.CollectionCoder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.PaneInfo.PaneInfoCoder;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * A timer consists of a user key, a dynamic timer tag, a set of windows and either a bit that says
 * that this timer should be cleared or data representing the firing timestamp, hold timestamp and
 * pane information that should be used when producing output.
 *
 * <p>Note that this is an implementation helper specifically intended for use during execution by
 * runners and the Java SDK harness. The API for pipeline authors is {@link
 * org.apache.beam.sdk.state.Timer}.
 */
@AutoValue
public abstract class Timer<K> {

  /**
   * Returns a non-cleared timer for the given {@code userKey}, {@code dynamicTimerTag}, {@code
   * fireTimestamp}, {@code holdTimestamp}, {@code windows} and {@code pane}.
   */
  public static <K> Timer<K> of(
      K userKey,
      String dynamicTimerTag,
      Collection<? extends BoundedWindow> windows,
      Instant fireTimestamp,
      Instant holdTimestamp,
      PaneInfo pane) {
    return new AutoValue_Timer(
        userKey, dynamicTimerTag, windows, false, fireTimestamp, holdTimestamp, pane);
  }

  /**
   * Returns a cleared timer for the given {@code userKey}, {@code dynamicTimerTag} and {@code
   * windows}.
   */
  public static <K> Timer<K> cleared(
      K userKey, String dynamicTimerTag, Collection<? extends BoundedWindow> windows) {
    return new AutoValue_Timer(userKey, dynamicTimerTag, windows, true, null, null, null);
  }

  /** Returns the key that the timer is set on. */
  public abstract @Nullable K getUserKey();

  /**
   * Returns the tag that the timer is set on. The tag is {@code ""} when the timer is for a {@link
   * TimerSpec}.
   */
  public abstract String getDynamicTimerTag();

  /** Returns the windows which are associated with the timer. */
  public abstract Collection<? extends BoundedWindow> getWindows();

  /** Returns whether the timer is going to be cleared. */
  public abstract boolean getClearBit();

  /**
   * Returns the timestamp of when the timer is scheduled to fire. This field is nullable only when
   * the timer is being cleared.
   *
   * <p>The time is absolute to the time domain defined in the {@link
   * org.apache.beam.model.pipeline.v1.RunnerApi.TimerFamilySpec} that is associated with this
   * timer.
   */
  public abstract @Nullable Instant getFireTimestamp();

  /**
   * Returns the watermark that the timer is supposed to be held. This field is nullable only when
   * the timer is being cleared.
   */
  public abstract @Nullable Instant getHoldTimestamp();

  /**
   * Returns the {@link PaneInfo} that is related to the timer. This field is nullable only when the
   * timer is being cleared.
   */
  public abstract @Nullable PaneInfo getPane();

  @Override
  public boolean equals(@Nullable Object other) {
    if (!(other instanceof Timer)) {
      return false;
    }
    Timer<?> that = (Timer<?>) other;
    return Objects.equals(this.getUserKey(), that.getUserKey())
        && Objects.equals(this.getDynamicTimerTag(), that.getDynamicTimerTag())
        && Objects.equals(this.getWindows(), that.getWindows())
        && (this.getClearBit() == that.getClearBit())
        && Objects.equals(this.getFireTimestamp(), that.getFireTimestamp())
        && Objects.equals(this.getHoldTimestamp(), that.getHoldTimestamp())
        && Objects.equals(this.getPane(), that.getPane());
  }

  @Override
  public int hashCode() {
    // Hash only the millis of the timestamp to be consistent with equals
    if (getClearBit()) {
      return Objects.hash(getUserKey(), getDynamicTimerTag(), getClearBit(), getWindows());
    }
    return Objects.hash(
        getUserKey(),
        getDynamicTimerTag(),
        getClearBit(),
        getFireTimestamp().getMillis(),
        getHoldTimestamp().getMillis(),
        getWindows(),
        getPane());
  }

  /**
   * A {@link org.apache.beam.sdk.coders.Coder} for timers.
   *
   * <p>This coder is deterministic if both the key coder and window coder are deterministic.
   *
   * <p>This coder is inexpensive for size estimation of elements if the key coder and window coder
   * are inexpensive for size estimation.
   */
  public static class Coder<K> extends StructuredCoder<Timer<K>> {

    public static <K> Coder<K> of(
        org.apache.beam.sdk.coders.Coder<K> keyCoder,
        org.apache.beam.sdk.coders.Coder<? extends BoundedWindow> windowCoder) {
      return new Coder<>(keyCoder, windowCoder);
    }

    private final org.apache.beam.sdk.coders.Coder<K> keyCoder;
    private final org.apache.beam.sdk.coders.Coder<Collection<? extends BoundedWindow>>
        windowsCoder;
    private final org.apache.beam.sdk.coders.Coder<? extends BoundedWindow> windowCoder;

    private Coder(
        org.apache.beam.sdk.coders.Coder<K> keyCoder,
        org.apache.beam.sdk.coders.Coder<? extends BoundedWindow> windowCoder) {
      this.windowCoder = windowCoder;
      this.keyCoder = keyCoder;
      this.windowsCoder = (org.apache.beam.sdk.coders.Coder) CollectionCoder.of(windowCoder);
    }

    @Override
    public void encode(Timer<K> timer, OutputStream outStream) throws IOException {
      keyCoder.encode(timer.getUserKey(), outStream);
      StringUtf8Coder.of().encode(timer.getDynamicTimerTag(), outStream);
      windowsCoder.encode(timer.getWindows(), outStream);
      BooleanCoder.of().encode(timer.getClearBit(), outStream);
      if (!timer.getClearBit()) {
        InstantCoder.of().encode(timer.getFireTimestamp(), outStream);
        InstantCoder.of().encode(timer.getHoldTimestamp(), outStream);
        PaneInfoCoder.INSTANCE.encode(timer.getPane(), outStream);
      }
    }

    @Override
    public Timer<K> decode(InputStream inStream) throws IOException {
      K userKey = keyCoder.decode(inStream);
      String dynamicTimerTag = StringUtf8Coder.of().decode(inStream);
      Collection<? extends BoundedWindow> windows = windowsCoder.decode(inStream);
      boolean clearBit = BooleanCoder.of().decode(inStream);
      if (clearBit) {
        return Timer.cleared(userKey, dynamicTimerTag, windows);
      }
      Instant fireTimestamp = InstantCoder.of().decode(inStream);
      Instant holdTimestamp = InstantCoder.of().decode(inStream);
      PaneInfo pane = PaneInfoCoder.INSTANCE.decode(inStream);
      return Timer.of(userKey, dynamicTimerTag, windows, fireTimestamp, holdTimestamp, pane);
    }

    @Override
    public List<? extends org.apache.beam.sdk.coders.Coder<?>> getCoderArguments() {
      return Collections.singletonList(keyCoder);
    }

    @Override
    public List<? extends org.apache.beam.sdk.coders.Coder<?>> getComponents() {
      return Arrays.asList(keyCoder, windowCoder);
    }

    public org.apache.beam.sdk.coders.Coder<? extends BoundedWindow> getWindowCoder() {
      return windowCoder;
    }

    public org.apache.beam.sdk.coders.Coder<Collection<? extends BoundedWindow>> getWindowsCoder() {
      return windowsCoder;
    }

    public org.apache.beam.sdk.coders.Coder<K> getValueCoder() {
      return keyCoder;
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      verifyDeterministic(this, "UserKey coder must be deterministic", keyCoder);
      verifyDeterministic(this, "Window coder must be deterministic", windowCoder);
    }

    @Override
    public void registerByteSizeObserver(Timer<K> value, ElementByteSizeObserver observer)
        throws Exception {
      keyCoder.registerByteSizeObserver(value.getUserKey(), observer);
      StringUtf8Coder.of().registerByteSizeObserver(value.getDynamicTimerTag(), observer);
      windowsCoder.registerByteSizeObserver(value.getWindows(), observer);
      BooleanCoder.of().registerByteSizeObserver(value.getClearBit(), observer);
      if (!value.getClearBit()) {
        InstantCoder.of().registerByteSizeObserver(value.getFireTimestamp(), observer);
        InstantCoder.of().registerByteSizeObserver(value.getHoldTimestamp(), observer);
        PaneInfoCoder.INSTANCE.registerByteSizeObserver(value.getPane(), observer);
      }
    }
  }
}
