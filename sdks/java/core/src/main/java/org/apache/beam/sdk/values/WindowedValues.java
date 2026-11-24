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
package org.apache.beam.sdk.values;

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import io.opentelemetry.context.Context;
import io.opentelemetry.context.ContextStorage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.AbstractList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CollectionCoder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.IterableLikeCoder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.PaneInfo.PaneInfoCoder;
import org.apache.beam.sdk.util.WindowedValueReceiver;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Pure;
import org.joda.time.Instant;

/**
 * Implementations of {@link org.apache.beam.sdk.values.WindowedValue} and static utility methods.
 *
 * <p>These are primarily intended for internal use by Beam SDK developers and runner developers.
 * Backwards incompatible changes will likely occur.
 */
@Internal
public class WindowedValues {
  private WindowedValues() {} // non-instantiable utility class

  public static <T> Builder<T> builder() {
    return new Builder<>();
  }

  /**
   * Creates a {@link Builder} that copies element metadata (excluding OpenTelemetry {@link
   * Context}) from the provided {@code template}.
   *
   * <p>The template's OpenTelemetry {@link Context} is intentionally not copied; {@link
   * Builder#getOpenTelemetryContext()} captures the current thread's OpenTelemetry {@link Context}
   * when built, normalizing {@link Context#root()} to {@code null}.
   */
  public static <T> Builder<T> builder(WindowedValue<T> template) {
    return builder(template, template.getValue());
  }

  /**
   * Creates a {@link Builder} that copies element metadata (excluding OpenTelemetry {@link
   * Context}) from the provided {@code template} with a new element {@code value}, without invoking
   * {@link WindowedValue#getValue()} on {@code template}.
   *
   * <p>The template's OpenTelemetry {@link Context} is intentionally not copied; {@link
   * Builder#getOpenTelemetryContext()} captures the current thread's OpenTelemetry {@link Context}
   * when built, normalizing {@link Context#root()} to {@code null}.
   *
   * @param template the template {@link WindowedValue} supplying metadata
   * @param value the value to set on the returned builder
   * @return a fresh mutable {@link Builder} containing {@code value} and metadata from {@code
   *     template}
   */
  public static <T> Builder<T> builder(WindowedValue<?> template, T value) {
    Builder<T> builder = new Builder<T>().setValue(value).setTimestamp(template.getTimestamp());
    if (template instanceof SimpleWindowedValue) {
      @Nullable BoundedWindow singleWindow =
          ((SimpleWindowedValue<?>) template).singleWindowOrNull();
      if (singleWindow != null) {
        builder.setWindow(singleWindow);
      } else {
        builder.setWindows(template.getWindows());
      }
    } else if (template instanceof Builder) {
      Builder<?> templateBuilder = (Builder<?>) template;
      if (templateBuilder.singleWindow != null) {
        builder.setWindow(templateBuilder.singleWindow);
      } else if (templateBuilder.windows != null) {
        builder.setWindows(templateBuilder.windows);
      } else {
        throw new IllegalStateException("Windows not set");
      }
    } else if (template instanceof SingleWindowedValue) {
      builder.setWindow(((SingleWindowedValue) template).getWindow());
    } else {
      builder.setWindows(template.getWindows());
    }
    return builder
        .setPaneInfo(template.getPaneInfo())
        .setRecordOffset(template.getRecordOffset())
        .setRecordId(template.getRecordId())
        .setCausedByDrain(template.causedByDrain())
        .setValueKind(template.getValueKind());
  }

  public static class Builder<T> implements OutputBuilder<T> {

    // Because T itself can be nullable, checking `maybeValue == null` cannot determine if it is set
    // or
    // not.
    //
    // Note also that JDK Optional class is written in such a way that it cannot have a nullable
    // type
    // for T (rendering it largely useless for its one reason for existing - composable
    // presence/absence).
    private @Nullable T maybeValue;
    private boolean hasValue = false;

    private @MonotonicNonNull WindowedValueReceiver<T> receiver;
    private @MonotonicNonNull PaneInfo paneInfo;
    private @MonotonicNonNull Instant timestamp;
    private @Nullable Collection<? extends BoundedWindow> windows;
    private @Nullable BoundedWindow singleWindow;
    private @Nullable String recordId;
    private @Nullable Long recordOffset;
    private CausedByDrain causedByDrain = CausedByDrain.NORMAL;
    private ValueKind valueKind = ValueKind.INSERT;

    @Override
    public Builder<T> setValue(T value) {
      this.hasValue = true;
      this.maybeValue = value;
      return this;
    }

    @Override
    public Builder<T> setTimestamp(Instant timestamp) {
      this.timestamp = timestamp;
      return this;
    }

    @Override
    public Builder<T> setWindows(Collection<? extends BoundedWindow> windows) {
      this.windows = windows;
      this.singleWindow = null;
      return this;
    }

    @Override
    public Builder<T> setPaneInfo(PaneInfo paneInfo) {
      this.paneInfo = paneInfo;
      return this;
    }

    @Override
    public Builder<T> setWindow(BoundedWindow window) {
      this.singleWindow = window;
      this.windows = null;
      return this;
    }

    @Override
    public Builder<T> setRecordId(@Nullable String recordId) {
      this.recordId = recordId;
      return this;
    }

    @Override
    public Builder<T> setRecordOffset(@Nullable Long recordOffset) {
      this.recordOffset = recordOffset;
      return this;
    }

    @Override
    public Builder<T> setCausedByDrain(CausedByDrain causedByDrain) {
      checkStateNotNull(causedByDrain, "CausedByDrain is null");
      this.causedByDrain = causedByDrain;
      return this;
    }

    @Override
    public Builder<T> setOpenTelemetryContext(@Nullable Context ignored) {
      return this;
    }

    @Override
    public Builder<T> setValueKind(ValueKind valueKind) {
      checkStateNotNull(valueKind, "ValueKind is null");
      this.valueKind = valueKind;
      return this;
    }

    public Builder<T> setReceiver(WindowedValueReceiver<T> receiver) {
      this.receiver = receiver;
      return this;
    }

    @Override
    public T getValue() {
      // If T is itself a nullable type, then this checkState ensures it is set, whether or not it
      // is null.
      // If T is a non-nullable type, this checkState ensures it is not null.
      checkState(hasValue, "Value not set");
      return getValueIgnoringNullness();
    }

    // This method is a way to @Nullable T to polymorphic-in-nullness T
    @SuppressWarnings("nullness")
    T getValueIgnoringNullness() {
      return maybeValue;
    }

    @Override
    public Instant getTimestamp() {
      checkStateNotNull(timestamp, "Timestamp not set");
      return timestamp;
    }

    /**
     * Returns the current thread's OpenTelemetry {@link Context}, or {@code null} when no non-root
     * context is active (normalizing {@link Context#root()} to {@code null} by identity
     * comparison).
     */
    @Override
    public @Nullable Context getOpenTelemetryContext() {
      // Builder may have been created under a different context at the beginning of a ParDo;
      // when building a WindowedValue we take the current context from storage and omit root.
      return currentOpenTelemetryContextOrNull();
    }

    @Override
    public Collection<? extends BoundedWindow> getWindows() {
      Collection<? extends BoundedWindow> result = windows;
      if (result != null) {
        return result;
      }
      BoundedWindow sw = checkStateNotNull(singleWindow, "Windows not set");
      result = sw == GlobalWindow.INSTANCE ? GLOBAL_WINDOWS : Collections.singletonList(sw);
      this.windows = result;
      return result;
    }

    @Override
    public PaneInfo getPaneInfo() {
      checkStateNotNull(paneInfo, "PaneInfo not set");
      return paneInfo;
    }

    @Override
    public @Nullable String getRecordId() {
      return recordId;
    }

    @Override
    public @Nullable Long getRecordOffset() {
      return recordOffset;
    }

    @Override
    public CausedByDrain causedByDrain() {
      checkStateNotNull(causedByDrain, "CausedByDrain not set");
      return causedByDrain;
    }

    @Override
    public ValueKind getValueKind() {
      checkStateNotNull(valueKind, "ValueKind not set");
      return valueKind;
    }

    @Override
    public Collection<Builder<T>> explodeWindows() {
      throw new UnsupportedOperationException(
          "Cannot explodeWindows() on WindowedValue builder; use build().explodeWindows()");
    }

    @Override
    @Pure
    public <OtherT> Builder<OtherT> withValue(OtherT newValue) {
      return WindowedValues.builder(this, newValue);
    }

    @Override
    public void output() {
      try {
        checkStateNotNull(receiver, "A WindowedValueReceiver must be set via setReceiver()")
            .output(build());
      } catch (Exception exc) {
        if (exc instanceof RuntimeException) {
          throw (RuntimeException) exc;
        } else {
          throw new RuntimeException("Exception thrown when outputting WindowedValue", exc);
        }
      }
    }

    public WindowedValue<T> build() {
      T val = getValue();
      Instant ts = getTimestamp();
      BoundedWindow sw = singleWindow;
      if (sw != null) {
        return WindowedValues.of(
            val,
            ts,
            sw,
            getPaneInfo(),
            getRecordId(),
            getRecordOffset(),
            causedByDrain(),
            getOpenTelemetryContext(),
            getValueKind());
      }
      return WindowedValues.of(
          val,
          ts,
          getWindows(),
          getPaneInfo(),
          getRecordId(),
          getRecordOffset(),
          causedByDrain(),
          getOpenTelemetryContext(),
          getValueKind());
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("value", getValue())
          .add("timestamp", getTimestamp())
          .add("windows", getWindows())
          .add("paneInfo", getPaneInfo())
          .add("causedByDrain", causedByDrain())
          .add("valueKind", getValueKind())
          .add("receiver", receiver)
          .toString();
    }
  }

  public static <T> WindowedValue<T> of(
      T value, Instant timestamp, Collection<? extends BoundedWindow> windows, PaneInfo paneInfo) {
    return of(
        value,
        timestamp,
        windows,
        paneInfo,
        null,
        null,
        CausedByDrain.NORMAL,
        null,
        ValueKind.INSERT);
  }

  private static final long MIN_TIMESTAMP_MILLIS = BoundedWindow.TIMESTAMP_MIN_VALUE.getMillis();

  @SuppressWarnings("ReferenceEquality")
  private static boolean isMinTimestamp(@Nullable Instant timestamp) {
    return timestamp == BoundedWindow.TIMESTAMP_MIN_VALUE
        || (timestamp != null && timestamp.getMillis() == MIN_TIMESTAMP_MILLIS);
  }

  @SuppressWarnings("ReferenceEquality")
  private static boolean isGlobalWindow(@Nullable BoundedWindow window) {
    return window == GlobalWindow.INSTANCE || GlobalWindow.INSTANCE.equals(window);
  }

  private static BoundedWindow extractSingleWindow(Collection<? extends BoundedWindow> windows) {
    if (windows instanceof AbstractList) {
      // Primary-super class check avoids JDK-8180450 _secondary_super_cache contention for
      // SingletonList, ArrayList, Arrays$ArrayList, etc.
      return ((AbstractList<? extends BoundedWindow>) windows).get(0);
    }
    if (windows instanceof List) {
      return ((List<? extends BoundedWindow>) windows).get(0);
    }
    return windows.iterator().next();
  }

  private static <T> WindowedValue<T> ofValidated(
      T value,
      Instant timestamp,
      BoundedWindow window,
      PaneInfo paneInfo,
      @Nullable String currentRecordId,
      @Nullable Long currentRecordOffset,
      CausedByDrain causedByDrain,
      @Nullable Context openTelemetryContext,
      ValueKind valueKind) {
    boolean isGlobal = isGlobalWindow(window);
    if (isGlobal && isMinTimestamp(timestamp)) {
      return new ValueInGlobalWindow<>(
          value,
          paneInfo,
          currentRecordId,
          currentRecordOffset,
          causedByDrain,
          openTelemetryContext,
          valueKind);
    } else if (isGlobal) {
      return new TimestampedValueInGlobalWindow<>(
          value,
          timestamp,
          paneInfo,
          currentRecordId,
          currentRecordOffset,
          causedByDrain,
          openTelemetryContext,
          valueKind);
    } else {
      return new TimestampedValueInSingleWindow<>(
          value,
          timestamp,
          window,
          paneInfo,
          currentRecordId,
          currentRecordOffset,
          causedByDrain,
          openTelemetryContext,
          valueKind);
    }
  }

  /** Returns a {@code WindowedValue} with the given value, timestamp, and windows. */
  public static <T> WindowedValue<T> of(
      T value,
      Instant timestamp,
      Collection<? extends BoundedWindow> windows,
      PaneInfo paneInfo,
      @Nullable String currentRecordId,
      @Nullable Long currentRecordOffset,
      CausedByDrain causedByDrain,
      @Nullable Context openTelemetryContext,
      ValueKind valueKind) {
    checkArgument(paneInfo != null, "WindowedValue requires PaneInfo, but it was null");
    checkArgument(windows != null, "WindowedValue requires windows, but it was null");
    checkArgument(causedByDrain != null, "WindowedValue requires CausedByDrain, but it was null");
    checkArgument(valueKind != null, "WindowedValue requires ValueKind, but it was null");
    if (windows == GLOBAL_WINDOWS) {
      return ofValidated(
          value,
          timestamp,
          GlobalWindow.INSTANCE,
          paneInfo,
          currentRecordId,
          currentRecordOffset,
          causedByDrain,
          openTelemetryContext,
          valueKind);
    }
    int size = windows.size();
    checkArgument(size > 0, "WindowedValue requires windows, but there were none");
    if (size == 1) {
      return ofValidated(
          value,
          timestamp,
          extractSingleWindow(windows),
          paneInfo,
          currentRecordId,
          currentRecordOffset,
          causedByDrain,
          openTelemetryContext,
          valueKind);
    } else {
      return new TimestampedValueInMultipleWindows<>(
          value,
          timestamp,
          windows,
          paneInfo,
          currentRecordId,
          currentRecordOffset,
          causedByDrain,
          openTelemetryContext,
          valueKind);
    }
  }

  private static @Nullable Context currentOpenTelemetryContextOrNull() {
    @Nullable Context current = ContextStorage.get().current();
    return (current == null || current == Context.root()) ? null : current;
  }

  /**
   * Returns the single {@link BoundedWindow} associated with {@code value} if {@code value} uses a
   * known single-window representation without allocating a wrapper collection, or {@code null}
   * otherwise.
   *
   * <p>A {@code null} return indicates only that the fast single-window lookup did not apply (for
   * example, a multi-window or empty element, an unset {@link Builder}, a {@link Builder} populated
   * via {@link Builder#setWindows(Collection)}, or a custom {@link WindowedValue} that does not
   * implement {@link SingleWindowedValue}); callers should fall back to {@link
   * WindowedValue#getWindows()}.
   *
   * <p>Dispatches via the {@code SimpleWindowedValue} / {@link Builder} class hierarchy first
   * (HotSpot primary-super check) rather than {@code instanceof SingleWindowedValue} to avoid
   * {@code _secondary_super_cache} contention (JDK-8180450) across concurrent worker threads when
   * interleaved with {@link WindowedValue} interface casts.
   *
   * @param value the {@link WindowedValue} to inspect
   * @return the single window, or {@code null} if {@code value} is not a known single-window
   *     representation
   */
  public static @Nullable BoundedWindow singleWindowOrNull(WindowedValue<?> value) {
    if (value instanceof SimpleWindowedValue) {
      return ((SimpleWindowedValue<?>) value).singleWindowOrNull();
    }
    if (value instanceof Builder) {
      return ((Builder<?>) value).singleWindow;
    }
    if (value instanceof SingleWindowedValue) {
      return ((SingleWindowedValue) value).getWindow();
    }
    return null;
  }

  /**
   * Returns a {@link WindowedValue} copying {@code timestamp}, {@code windows}, {@code paneInfo},
   * {@code recordId}, {@code recordOffset}, {@code causedByDrain}, and {@code valueKind} from
   * {@code template} with a new {@code value}, and replacing the OpenTelemetry {@link Context} with
   * the current thread's OpenTelemetry {@link Context} (normalizing {@link Context#root()} to
   * {@code null} by identity comparison).
   *
   * <p>Unlike {@link #withValue(WindowedValue, Object)}, which preserves {@code template}'s own
   * OpenTelemetry {@link Context}, this method intentionally ignores {@code
   * template.getOpenTelemetryContext()} and captures the current thread's active context.
   *
   * @param template the template {@link WindowedValue} supplying metadata
   * @param value the element value for the returned {@link WindowedValue}
   * @return a new {@link WindowedValue} containing {@code value} and metadata from {@code template}
   */
  public static <T> WindowedValue<T> ofWithCurrentContext(WindowedValue<?> template, T value) {
    @Nullable Context otelContext = currentOpenTelemetryContextOrNull();
    if (template instanceof SimpleWindowedValue) {
      return ((SimpleWindowedValue<?>) template).withValueAndContext(value, otelContext);
    }
    return ofWithCurrentContextFallback(template, value, template.getTimestamp(), otelContext);
  }

  /**
   * Returns a {@link WindowedValue} copying {@code windows}, {@code paneInfo}, {@code recordId},
   * {@code recordOffset}, {@code causedByDrain}, and {@code valueKind} from {@code template} with a
   * new {@code value} and {@code timestamp}, and replacing the OpenTelemetry {@link Context} with
   * the current thread's OpenTelemetry {@link Context} (normalizing {@link Context#root()} to
   * {@code null} by identity comparison).
   *
   * <p>Unlike {@link #withValue(WindowedValue, Object)}, this method intentionally ignores {@code
   * template.getOpenTelemetryContext()} and captures the current thread's active context.
   *
   * @param template the template {@link WindowedValue} supplying metadata
   * @param value the element value for the returned {@link WindowedValue}
   * @param timestamp the non-null event timestamp for the returned {@link WindowedValue}
   * @return a new {@link WindowedValue} containing {@code value}, {@code timestamp}, and metadata
   *     from {@code template}
   */
  public static <T> WindowedValue<T> ofWithCurrentContext(
      WindowedValue<?> template, T value, Instant timestamp) {
    @Nullable Context otelContext = currentOpenTelemetryContextOrNull();
    if (template instanceof SimpleWindowedValue) {
      return ((SimpleWindowedValue<?>) template)
          .withValueAndTimestampAndContext(value, timestamp, otelContext);
    }
    return ofWithCurrentContextFallback(template, value, timestamp, otelContext);
  }

  private static <T> WindowedValue<T> ofWithCurrentContextFallback(
      WindowedValue<?> template, T value, Instant timestamp, @Nullable Context otelContext) {
    @Nullable BoundedWindow singleWindow = singleWindowOrNull(template);
    if (singleWindow != null) {
      return of(
          value,
          timestamp,
          singleWindow,
          template.getPaneInfo(),
          template.getRecordId(),
          template.getRecordOffset(),
          template.causedByDrain(),
          otelContext,
          template.getValueKind());
    }
    return of(
        value,
        timestamp,
        template.getWindows(),
        template.getPaneInfo(),
        template.getRecordId(),
        template.getRecordOffset(),
        template.causedByDrain(),
        otelContext,
        template.getValueKind());
  }

  /**
   * Returns a {@link WindowedValue} copying {@code recordId}, {@code recordOffset}, {@code
   * causedByDrain}, and {@code valueKind} from {@code template} with the specified {@code value},
   * {@code timestamp}, {@code windows}, and {@code paneInfo}, and replacing the OpenTelemetry
   * {@link Context} with the current thread's OpenTelemetry {@link Context} (normalizing {@link
   * Context#root()} to {@code null} by identity comparison).
   *
   * @param template the template {@link WindowedValue} supplying record metadata, drain status, and
   *     {@link ValueKind}
   * @param value the element value
   * @param timestamp the non-null event timestamp
   * @param windows the non-empty collection of windows
   * @param paneInfo the non-null {@link PaneInfo}
   * @return a new {@link WindowedValue} with the specified attributes and metadata from {@code
   *     template}
   */
  public static <T> WindowedValue<T> ofWithCurrentContext(
      WindowedValue<?> template,
      T value,
      Instant timestamp,
      Collection<? extends BoundedWindow> windows,
      PaneInfo paneInfo) {
    return of(
        value,
        timestamp,
        windows,
        paneInfo,
        template.getRecordId(),
        template.getRecordOffset(),
        template.causedByDrain(),
        currentOpenTelemetryContextOrNull(),
        template.getValueKind());
  }

  /**
   * Returns a {@link WindowedValue} copying {@code recordId}, {@code recordOffset}, and {@code
   * causedByDrain} from {@code template} with the specified {@code value}, {@code timestamp},
   * {@code windows}, {@code paneInfo}, and {@code valueKind}, and replacing the OpenTelemetry
   * {@link Context} with the current thread's OpenTelemetry {@link Context} (normalizing {@link
   * Context#root()} to {@code null} by identity comparison).
   *
   * @param template the template {@link WindowedValue} supplying record metadata and drain status
   * @param value the element value
   * @param timestamp the non-null event timestamp
   * @param windows the non-empty collection of windows
   * @param paneInfo the non-null {@link PaneInfo}
   * @param valueKind the non-null {@link ValueKind}
   * @return a new {@link WindowedValue} with the specified attributes and metadata from {@code
   *     template}
   */
  public static <T> WindowedValue<T> ofWithCurrentContext(
      WindowedValue<?> template,
      T value,
      Instant timestamp,
      Collection<? extends BoundedWindow> windows,
      PaneInfo paneInfo,
      ValueKind valueKind) {
    return of(
        value,
        timestamp,
        windows,
        paneInfo,
        template.getRecordId(),
        template.getRecordOffset(),
        template.causedByDrain(),
        currentOpenTelemetryContextOrNull(),
        valueKind);
  }

  /**
   * Returns a {@link WindowedValue} in a single {@code window} using the current thread's
   * OpenTelemetry {@link Context} (normalizing {@link Context#root()} to {@code null} by identity
   * comparison).
   *
   * @param value the element value
   * @param timestamp the non-null event timestamp
   * @param window the non-null {@link BoundedWindow}
   * @param paneInfo the non-null {@link PaneInfo}
   * @param currentRecordId the optional record id
   * @param currentRecordOffset the optional record offset
   * @param causedByDrain the non-null {@link CausedByDrain}
   * @param valueKind the non-null {@link ValueKind}
   * @return a new single-window {@link WindowedValue}
   */
  public static <T> WindowedValue<T> ofWithCurrentContext(
      T value,
      Instant timestamp,
      BoundedWindow window,
      PaneInfo paneInfo,
      @Nullable String currentRecordId,
      @Nullable Long currentRecordOffset,
      CausedByDrain causedByDrain,
      ValueKind valueKind) {
    return of(
        value,
        timestamp,
        window,
        paneInfo,
        currentRecordId,
        currentRecordOffset,
        causedByDrain,
        currentOpenTelemetryContextOrNull(),
        valueKind);
  }

  /**
   * Returns a {@link WindowedValue} with the given {@code windows} using the current thread's
   * OpenTelemetry {@link Context} (normalizing {@link Context#root()} to {@code null} by identity
   * comparison).
   *
   * @param value the element value
   * @param timestamp the non-null event timestamp
   * @param windows the non-empty collection of windows
   * @param paneInfo the non-null {@link PaneInfo}
   * @param currentRecordId the optional record id
   * @param currentRecordOffset the optional record offset
   * @param causedByDrain the non-null {@link CausedByDrain}
   * @param valueKind the non-null {@link ValueKind}
   * @return a new {@link WindowedValue}
   */
  public static <T> WindowedValue<T> ofWithCurrentContext(
      T value,
      Instant timestamp,
      Collection<? extends BoundedWindow> windows,
      PaneInfo paneInfo,
      @Nullable String currentRecordId,
      @Nullable Long currentRecordOffset,
      CausedByDrain causedByDrain,
      ValueKind valueKind) {
    return of(
        value,
        timestamp,
        windows,
        paneInfo,
        currentRecordId,
        currentRecordOffset,
        causedByDrain,
        currentOpenTelemetryContextOrNull(),
        valueKind);
  }

  /**
   * @deprecated for use only in compatibility with old broken code
   */
  @Deprecated
  static <T> WindowedValue<T> createWithoutValidation(
      T value,
      Instant timestamp,
      Collection<? extends BoundedWindow> windows,
      PaneInfo paneInfo,
      CausedByDrain causedByDrain,
      @Nullable Context openTelemetryContext,
      ValueKind valueKind) {
    if (windows == GLOBAL_WINDOWS) {
      return of(
          value,
          timestamp,
          GlobalWindow.INSTANCE,
          paneInfo,
          null,
          null,
          causedByDrain,
          openTelemetryContext,
          valueKind);
    }
    if (windows.size() == 1) {
      return of(
          value,
          timestamp,
          extractSingleWindow(windows),
          paneInfo,
          null,
          null,
          causedByDrain,
          openTelemetryContext,
          valueKind);
    } else {
      return new TimestampedValueInMultipleWindows<>(
          value,
          timestamp,
          windows,
          paneInfo,
          null,
          null,
          causedByDrain,
          openTelemetryContext,
          valueKind);
    }
  }

  /** Returns a {@code WindowedValue} with the given value, timestamp, and window. */
  public static <T> WindowedValue<T> of(
      T value, Instant timestamp, BoundedWindow window, PaneInfo paneInfo) {
    return of(
        value,
        timestamp,
        window,
        paneInfo,
        null,
        null,
        CausedByDrain.NORMAL,
        null,
        ValueKind.INSERT);
  }

  public static <T> WindowedValue<T> of(
      T value,
      Instant timestamp,
      BoundedWindow window,
      PaneInfo paneInfo,
      @Nullable String currentRecordId,
      @Nullable Long currentRecordOffset,
      CausedByDrain causedByDrain,
      @Nullable Context openTelemetryContext,
      ValueKind valueKind) {
    checkArgument(paneInfo != null, "WindowedValue requires PaneInfo, but it was null");
    checkArgument(causedByDrain != null, "WindowedValue requires CausedByDrain, but it was null");
    checkArgument(valueKind != null, "WindowedValue requires ValueKind, but it was null");
    return ofValidated(
        value,
        timestamp,
        window,
        paneInfo,
        currentRecordId,
        currentRecordOffset,
        causedByDrain,
        openTelemetryContext,
        valueKind);
  }

  /**
   * Returns a {@code WindowedValue} with the given value in the {@link GlobalWindow} using the
   * default timestamp and pane.
   */
  public static <T> WindowedValue<T> valueInGlobalWindow(T value) {
    return new ValueInGlobalWindow<>(
        value, PaneInfo.NO_FIRING, null, null, CausedByDrain.NORMAL, null, ValueKind.INSERT);
  }

  /**
   * Returns a {@code WindowedValue} with the given value in the {@link GlobalWindow} using the
   * default timestamp and the specified pane.
   */
  public static <T> WindowedValue<T> valueInGlobalWindow(T value, PaneInfo paneInfo) {
    return new ValueInGlobalWindow<>(
        value, paneInfo, null, null, CausedByDrain.NORMAL, null, ValueKind.INSERT);
  }

  /**
   * Returns a {@code WindowedValue} with the given value and timestamp, {@code GlobalWindow} and
   * default pane.
   */
  public static <T> WindowedValue<T> timestampedValueInGlobalWindow(T value, Instant timestamp) {
    if (isMinTimestamp(timestamp)) {
      return valueInGlobalWindow(value);
    } else {
      return new TimestampedValueInGlobalWindow<>(
          value,
          timestamp,
          PaneInfo.NO_FIRING,
          null,
          null,
          CausedByDrain.NORMAL,
          null,
          ValueKind.INSERT);
    }
  }

  /**
   * Returns a {@code WindowedValue} with the given value, timestamp, and pane in the {@code
   * GlobalWindow}.
   */
  public static <T> WindowedValue<T> timestampedValueInGlobalWindow(
      T value, Instant timestamp, PaneInfo paneInfo) {
    if (paneInfo.equals(PaneInfo.NO_FIRING)) {
      return timestampedValueInGlobalWindow(value, timestamp);
    } else {
      return new TimestampedValueInGlobalWindow<>(
          value, timestamp, paneInfo, null, null, CausedByDrain.NORMAL, null, ValueKind.INSERT);
    }
  }

  /**
   * Returns a new {@code WindowedValue} that is a copy of this one, but with a different value,
   * which may have a new type {@code NewT}. Unlike {@link #ofWithCurrentContext(WindowedValue,
   * Object)}, this preserves {@code windowedValue}'s existing OpenTelemetry {@link Context}.
   */
  public static <OldT, NewT> WindowedValue<NewT> withValue(
      WindowedValue<OldT> windowedValue, NewT newValue) {
    if (windowedValue instanceof SimpleWindowedValue) {
      return ((SimpleWindowedValue<OldT>) windowedValue).withValue(newValue);
    }
    @Nullable BoundedWindow singleWindow = singleWindowOrNull(windowedValue);
    if (singleWindow != null) {
      return WindowedValues.of(
          newValue,
          windowedValue.getTimestamp(),
          singleWindow,
          windowedValue.getPaneInfo(),
          windowedValue.getRecordId(),
          windowedValue.getRecordOffset(),
          windowedValue.causedByDrain(),
          windowedValue.getOpenTelemetryContext(),
          windowedValue.getValueKind());
    }
    return WindowedValues.of(
        newValue,
        windowedValue.getTimestamp(),
        windowedValue.getWindows(),
        windowedValue.getPaneInfo(),
        windowedValue.getRecordId(),
        windowedValue.getRecordOffset(),
        windowedValue.causedByDrain(),
        windowedValue.getOpenTelemetryContext(),
        windowedValue.getValueKind());
  }

  public static <T> boolean equals(
      @Nullable WindowedValue<T> left, @Nullable WindowedValue<T> right) {
    if (left == null) {
      return right == null;
    }

    if (right == null) {
      return false;
    }

    // Compare timestamps first as they are most likely to differ.
    // Also compare timestamps according to millis-since-epoch because otherwise expensive
    // comparisons are made on their Chronology objects.
    return left.getTimestamp().isEqual(right.getTimestamp())
        && Objects.equals(left.getValue(), right.getValue())
        && Iterables.elementsEqual(left.getWindows(), right.getWindows())
        && Objects.equals(left.getPaneInfo(), right.getPaneInfo())
        && Objects.equals(left.getValueKind(), right.getValueKind());
  }

  public static <T> int hashCode(WindowedValue<T> windowedValue) {
    // Hash only the millis of the timestamp to be consistent with equals
    return Objects.hash(
        windowedValue.getValue(),
        windowedValue.getTimestamp().getMillis(),
        windowedValue.getWindows(),
        windowedValue.getPaneInfo(),
        windowedValue.getValueKind());
  }

  private static final Collection<? extends BoundedWindow> GLOBAL_WINDOWS =
      Collections.singletonList(GlobalWindow.INSTANCE);

  /** A {@link WindowedValues} which holds exactly single window per value. */
  public interface SingleWindowedValue {

    /** Returns the single window associated with this value. */
    BoundedWindow getWindow();
  }

  /**
   * An abstract superclass for implementations of {@link WindowedValues} that stores the value and
   * pane info.
   */
  private abstract static class SimpleWindowedValue<T> implements WindowedValue<T> {

    private final T value;
    private final PaneInfo paneInfo;
    private final @Nullable String currentRecordId;
    private final @Nullable Long currentRecordOffset;
    private final CausedByDrain causedByDrain;
    private final @Nullable Context context;
    private final ValueKind valueKind;

    @Override
    public @Nullable String getRecordId() {
      return currentRecordId;
    }

    @Override
    public @Nullable Long getRecordOffset() {
      return currentRecordOffset;
    }

    @Override
    public CausedByDrain causedByDrain() {
      return causedByDrain;
    }

    @Override
    public ValueKind getValueKind() {
      return valueKind;
    }

    protected SimpleWindowedValue(
        T value,
        PaneInfo paneInfo,
        @Nullable String currentRecordId,
        @Nullable Long currentRecordOffset,
        CausedByDrain causedByDrain,
        @Nullable Context context,
        ValueKind valueKind) {
      this.value = value;
      this.paneInfo = checkNotNull(paneInfo);
      this.currentRecordId = currentRecordId;
      this.currentRecordOffset = currentRecordOffset;
      this.causedByDrain = causedByDrain;
      this.context = context;
      this.valueKind = checkNotNull(valueKind, "ValueKind is null");
    }

    @Override
    public PaneInfo getPaneInfo() {
      return paneInfo;
    }

    @Override
    public T getValue() {
      return value;
    }

    @Override
    public @Nullable Context getOpenTelemetryContext() {
      return context;
    }

    @Override
    public Iterable<WindowedValue<T>> explodeWindows() {
      if (this.getWindows().size() == 1) {
        return ImmutableList.of(this);
      }
      ImmutableList.Builder<WindowedValue<T>> windowedValues = ImmutableList.builder();
      for (BoundedWindow w : this.getWindows()) {
        windowedValues.add(
            WindowedValues.of(this.getValue(), this.getTimestamp(), w, this.getPaneInfo()));
      }
      return windowedValues.build();
    }

    @Override
    public boolean equals(@Nullable Object other) {
      if (!(other instanceof WindowedValue)) {
        return false;
      }

      return WindowedValues.equals(this, (WindowedValue<T>) other);
    }

    @Nullable BoundedWindow singleWindowOrNull() {
      return null;
    }

    abstract <NewT> WindowedValue<NewT> withValueAndContext(
        NewT newValue, @Nullable Context newContext);

    abstract <NewT> WindowedValue<NewT> withValueAndTimestampAndContext(
        NewT newValue, Instant newTimestamp, @Nullable Context newContext);

    @Override
    public int hashCode() {
      return WindowedValues.hashCode(this);
    }
  }

  /** The abstract superclass of WindowedValue representations where timestamp == MIN. */
  private abstract static class MinTimestampWindowedValue<T> extends SimpleWindowedValue<T> {

    public MinTimestampWindowedValue(
        T value,
        PaneInfo pane,
        @Nullable String currentRecordId,
        @Nullable Long currentRecordOffset,
        CausedByDrain causedByDrain,
        @Nullable Context context,
        ValueKind valueKind) {
      super(value, pane, currentRecordId, currentRecordOffset, causedByDrain, context, valueKind);
    }

    @Override
    public Instant getTimestamp() {
      return BoundedWindow.TIMESTAMP_MIN_VALUE;
    }
  }

  /** The representation of a WindowedValue where timestamp == MIN and windows == {GlobalWindow}. */
  private static final class ValueInGlobalWindow<T> extends MinTimestampWindowedValue<T>
      implements SingleWindowedValue {

    public ValueInGlobalWindow(
        T value,
        PaneInfo paneInfo,
        @Nullable String currentRecordId,
        @Nullable Long currentRecordOffset,
        CausedByDrain causedByDrain,
        @Nullable Context context,
        ValueKind valueKind) {
      super(
          value, paneInfo, currentRecordId, currentRecordOffset, causedByDrain, context, valueKind);
    }

    @Override
    public Collection<? extends BoundedWindow> getWindows() {
      return GLOBAL_WINDOWS;
    }

    @Override
    public BoundedWindow getWindow() {
      return GlobalWindow.INSTANCE;
    }

    @Override
    BoundedWindow singleWindowOrNull() {
      return GlobalWindow.INSTANCE;
    }

    @Override
    public Iterable<WindowedValue<T>> explodeWindows() {
      return Collections.singletonList(this);
    }

    @Override
    public <NewT> WindowedValue<NewT> withValue(NewT newValue) {
      return new ValueInGlobalWindow<>(
          newValue,
          getPaneInfo(),
          getRecordId(),
          getRecordOffset(),
          causedByDrain(),
          getOpenTelemetryContext(),
          getValueKind());
    }

    @Override
    <NewT> WindowedValue<NewT> withValueAndContext(NewT newValue, @Nullable Context newContext) {
      return new ValueInGlobalWindow<>(
          newValue,
          getPaneInfo(),
          getRecordId(),
          getRecordOffset(),
          causedByDrain(),
          newContext,
          getValueKind());
    }

    @Override
    <NewT> WindowedValue<NewT> withValueAndTimestampAndContext(
        NewT newValue, Instant newTimestamp, @Nullable Context newContext) {
      if (isMinTimestamp(newTimestamp)) {
        return new ValueInGlobalWindow<>(
            newValue,
            getPaneInfo(),
            getRecordId(),
            getRecordOffset(),
            causedByDrain(),
            newContext,
            getValueKind());
      }
      return new TimestampedValueInGlobalWindow<>(
          newValue,
          newTimestamp,
          getPaneInfo(),
          getRecordId(),
          getRecordOffset(),
          causedByDrain(),
          newContext,
          getValueKind());
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (o instanceof ValueInGlobalWindow) {
        ValueInGlobalWindow<?> that = (ValueInGlobalWindow<?>) o;
        return Objects.equals(that.getPaneInfo(), this.getPaneInfo())
            && Objects.equals(that.getValue(), this.getValue());
      } else {
        return super.equals(o);
      }
    }

    @Override
    public int hashCode() {
      return Objects.hash(getValue(), getPaneInfo());
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass())
          .add("value", getValue())
          .add("paneInfo", getPaneInfo())
          .add("causedByDrain", causedByDrain())
          .toString();
    }
  }

  /** The abstract superclass of WindowedValue representations where timestamp is arbitrary. */
  private abstract static class TimestampedWindowedValue<T> extends SimpleWindowedValue<T> {
    private final Instant timestamp;

    public TimestampedWindowedValue(
        T value,
        Instant timestamp,
        PaneInfo paneInfo,
        @Nullable String currentRecordId,
        @Nullable Long currentRecordOffset,
        CausedByDrain causedByDrain,
        @Nullable Context context,
        ValueKind valueKind) {
      super(
          value, paneInfo, currentRecordId, currentRecordOffset, causedByDrain, context, valueKind);
      this.timestamp = checkNotNull(timestamp);
    }

    @Override
    public Instant getTimestamp() {
      return timestamp;
    }
  }

  /**
   * The representation of a WindowedValue where timestamp {@code >} MIN and windows ==
   * {GlobalWindow}.
   */
  private static final class TimestampedValueInGlobalWindow<T> extends TimestampedWindowedValue<T>
      implements SingleWindowedValue {

    public TimestampedValueInGlobalWindow(
        T value,
        Instant timestamp,
        PaneInfo paneInfo,
        @Nullable String currentRecordId,
        @Nullable Long currentRecordOffset,
        CausedByDrain causedByDrain,
        @Nullable Context context,
        ValueKind valueKind) {
      super(
          value,
          timestamp,
          paneInfo,
          currentRecordId,
          currentRecordOffset,
          causedByDrain,
          context,
          valueKind);
    }

    @Override
    public Collection<? extends BoundedWindow> getWindows() {
      return GLOBAL_WINDOWS;
    }

    @Override
    public BoundedWindow getWindow() {
      return GlobalWindow.INSTANCE;
    }

    @Override
    BoundedWindow singleWindowOrNull() {
      return GlobalWindow.INSTANCE;
    }

    @Override
    public Iterable<WindowedValue<T>> explodeWindows() {
      return Collections.singletonList(this);
    }

    @Override
    public <NewT> WindowedValue<NewT> withValue(NewT newValue) {
      return new TimestampedValueInGlobalWindow<>(
          newValue,
          getTimestamp(),
          getPaneInfo(),
          getRecordId(),
          getRecordOffset(),
          causedByDrain(),
          getOpenTelemetryContext(),
          getValueKind());
    }

    @Override
    <NewT> WindowedValue<NewT> withValueAndContext(NewT newValue, @Nullable Context newContext) {
      return new TimestampedValueInGlobalWindow<>(
          newValue,
          getTimestamp(),
          getPaneInfo(),
          getRecordId(),
          getRecordOffset(),
          causedByDrain(),
          newContext,
          getValueKind());
    }

    @Override
    <NewT> WindowedValue<NewT> withValueAndTimestampAndContext(
        NewT newValue, Instant newTimestamp, @Nullable Context newContext) {
      if (isMinTimestamp(newTimestamp)) {
        return new ValueInGlobalWindow<>(
            newValue,
            getPaneInfo(),
            getRecordId(),
            getRecordOffset(),
            causedByDrain(),
            newContext,
            getValueKind());
      }
      return new TimestampedValueInGlobalWindow<>(
          newValue,
          newTimestamp,
          getPaneInfo(),
          getRecordId(),
          getRecordOffset(),
          causedByDrain(),
          newContext,
          getValueKind());
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (o instanceof TimestampedValueInGlobalWindow) {
        TimestampedValueInGlobalWindow<?> that = (TimestampedValueInGlobalWindow<?>) o;
        // Compare timestamps first as they are most likely to differ.
        // Also compare timestamps according to millis-since-epoch because otherwise expensive
        // comparisons are made on their Chronology objects.
        return this.getTimestamp().isEqual(that.getTimestamp())
            && Objects.equals(that.getPaneInfo(), this.getPaneInfo())
            && Objects.equals(that.getValue(), this.getValue());
      } else {
        return super.equals(o);
      }
    }

    @Override
    public int hashCode() {
      // Hash only the millis of the timestamp to be consistent with equals
      return Objects.hash(getValue(), getPaneInfo(), getTimestamp().getMillis());
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass())
          .add("value", getValue())
          .add("timestamp", getTimestamp())
          .add("paneInfo", getPaneInfo())
          .add("causedByDrain", causedByDrain())
          .toString();
    }
  }

  /**
   * The representation of a WindowedValue where timestamp is arbitrary and windows == a single
   * non-Global window.
   */
  private static final class TimestampedValueInSingleWindow<T> extends TimestampedWindowedValue<T>
      implements SingleWindowedValue {

    private final BoundedWindow window;

    public TimestampedValueInSingleWindow(
        T value,
        Instant timestamp,
        BoundedWindow window,
        PaneInfo paneInfo,
        @Nullable String currentRecordId,
        @Nullable Long currentRecordOffset,
        CausedByDrain causedByDrain,
        @Nullable Context openTelemetryContext,
        ValueKind valueKind) {
      super(
          value,
          timestamp,
          paneInfo,
          currentRecordId,
          currentRecordOffset,
          causedByDrain,
          openTelemetryContext,
          valueKind);
      this.window = checkNotNull(window);
    }

    @Override
    public <NewT> WindowedValue<NewT> withValue(NewT newValue) {
      return new TimestampedValueInSingleWindow<>(
          newValue,
          getTimestamp(),
          window,
          getPaneInfo(),
          getRecordId(),
          getRecordOffset(),
          causedByDrain(),
          getOpenTelemetryContext(),
          getValueKind());
    }

    @Override
    <NewT> WindowedValue<NewT> withValueAndContext(NewT newValue, @Nullable Context newContext) {
      return new TimestampedValueInSingleWindow<>(
          newValue,
          getTimestamp(),
          window,
          getPaneInfo(),
          getRecordId(),
          getRecordOffset(),
          causedByDrain(),
          newContext,
          getValueKind());
    }

    @Override
    <NewT> WindowedValue<NewT> withValueAndTimestampAndContext(
        NewT newValue, Instant newTimestamp, @Nullable Context newContext) {
      return new TimestampedValueInSingleWindow<>(
          newValue,
          newTimestamp,
          window,
          getPaneInfo(),
          getRecordId(),
          getRecordOffset(),
          causedByDrain(),
          newContext,
          getValueKind());
    }

    @Override
    public Collection<? extends BoundedWindow> getWindows() {
      return Collections.singletonList(window);
    }

    @Override
    public BoundedWindow getWindow() {
      return window;
    }

    @Override
    BoundedWindow singleWindowOrNull() {
      return window;
    }

    @Override
    public Iterable<WindowedValue<T>> explodeWindows() {
      return Collections.singletonList(this);
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (o instanceof TimestampedValueInSingleWindow) {
        TimestampedValueInSingleWindow<?> that = (TimestampedValueInSingleWindow<?>) o;
        // Compare timestamps first as they are most likely to differ.
        // Also compare timestamps according to millis-since-epoch because otherwise expensive
        // comparisons are made on their Chronology objects.
        return this.getTimestamp().isEqual(that.getTimestamp())
            && Objects.equals(that.getValue(), this.getValue())
            && Objects.equals(that.getPaneInfo(), this.getPaneInfo())
            && Objects.equals(that.window, this.window);
      } else {
        return super.equals(o);
      }
    }

    @Override
    public int hashCode() {
      // Hash only the millis of the timestamp to be consistent with equals
      return Objects.hash(getValue(), getTimestamp().getMillis(), getPaneInfo(), window);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass())
          .add("value", getValue())
          .add("timestamp", getTimestamp())
          .add("window", window)
          .add("paneInfo", getPaneInfo())
          .add("causedByDrain", causedByDrain())
          .toString();
    }
  }

  /** The representation of a WindowedValue, excluding the special cases captured above. */
  private static final class TimestampedValueInMultipleWindows<T>
      extends TimestampedWindowedValue<T> {
    private Collection<? extends BoundedWindow> windows;

    public TimestampedValueInMultipleWindows(
        T value,
        Instant timestamp,
        Collection<? extends BoundedWindow> windows,
        PaneInfo paneInfo,
        @Nullable String currentRecordId,
        @Nullable Long currentRecordOffset,
        CausedByDrain causedByDrain,
        @Nullable Context openTelemetryContext,
        ValueKind valueKind) {
      super(
          value,
          timestamp,
          paneInfo,
          currentRecordId,
          currentRecordOffset,
          causedByDrain,
          openTelemetryContext,
          valueKind);
      this.windows = checkNotNull(windows);
    }

    @Override
    public Collection<? extends BoundedWindow> getWindows() {
      return windows;
    }

    @Override
    public <NewT> WindowedValue<NewT> withValue(NewT newValue) {
      return new TimestampedValueInMultipleWindows<>(
          newValue,
          getTimestamp(),
          getWindows(),
          getPaneInfo(),
          getRecordId(),
          getRecordOffset(),
          causedByDrain(),
          getOpenTelemetryContext(),
          getValueKind());
    }

    @Override
    <NewT> WindowedValue<NewT> withValueAndContext(NewT newValue, @Nullable Context newContext) {
      return withValueAndTimestampAndContext(newValue, getTimestamp(), newContext);
    }

    @Override
    <NewT> WindowedValue<NewT> withValueAndTimestampAndContext(
        NewT newValue, Instant newTimestamp, @Nullable Context newContext) {
      if (windows.size() > 1) {
        return new TimestampedValueInMultipleWindows<>(
            newValue,
            newTimestamp,
            windows,
            getPaneInfo(),
            getRecordId(),
            getRecordOffset(),
            causedByDrain(),
            newContext,
            getValueKind());
      }
      return of(
          newValue,
          newTimestamp,
          windows,
          getPaneInfo(),
          getRecordId(),
          getRecordOffset(),
          causedByDrain(),
          newContext,
          getValueKind());
    }

    @Override
    @SuppressWarnings("UndefinedEquals")
    public boolean equals(@Nullable Object o) {
      if (o instanceof TimestampedValueInMultipleWindows) {
        TimestampedValueInMultipleWindows<?> that = (TimestampedValueInMultipleWindows<?>) o;
        // Compare timestamps first as they are most likely to differ.
        // Also compare timestamps according to millis-since-epoch because otherwise expensive
        // comparisons are made on their Chronology objects.
        if (this.getTimestamp().isEqual(that.getTimestamp())
            && Objects.equals(that.getValue(), this.getValue())
            && Objects.equals(that.getPaneInfo(), this.getPaneInfo())) {
          ensureWindowsAreASet();
          that.ensureWindowsAreASet();
          return that.windows.equals(this.windows);
        } else {
          return false;
        }
      } else {
        return super.equals(o);
      }
    }

    @Override
    public int hashCode() {
      // Hash only the millis of the timestamp to be consistent with equals
      ensureWindowsAreASet();
      return Objects.hash(getValue(), getTimestamp().getMillis(), getPaneInfo(), windows);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass())
          .add("value", getValue())
          .add("timestamp", getTimestamp())
          .add("windows", windows)
          .add("paneInfo", getPaneInfo())
          .add("causedByDrain", causedByDrain())
          .toString();
    }

    private void ensureWindowsAreASet() {
      if (!(windows instanceof Set)) {
        windows = new LinkedHashSet<>(windows);
      }
    }
  }

  /////////////////////////////////////////////////////////////////////////////

  /**
   * Returns the {@code Coder} to use for a {@code WindowedValue<T>}, using the given valueCoder and
   * windowCoder.
   */
  public static <T> FullWindowedValueCoder<T> getFullCoder(
      Coder<T> valueCoder, Coder<? extends BoundedWindow> windowCoder) {
    return FullWindowedValueCoder.of(valueCoder, windowCoder);
  }

  /** Returns the {@code ValueOnlyCoder} from the given valueCoder. */
  public static <T> ValueOnlyWindowedValueCoder<T> getValueOnlyCoder(Coder<T> valueCoder) {
    return ValueOnlyWindowedValueCoder.of(valueCoder);
  }

  /** Returns the {@code ParamWindowedValueCoder} from the given valueCoder. */
  public static <T> ParamWindowedValueCoder<T> getParamWindowedValueCoder(Coder<T> valueCoder) {
    return ParamWindowedValueCoder.of(valueCoder);
  }

  /** Abstract class for {@code WindowedValue} coder. */
  public abstract static class WindowedValueCoder<T> extends StructuredCoder<WindowedValue<T>> {
    final Coder<T> valueCoder;
    private static volatile boolean metadataSupported = false;

    public static void setMetadataSupported() {
      metadataSupported = true;
    }

    public static void setMetadataNotSupported() {
      metadataSupported = false;
    }

    public static boolean isMetadataSupported() {
      return metadataSupported;
    }

    WindowedValueCoder(Coder<T> valueCoder) {
      this.valueCoder = checkNotNull(valueCoder);
    }

    /** Returns the value coder. */
    public Coder<T> getValueCoder() {
      return valueCoder;
    }

    /**
     * Returns a new {@code WindowedValueCoder} that is a copy of this one, but with a different
     * value coder.
     */
    public abstract <NewT> WindowedValueCoder<NewT> withValueCoder(Coder<NewT> valueCoder);
  }

  /** Coder for {@code WindowedValue}. */
  public static class FullWindowedValueCoder<T> extends WindowedValueCoder<T> {
    private final Coder<? extends BoundedWindow> windowCoder;
    // Precompute and cache the coder for a list of windows.
    private final Coder<Collection<? extends BoundedWindow>> windowsCoder;

    public static <T> FullWindowedValueCoder<T> of(
        Coder<T> valueCoder, Coder<? extends BoundedWindow> windowCoder) {
      return new FullWindowedValueCoder<>(valueCoder, windowCoder);
    }

    FullWindowedValueCoder(Coder<T> valueCoder, Coder<? extends BoundedWindow> windowCoder) {
      super(valueCoder);
      this.windowCoder = checkNotNull(windowCoder);
      // It's not possible to statically type-check correct use of the
      // windowCoder (we have to ensure externally that we only get
      // windows of the class handled by windowCoder), so type
      // windowsCoder in a way that makes encode() and decode() work
      // right, and cast the window type away here.
      @SuppressWarnings({"unchecked", "rawtypes"})
      Coder<Collection<? extends BoundedWindow>> collectionCoder =
          (Coder) CollectionCoder.of(this.windowCoder);
      this.windowsCoder = collectionCoder;
    }

    public Coder<? extends BoundedWindow> getWindowCoder() {
      return windowCoder;
    }

    public Coder<Collection<? extends BoundedWindow>> getWindowsCoder() {
      return windowsCoder;
    }

    @Override
    public <NewT> WindowedValueCoder<NewT> withValueCoder(Coder<NewT> valueCoder) {
      return new FullWindowedValueCoder<>(valueCoder, windowCoder);
    }

    @SuppressWarnings("unchecked")
    private Collection<? extends BoundedWindow> decodeMultipleWindows(
        int numWindows, InputStream inStream) throws IOException {
      return ((CollectionCoder<BoundedWindow>) (Coder<?>) windowsCoder)
          .decodeAfterSizeHeader(numWindows, inStream);
    }

    private static <T> void encodeElementMetadata(
        WindowedValue<T> windowedElem, OutputStream outStream) throws IOException {
      BeamFnApi.Elements.ElementMetadata.Builder builder =
          BeamFnApi.Elements.ElementMetadata.newBuilder();
      io.opentelemetry.context.@Nullable Context openTelemetryContext =
          windowedElem.getOpenTelemetryContext();
      if (openTelemetryContext != null
          && openTelemetryContext != io.opentelemetry.context.Context.root()) {
        OpenTelemetryContextPropagator.set(openTelemetryContext, builder);
      }
      BeamFnApi.Elements.ElementMetadata em =
          builder
              .setDrain(
                  windowedElem.causedByDrain() == CausedByDrain.CAUSED_BY_DRAIN
                      ? BeamFnApi.Elements.DrainMode.Enum.DRAINING
                      : BeamFnApi.Elements.DrainMode.Enum.NOT_DRAINING)
              .setValueKind(ValueKindUtil.toProto(windowedElem.getValueKind()))
              .build();
      ByteArrayCoder.of().encode(em.toByteArray(), outStream);
    }

    private WindowedValue<T> decodeWithMetadata(
        InputStream inStream,
        Coder.Context context,
        Instant timestamp,
        int numWindows,
        @Nullable BoundedWindow singleWindow,
        @Nullable Collection<? extends BoundedWindow> windows,
        PaneInfo paneInfo)
        throws IOException {
      BeamFnApi.Elements.ElementMetadata elementMetadata =
          BeamFnApi.Elements.ElementMetadata.parseFrom(ByteArrayCoder.of().decode(inStream));
      CausedByDrain causedByDrain =
          elementMetadata.getDrain().equals(BeamFnApi.Elements.DrainMode.Enum.DRAINING)
              ? CausedByDrain.CAUSED_BY_DRAIN
              : CausedByDrain.NORMAL;
      io.opentelemetry.context.@Nullable Context openTelemetryContext = null;
      if (!elementMetadata.getTraceparent().isEmpty()
          || !elementMetadata.getTracestate().isEmpty()) {
        io.opentelemetry.context.Context extracted =
            OpenTelemetryContextPropagator.read(elementMetadata);
        openTelemetryContext =
            extracted == io.opentelemetry.context.Context.root() ? null : extracted;
      }
      ValueKind valueKind = ValueKindUtil.fromProto(elementMetadata.getValueKind());
      T value = valueCoder.decode(inStream, context);
      if (numWindows == 1) {
        return WindowedValues.of(
            value,
            timestamp,
            checkStateNotNull(singleWindow),
            paneInfo,
            null,
            null,
            causedByDrain,
            openTelemetryContext,
            valueKind);
      }
      // Because there are some remaining (incorrect) uses of WindowedValue with no windows,
      // we call this deprecated no-validation path when decoding
      return WindowedValues.createWithoutValidation(
          value,
          timestamp,
          checkStateNotNull(windows),
          paneInfo,
          causedByDrain,
          openTelemetryContext,
          valueKind);
    }

    @Override
    public void encode(WindowedValue<T> windowedElem, OutputStream outStream)
        throws CoderException, IOException {
      encode(windowedElem, outStream, Coder.Context.NESTED);
    }

    @Override
    public void encode(WindowedValue<T> windowedElem, OutputStream outStream, Coder.Context context)
        throws CoderException, IOException {
      InstantCoder.of().encode(windowedElem.getTimestamp(), outStream);
      @Nullable BoundedWindow singleWindow = singleWindowOrNull(windowedElem);
      if (singleWindow != null) {
        // Same invariant as constructor: callers must ensure windows match windowCoder.
        @SuppressWarnings("unchecked")
        Coder<BoundedWindow> typedWindowCoder = (Coder<BoundedWindow>) windowCoder;
        IterableLikeCoder.encodeSingleElement(typedWindowCoder, singleWindow, outStream);
      } else {
        windowsCoder.encode(windowedElem.getWindows(), outStream);
      }
      boolean metadataSupported = isMetadataSupported();
      PaneInfo paneInfo = windowedElem.getPaneInfo();
      if (paneInfo.isElementMetadata() != metadataSupported) {
        paneInfo = paneInfo.withElementMetadata(metadataSupported);
      }
      PaneInfoCoder.INSTANCE.encode(paneInfo, outStream);
      if (metadataSupported) {
        encodeElementMetadata(windowedElem, outStream);
      }
      valueCoder.encode(windowedElem.getValue(), outStream, context);
    }

    @Override
    public WindowedValue<T> decode(InputStream inStream) throws CoderException, IOException {
      return decode(inStream, Coder.Context.NESTED);
    }

    @Override
    public WindowedValue<T> decode(InputStream inStream, Coder.Context context)
        throws CoderException, IOException {
      Instant timestamp = InstantCoder.of().decode(inStream);
      int numWindows = IterableLikeCoder.readSizeHeader(inStream);
      @Nullable BoundedWindow singleWindow = null;
      @Nullable Collection<? extends BoundedWindow> windows = null;
      if (numWindows == 1) {
        singleWindow =
            checkNotNull(windowCoder.decode(inStream), "Decoded single window must not be null");
      } else {
        windows = decodeMultipleWindows(numWindows, inStream);
      }
      PaneInfo paneInfo = PaneInfoCoder.INSTANCE.decode(inStream);
      // Check plain paneInfo field before volatile metadataSupported load-acquire.
      if (paneInfo.isElementMetadata() && isMetadataSupported()) {
        return decodeWithMetadata(
            inStream, context, timestamp, numWindows, singleWindow, windows, paneInfo);
      }
      T value = valueCoder.decode(inStream, context);
      if (numWindows == 1) {
        return WindowedValues.of(value, timestamp, checkStateNotNull(singleWindow), paneInfo);
      }
      // Because there are some remaining (incorrect) uses of WindowedValue with no windows,
      // we call this deprecated no-validation path when decoding
      return WindowedValues.createWithoutValidation(
          value,
          timestamp,
          checkStateNotNull(windows),
          paneInfo,
          CausedByDrain.NORMAL,
          null,
          ValueKind.INSERT);
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      verifyDeterministic(
          this, "FullWindowedValueCoder requires a deterministic valueCoder", valueCoder);
      verifyDeterministic(
          this, "FullWindowedValueCoder requires a deterministic windowCoder", windowCoder);
    }

    @Override
    public void registerByteSizeObserver(WindowedValue<T> value, ElementByteSizeObserver observer)
        throws Exception {
      InstantCoder.of().registerByteSizeObserver(value.getTimestamp(), observer);
      @Nullable BoundedWindow singleWindow = singleWindowOrNull(value);
      if (singleWindow != null) {
        // Same invariant as constructor: callers must ensure windows match windowCoder.
        @SuppressWarnings("unchecked")
        Coder<BoundedWindow> typedWindowCoder = (Coder<BoundedWindow>) windowCoder;
        IterableLikeCoder.registerSingleElementByteSize(typedWindowCoder, singleWindow, observer);
      } else {
        windowsCoder.registerByteSizeObserver(value.getWindows(), observer);
      }
      PaneInfoCoder.INSTANCE.registerByteSizeObserver(value.getPaneInfo(), observer);
      valueCoder.registerByteSizeObserver(value.getValue(), observer);
    }

    /**
     * {@inheritDoc}.
     *
     * @return a singleton list containing the {@code valueCoder} of this {@link
     *     FullWindowedValueCoder}.
     */
    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      // The value type is the only generic type parameter exposed by this coder. The component
      // coders include the window coder as well
      return Collections.singletonList(valueCoder);
    }

    @Override
    public List<? extends Coder<?>> getComponents() {
      return Arrays.asList(valueCoder, windowCoder);
    }
  }

  /**
   * Coder for {@code WindowedValue}.
   *
   * <p>A {@code ValueOnlyWindowedValueCoder} only encodes and decodes the value. It drops timestamp
   * and windows for encoding, and uses defaults timestamp, and windows for decoding.
   *
   * @deprecated Use ParamWindowedValueCoder instead, it is a general purpose implementation of the
   *     same concept but makes timestamp, windows and pane info configurable.
   */
  @Deprecated
  public static class ValueOnlyWindowedValueCoder<T> extends WindowedValueCoder<T> {
    public static <T> ValueOnlyWindowedValueCoder<T> of(Coder<T> valueCoder) {
      return new ValueOnlyWindowedValueCoder<>(valueCoder);
    }

    ValueOnlyWindowedValueCoder(Coder<T> valueCoder) {
      super(valueCoder);
    }

    @Override
    public <NewT> WindowedValueCoder<NewT> withValueCoder(Coder<NewT> valueCoder) {
      return new ValueOnlyWindowedValueCoder<>(valueCoder);
    }

    @Override
    public void encode(WindowedValue<T> windowedElem, OutputStream outStream)
        throws CoderException, IOException {
      encode(windowedElem, outStream, Coder.Context.NESTED);
    }

    @Override
    public void encode(WindowedValue<T> windowedElem, OutputStream outStream, Coder.Context context)
        throws CoderException, IOException {
      valueCoder.encode(windowedElem.getValue(), outStream, context);
    }

    @Override
    public WindowedValue<T> decode(InputStream inStream) throws CoderException, IOException {
      return decode(inStream, Coder.Context.NESTED);
    }

    @Override
    public WindowedValue<T> decode(InputStream inStream, Coder.Context context)
        throws CoderException, IOException {
      T value = valueCoder.decode(inStream, context);
      return WindowedValues.valueInGlobalWindow(value);
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      verifyDeterministic(
          this, "ValueOnlyWindowedValueCoder requires a deterministic valueCoder", valueCoder);
    }

    @Override
    public void registerByteSizeObserver(WindowedValue<T> value, ElementByteSizeObserver observer)
        throws Exception {
      valueCoder.registerByteSizeObserver(value.getValue(), observer);
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return Collections.singletonList(valueCoder);
    }
  }

  /**
   * A parameterized coder for {@code WindowedValue}.
   *
   * <p>A {@code ParamWindowedValueCoder} only encodes and decodes the value. It drops timestamp,
   * windows, and pane info during encoding, and uses the supplied parameterized timestamp, windows
   * and pane info values during decoding when reconstructing the windowed value.
   */
  public static class ParamWindowedValueCoder<T> extends FullWindowedValueCoder<T> {

    private static final long serialVersionUID = 1L;

    private transient WindowedValue<byte[]> windowedValuePrototype;

    private static final byte[] EMPTY_BYTES = new byte[0];

    /**
     * Returns the {@link ParamWindowedValueCoder} for the given valueCoder and windowCoder using
     * the supplied parameterized timestamp, windows and pane info for {@link WindowedValues}.
     */
    public static <T> ParamWindowedValueCoder<T> of(
        Coder<T> valueCoder,
        Coder<? extends BoundedWindow> windowCoder,
        Instant timestamp,
        Collection<? extends BoundedWindow> windows,
        PaneInfo paneInfo) {
      return of(valueCoder, windowCoder, timestamp, windows, paneInfo, ValueKind.INSERT);
    }

    public static <T> ParamWindowedValueCoder<T> of(
        Coder<T> valueCoder,
        Coder<? extends BoundedWindow> windowCoder,
        Instant timestamp,
        Collection<? extends BoundedWindow> windows,
        PaneInfo paneInfo,
        ValueKind valueKind) {
      return new ParamWindowedValueCoder<>(
          valueCoder, windowCoder, timestamp, windows, paneInfo, valueKind);
    }

    /**
     * Returns the {@link ParamWindowedValueCoder} for the given valueCoder and windowCoder using
     * {@link BoundedWindow#TIMESTAMP_MIN_VALUE} as the timestamp, {@link #GLOBAL_WINDOWS} as the
     * window and {@link PaneInfo#NO_FIRING} as the pane info for parameters.
     */
    public static <T> ParamWindowedValueCoder<T> of(
        Coder<T> valueCoder, Coder<? extends BoundedWindow> windowCoder) {
      return ParamWindowedValueCoder.of(
          valueCoder,
          windowCoder,
          BoundedWindow.TIMESTAMP_MIN_VALUE,
          GLOBAL_WINDOWS,
          PaneInfo.NO_FIRING,
          ValueKind.INSERT);
    }

    /**
     * Returns the {@link ParamWindowedValueCoder} for the given valueCoder and {@link
     * GlobalWindow.Coder#INSTANCE} using {@link BoundedWindow#TIMESTAMP_MIN_VALUE} as the
     * timestamp, {@link #GLOBAL_WINDOWS} as the window and {@link PaneInfo#NO_FIRING} as the pane
     * info for parameters.
     */
    public static <T> ParamWindowedValueCoder<T> of(Coder<T> valueCoder) {
      return ParamWindowedValueCoder.of(valueCoder, GlobalWindow.Coder.INSTANCE);
    }

    ParamWindowedValueCoder(
        Coder<T> valueCoder,
        Coder<? extends BoundedWindow> windowCoder,
        Instant timestamp,
        Collection<? extends BoundedWindow> windows,
        PaneInfo paneInfo,
        ValueKind valueKind) {
      super(valueCoder, windowCoder);
      this.windowedValuePrototype =
          WindowedValues.of(
              EMPTY_BYTES,
              timestamp,
              windows,
              paneInfo,
              null,
              null,
              CausedByDrain.NORMAL,
              null,
              valueKind);
    }

    @Override
    public <NewT> WindowedValueCoder<NewT> withValueCoder(Coder<NewT> valueCoder) {
      return new ParamWindowedValueCoder<>(
          valueCoder,
          getWindowCoder(),
          getTimestamp(),
          getWindows(),
          getPaneInfo(),
          getValueKind());
    }

    @Override
    public void encode(WindowedValue<T> windowedElem, OutputStream outStream)
        throws CoderException, IOException {
      encode(windowedElem, outStream, Coder.Context.NESTED);
    }

    @Override
    public void encode(WindowedValue<T> windowedElem, OutputStream outStream, Coder.Context context)
        throws CoderException, IOException {
      valueCoder.encode(windowedElem.getValue(), outStream, context);
    }

    @Override
    public WindowedValue<T> decode(InputStream inStream) throws CoderException, IOException {
      return decode(inStream, Coder.Context.NESTED);
    }

    @Override
    public WindowedValue<T> decode(InputStream inStream, Coder.Context context)
        throws CoderException, IOException {
      return WindowedValues.withValue(windowedValuePrototype, valueCoder.decode(inStream, context));
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      verifyDeterministic(
          this, "ParamWindowedValueCoder requires a deterministic valueCoder", valueCoder);
    }

    @Override
    public void registerByteSizeObserver(WindowedValue<T> value, ElementByteSizeObserver observer)
        throws Exception {
      valueCoder.registerByteSizeObserver(value.getValue(), observer);
    }

    public ValueKind getValueKind() {
      return windowedValuePrototype.getValueKind();
    }

    public Instant getTimestamp() {
      return windowedValuePrototype.getTimestamp();
    }

    public Collection<? extends BoundedWindow> getWindows() {
      return windowedValuePrototype.getWindows();
    }

    public PaneInfo getPaneInfo() {
      return windowedValuePrototype.getPaneInfo();
    }

    /** Returns the serialized payload that will be provided when deserializing this coder. */
    public static byte[] getPayload(ParamWindowedValueCoder<?> from) {
      // Use FullWindowedValueCoder to encode the constant members(timestamp, window, pane) in
      // ParamWindowedValueCoder
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      WindowedValue<byte[]> windowedValue =
          WindowedValues.of(
              EMPTY_BYTES,
              from.getTimestamp(),
              from.getWindows(),
              from.getPaneInfo(),
              null,
              null,
              CausedByDrain.NORMAL,
              null,
              from.getValueKind());
      WindowedValues.FullWindowedValueCoder<byte[]> windowedValueCoder =
          WindowedValues.FullWindowedValueCoder.of(ByteArrayCoder.of(), from.getWindowCoder());
      try {
        windowedValueCoder.encode(windowedValue, baos);
      } catch (IOException e) {
        throw new RuntimeException(
            "Unable to encode constant members of ParamWindowedValueCoder: ", e);
      }
      return baos.toByteArray();
    }

    /** Create a {@link Coder} from its component {@link Coder coders}. */
    public static WindowedValues.ParamWindowedValueCoder<?> fromComponents(
        List<Coder<?>> components, byte[] payload) {
      Coder<? extends BoundedWindow> windowCoder =
          (Coder<? extends BoundedWindow>) components.get(1);
      WindowedValues.FullWindowedValueCoder<byte[]> windowedValueCoder =
          WindowedValues.FullWindowedValueCoder.of(ByteArrayCoder.of(), windowCoder);

      try {
        ByteArrayInputStream bais = new ByteArrayInputStream(payload);
        WindowedValue<byte[]> windowedValue = windowedValueCoder.decode(bais);
        return WindowedValues.ParamWindowedValueCoder.of(
            components.get(0),
            windowCoder,
            windowedValue.getTimestamp(),
            windowedValue.getWindows(),
            windowedValue.getPaneInfo(),
            windowedValue.getValueKind());
      } catch (IOException e) {
        throw new RuntimeException(
            "Unable to decode constant members from payload for ParamWindowedValueCoder: ", e);
      }
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
      out.defaultWriteObject();
      out.writeObject(getPayload(this));
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
      in.defaultReadObject();
      byte[] payload = (byte[]) in.readObject();
      ParamWindowedValueCoder<?> paramWindowedValueCoder =
          fromComponents(Arrays.asList(valueCoder, getWindowCoder()), payload);
      this.windowedValuePrototype = paramWindowedValueCoder.windowedValuePrototype;
    }
  }
}
