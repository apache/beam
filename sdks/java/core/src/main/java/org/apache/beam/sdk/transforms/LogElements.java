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
package org.apache.beam.sdk.transforms;

import java.util.Objects;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

/**
 * {@link PTransform} for logging elements of a {@link PCollection}.
 *
 * <p>Each element is logged and then emitted unchanged.
 *
 * <pre>{@code
 * PCollection<String> words = ...;
 * PCollection<String> loggedWords = words.apply(LogElements.info().withPrefix("word: "));
 * }</pre>
 *
 * @param <T> the element type of the input {@link PCollection}
 */
public class LogElements<T> extends PTransform<PCollection<T>, PCollection<T>> {
  private static final Logger LOG = LoggerFactory.getLogger(LogElements.class);

  private final Level level;
  private final String prefix;
  private final boolean withTimestamp;
  private final boolean withWindow;
  private final boolean withPaneInfo;

  /** Returns a {@link LogElements} transform that logs elements at {@link Level#TRACE}. */
  public static <T> LogElements<T> trace() {
    return of(Level.TRACE);
  }

  /** Returns a {@link LogElements} transform that logs elements at {@link Level#DEBUG}. */
  public static <T> LogElements<T> debug() {
    return of(Level.DEBUG);
  }

  /** Returns a {@link LogElements} transform that logs elements at {@link Level#INFO}. */
  public static <T> LogElements<T> info() {
    return of(Level.INFO);
  }

  /** Returns a {@link LogElements} transform that logs elements at {@link Level#WARN}. */
  public static <T> LogElements<T> warn() {
    return of(Level.WARN);
  }

  /** Returns a {@link LogElements} transform that logs elements at {@link Level#ERROR}. */
  public static <T> LogElements<T> error() {
    return of(Level.ERROR);
  }

  /** Returns a {@link LogElements} transform that logs elements at the given level. */
  public static <T> LogElements<T> of(Level level) {
    return new LogElements<>(level, "", false, false, false);
  }

  private LogElements(
      Level level, String prefix, boolean withTimestamp, boolean withWindow, boolean withPaneInfo) {
    this.level = Objects.requireNonNull(level, "level");
    this.prefix = Objects.requireNonNull(prefix, "prefix");
    this.withTimestamp = withTimestamp;
    this.withWindow = withWindow;
    this.withPaneInfo = withPaneInfo;
  }

  /** Returns a new {@link LogElements} transform with the given prefix before each element. */
  public LogElements<T> withPrefix(String prefix) {
    return new LogElements<>(level, prefix, withTimestamp, withWindow, withPaneInfo);
  }

  /** Returns a new {@link LogElements} transform that logs each element's timestamp. */
  public LogElements<T> withTimestamp() {
    return new LogElements<>(level, prefix, true, withWindow, withPaneInfo);
  }

  /** Returns a new {@link LogElements} transform that logs each element's window. */
  public LogElements<T> withWindow() {
    return new LogElements<>(level, prefix, withTimestamp, true, withPaneInfo);
  }

  /** Returns a new {@link LogElements} transform that logs each element's pane info. */
  public LogElements<T> withPaneInfo() {
    return new LogElements<>(level, prefix, withTimestamp, withWindow, true);
  }

  @Override
  public PCollection<T> expand(PCollection<T> input) {
    return input.apply("Log", ParDo.of(new LoggingFn<>(this)));
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);
    builder
        .add(DisplayData.item("level", level.name()).withLabel("Log Level"))
        .addIfNotDefault(DisplayData.item("prefix", prefix).withLabel("Prefix"), "")
        .addIfNotDefault(
            DisplayData.item("withTimestamp", withTimestamp).withLabel("Log Timestamp"), false)
        .addIfNotDefault(DisplayData.item("withWindow", withWindow).withLabel("Log Window"), false)
        .addIfNotDefault(
            DisplayData.item("withPaneInfo", withPaneInfo).withLabel("Log Pane Info"), false);
  }

  static String formatForLogging(
      @Nullable Object element,
      String prefix,
      boolean withTimestamp,
      boolean withWindow,
      boolean withPaneInfo,
      Instant timestamp,
      BoundedWindow window,
      PaneInfo paneInfo) {
    StringBuilder builder = new StringBuilder(prefix).append(element);
    if (withTimestamp) {
      builder.append(", timestamp=").append(timestamp);
    }
    if (withWindow) {
      builder.append(", window=").append(window);
    }
    if (withPaneInfo) {
      builder.append(", paneInfo=").append(paneInfo);
    }
    return builder.toString();
  }

  @VisibleForTesting
  static void log(Level level, String message) {
    switch (level) {
      case TRACE:
        LOG.trace("{}", message);
        break;
      case DEBUG:
        LOG.debug("{}", message);
        break;
      case INFO:
        LOG.info("{}", message);
        break;
      case WARN:
        LOG.warn("{}", message);
        break;
      case ERROR:
        LOG.error("{}", message);
        break;
      default:
        throw unsupportedLogLevel(level);
    }
  }

  private static boolean isLoggingEnabled(Level level) {
    switch (level) {
      case TRACE:
        return LOG.isTraceEnabled();
      case DEBUG:
        return LOG.isDebugEnabled();
      case INFO:
        return LOG.isInfoEnabled();
      case WARN:
        return LOG.isWarnEnabled();
      case ERROR:
        return LOG.isErrorEnabled();
      default:
        throw unsupportedLogLevel(level);
    }
  }

  private static IllegalArgumentException unsupportedLogLevel(Level level) {
    return new IllegalArgumentException("Unsupported log level: " + level);
  }

  private static class LoggingFn<T> extends DoFn<T, T> {
    private final Level level;
    private final String prefix;
    private final boolean withTimestamp;
    private final boolean withWindow;
    private final boolean withPaneInfo;

    private LoggingFn(LogElements<T> transform) {
      this.level = transform.level;
      this.prefix = transform.prefix;
      this.withTimestamp = transform.withTimestamp;
      this.withWindow = transform.withWindow;
      this.withPaneInfo = transform.withPaneInfo;
    }

    @ProcessElement
    public void processElement(
        @Element T element,
        @DoFn.Timestamp Instant timestamp,
        BoundedWindow window,
        PaneInfo paneInfo,
        OutputReceiver<T> receiver) {
      if (isLoggingEnabled(level)) {
        log(
            level,
            formatForLogging(
                element,
                prefix,
                withTimestamp,
                withWindow,
                withPaneInfo,
                timestamp,
                window,
                paneInfo));
      }
      receiver.output(element);
    }
  }
}
