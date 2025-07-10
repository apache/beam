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
package org.apache.beam.sdk.metrics;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.util.StringUtils;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages and provides the metrics container associated with each thread.
 *
 * <p>Users should not interact directly with this class. Instead, use {@link Metrics} and the
 * returned objects to create and modify metrics.
 *
 * <p>The runner should create a {@link MetricsContainer} for each context in which metrics are
 * reported (by step and name) and call {@link #setCurrentContainer} before invoking any code that
 * may update metrics within that step. It should call {@link #setCurrentContainer} again to restore
 * the previous container.
 *
 * <p>Alternatively, the runner can use {@link #scopedMetricsContainer(MetricsContainer)} to set the
 * container for the current thread and get a {@link Closeable} that will restore the previous
 * container when closed.
 */
@Internal
public class MetricsEnvironment {

  private static final Logger LOG = LoggerFactory.getLogger(MetricsEnvironment.class);

  private static final AtomicBoolean METRICS_SUPPORTED = new AtomicBoolean(false);
  private static final AtomicBoolean REPORTED_MISSING_CONTAINER = new AtomicBoolean(false);

  @SuppressWarnings("type.argument") // object guaranteed to be non-null
  private static final ThreadLocal<@NonNull MetricsContainerHolder> CONTAINER_FOR_THREAD =
      ThreadLocal.withInitial(MetricsContainerHolder::new);

  private static final AtomicReference<@Nullable MetricsContainer> PROCESS_WIDE_METRICS_CONTAINER =
      new AtomicReference<>();

  /** Returns the container holder for the current thread. */
  public static MetricsEnvironmentState getMetricsEnvironmentStateForCurrentThread() {
    return CONTAINER_FOR_THREAD.get();
  }

  /**
   * Set the {@link MetricsContainer} for the current thread.
   *
   * @return The previous container for the current thread.
   */
  public static @Nullable MetricsContainer setCurrentContainer(
      @Nullable MetricsContainer container) {
    MetricsContainerHolder holder = CONTAINER_FOR_THREAD.get();
    @Nullable MetricsContainer previous = holder.container;
    holder.container = container;
    return previous;
  }

  /**
   * Set the {@link MetricsContainer} for the current process.
   *
   * @return The previous container for the current process.
   */
  public static @Nullable MetricsContainer setProcessWideContainer(
      @Nullable MetricsContainer container) {
    return PROCESS_WIDE_METRICS_CONTAINER.getAndSet(container);
  }

  /** Called by the run to indicate whether metrics reporting is supported. */
  public static void setMetricsSupported(boolean supported) {
    METRICS_SUPPORTED.set(supported);
  }

  /** Indicates whether metrics reporting is supported. */
  public static boolean isMetricsSupported() {
    return METRICS_SUPPORTED.get();
  }

  /**
   * Set the {@link MetricsContainer} for the current thread.
   *
   * @return A {@link Closeable} that will reset the current container to the previous {@link
   *     MetricsContainer} when closed.
   */
  public static Closeable scopedMetricsContainer(MetricsContainer container) {
    return new ScopedContainer(container);
  }

  private static class ScopedContainer implements Closeable {
    private final MetricsContainerHolder holder;
    private final @Nullable MetricsContainer oldContainer;

    private ScopedContainer(MetricsContainer newContainer) {
      // It is safe to cache the thread-local holder because it never changes for the thread.
      holder = CONTAINER_FOR_THREAD.get();
      this.oldContainer = holder.container;
      holder.container = newContainer;
    }

    @Override
    public void close() throws IOException {
      holder.container = oldContainer;
    }
  }

  /**
   * Return the {@link MetricsContainer} for the current thread.
   *
   * <p>May return null if metrics are not supported by the current runner or if the current thread
   * is not a work-execution thread. The first time this happens in a given thread it will log a
   * diagnostic message.
   */
  public static @Nullable MetricsContainer getCurrentContainer() {
    MetricsContainer container = CONTAINER_FOR_THREAD.get().container;
    if (container == null && REPORTED_MISSING_CONTAINER.compareAndSet(false, true)) {
      if (isMetricsSupported()) {
        LOG.error(
            "Unable to update metrics on the current thread. Most likely caused by using metrics "
                + "outside the managed work-execution thread:\n  {}",
            StringUtils.arrayToNewlines(Thread.currentThread().getStackTrace(), 10));
      } else {
        // rate limiting this log as it can be emitted each time metrics incremented
        LOG.warn(
            "Reporting metrics are not supported in the current execution environment:\n  {}",
            StringUtils.arrayToNewlines(Thread.currentThread().getStackTrace(), 10));
      }
    }
    return container;
  }

  /** Return the {@link MetricsContainer} for the current process. */
  public static @Nullable MetricsContainer getProcessWideContainer() {
    return PROCESS_WIDE_METRICS_CONTAINER.get();
  }

  public static class MetricsContainerHolder implements MetricsEnvironmentState {
    private @Nullable MetricsContainer container = null;

    @Override
    public @Nullable MetricsContainer activate(@Nullable MetricsContainer metricsContainer) {
      MetricsContainer old = container;
      container = metricsContainer;
      return old;
    }
  }

  /**
   * Set the {@link MetricsContainer} for the associated {@link MetricsEnvironment}.
   *
   * @return The previous container for the associated {@link MetricsEnvironment}.
   */
  public interface MetricsEnvironmentState {
    @Nullable
    MetricsContainer activate(@Nullable MetricsContainer metricsContainer);
  }
}
