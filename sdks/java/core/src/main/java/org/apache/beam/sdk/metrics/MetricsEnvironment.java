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
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.annotations.Internal;
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
@Experimental(Kind.METRICS)
@Internal
public class MetricsEnvironment {

  private static final Logger LOG = LoggerFactory.getLogger(MetricsEnvironment.class);

  private static final AtomicBoolean METRICS_SUPPORTED = new AtomicBoolean(false);
  private static final AtomicBoolean REPORTED_MISSING_CONTAINER = new AtomicBoolean(false);

  private static final ThreadLocal<@Nullable MetricsContainer> CONTAINER_FOR_THREAD =
      new ThreadLocal<>();
  private static final AtomicReference<MetricsContainer> CONTAINER_GLOBAL =
      new AtomicReference<>(null);

  /** Set the global {@link MetricsContainer}. */
  public static void setGlobalContainer(@Nullable MetricsContainer container) {
    CONTAINER_GLOBAL.set(container);
  }

  /**
   * Set the {@link MetricsContainer} for the current thread.
   *
   * @return The previous container for the current thread.
   */
  public static @Nullable MetricsContainer setCurrentContainer(
      @Nullable MetricsContainer container) {
    MetricsContainer previous = CONTAINER_FOR_THREAD.get();
    if (container == null) {
      CONTAINER_FOR_THREAD.remove();
    } else {
      CONTAINER_FOR_THREAD.set(container);
    }
    return previous;
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

    private final @Nullable MetricsContainer oldContainer;

    private ScopedContainer(MetricsContainer newContainer) {
      this.oldContainer = setCurrentContainer(newContainer);
    }

    @Override
    public void close() throws IOException {
      setCurrentContainer(oldContainer);
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
    MetricsContainer container = CONTAINER_FOR_THREAD.get();
    if (container == null) {
      container = CONTAINER_GLOBAL.get();
    }
    if (container == null && REPORTED_MISSING_CONTAINER.compareAndSet(false, true)) {
      if (isMetricsSupported()) {
        LOG.error(
            "Unable to update metrics on the current thread. "
                + "Most likely caused by using metrics outside the managed work-execution thread.");
      } else {
        LOG.warn("Reporting metrics are not supported in the current execution environment.");
      }
    }
    return container;
  }
}
