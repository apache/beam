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

import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages and provides the metrics container associated with each thread.
 *
 * <p>Users should not interact directly with this class. Instead, use {@link Metrics} and the
 * returned objects to create and modify metrics.
 *
 * <p>The runner should create {@link MetricsContainer} for each context in which metrics are
 * reported (by step and name) and call {@link #setMetricsContainer} before invoking any code that
 * may update metrics within that step.
 *
 * <p>The runner should call {@link #unsetMetricsContainer} (or {@link #setMetricsContainer} back to
 * the previous value) when exiting code that set the metrics container.
 */
public class MetricsEnvironment {

  private static final Logger LOGGER = LoggerFactory.getLogger(MetricsContainer.class);

  private static final AtomicBoolean METRICS_SUPPORTED = new AtomicBoolean(false);
  private static final AtomicBoolean REPORTED_MISSING_CONTAINER = new AtomicBoolean(false);

  private static final ThreadLocal<MetricsContainer> CONTAINER_FOR_THREAD =
      new ThreadLocal<MetricsContainer>();

  /** Set the {@link MetricsContainer} for the current thread. */
  public static void setMetricsContainer(MetricsContainer container) {
    CONTAINER_FOR_THREAD.set(container);
  }


  /** Clear the {@link MetricsContainer} for the current thread. */
  public static void unsetMetricsContainer() {
    CONTAINER_FOR_THREAD.remove();
  }

  /** Called by the run to indicate whether metrics reporting is supported. */
  public static void setMetricsSupported(boolean supported) {
    METRICS_SUPPORTED.set(supported);
  }

  /**
   * Return the {@link MetricsContainer} for the current thread.
   *
   * <p>May return null if metrics are not supported by the current runner or if the current thread
   * is not a work-execution thread. The first time this happens in a given thread it will log a
   * diagnostic message.
   */
  @Nullable
  public static MetricsContainer getCurrentContainer() {
    MetricsContainer container = CONTAINER_FOR_THREAD.get();
    if (container == null && REPORTED_MISSING_CONTAINER.compareAndSet(false, true)) {
      if (METRICS_SUPPORTED.get()) {
        LOGGER.error(
            "Unable to update metrics on the current thread. "
                + "Most likely caused by using metrics outside the managed work-execution thread.");
      } else {
        LOGGER.warn("Reporting metrics are not supported in the current execution environment.");
      }
    }
    return container;
  }
}
