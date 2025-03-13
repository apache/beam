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
package org.apache.beam.runners.core.metrics;

import java.util.Date;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsLogger extends MetricsContainerImpl {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsLogger.class);

  Lock reportingLocK = new ReentrantLock();
  AtomicLong lastReportedMillis = new AtomicLong(System.currentTimeMillis());
  @Nullable MetricsContainerImpl lastMetricsSnapshot = null;

  public MetricsLogger(@Nullable String stepName) {
    super(stepName);
  }

  public String generateLogMessage(
      String header, Set<String> allowedMetricUrns, long lastReported) {
    MetricsContainerImpl nextMetricsSnapshot = new MetricsContainerImpl(this.stepName);
    nextMetricsSnapshot.update(this);
    MetricsContainerImpl deltaContainer =
        MetricsContainerImpl.deltaContainer(lastMetricsSnapshot, nextMetricsSnapshot);

    StringBuilder logMessage = new StringBuilder();
    logMessage.append(header);
    logMessage.append(deltaContainer.getCumulativeString(allowedMetricUrns));
    logMessage.append(String.format("(last reported at %s)%n", new Date(lastReported)));

    lastMetricsSnapshot = nextMetricsSnapshot;
    return logMessage.toString();
  }

  public void tryLoggingMetrics(
      String header, Set<String> allowedMetricUrns, long minimumLoggingFrequencyMillis) {

    if (reportingLocK.tryLock()) {
      try {
        long currentTimeMillis = System.currentTimeMillis();
        long lastReported = lastReportedMillis.get();
        if (currentTimeMillis - lastReported > minimumLoggingFrequencyMillis) {
          LOG.info(generateLogMessage(header, allowedMetricUrns, lastReported));
          lastReportedMillis.set(currentTimeMillis);
        }
      } finally {
        reportingLocK.unlock();
      }
    }
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  @Override
  public boolean equals(@Nullable Object object) {
    if (object instanceof MetricsLogger) {
      return super.equals(object);
    }
    return false;
  }
}
