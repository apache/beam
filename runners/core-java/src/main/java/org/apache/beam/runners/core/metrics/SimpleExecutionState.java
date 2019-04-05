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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker.ExecutionState;
import org.apache.beam.vendor.guava.v20_0.com.google.common.annotations.VisibleForTesting;
import org.joda.time.Duration;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple state class which collects the totalMillis spent in the state. Allows storing an arbitrary
 * set of key value labels in the object which can be retrieved later for reporting purposes via
 * getLabels().
 */
public class SimpleExecutionState extends ExecutionState {
  private long totalMillis = 0;
  private HashMap<String, String> labelsMetadata;
  private String urn;

  private static final Logger LOG = LoggerFactory.getLogger(SimpleExecutionState.class);

  private static final PeriodFormatter DURATION_FORMATTER =
      new PeriodFormatterBuilder()
          .appendDays()
          .appendSuffix("d")
          .minimumPrintedDigits(2)
          .appendHours()
          .appendSuffix("h")
          .printZeroAlways()
          .appendMinutes()
          .appendSuffix("m")
          .appendSeconds()
          .appendSuffix("s")
          .toFormatter();

  /**
   * @param stateName A state name to be used in lull logging when stuck in a state.
   * @param urn A optional string urn for an execution time metric.
   * @param labelsMetadata arbitrary metadata to use for reporting purposes.
   */
  public SimpleExecutionState(
      String stateName, String urn, HashMap<String, String> labelsMetadata) {
    super(stateName);
    this.urn = urn;
    this.labelsMetadata = labelsMetadata;
    if (this.labelsMetadata == null) {
      this.labelsMetadata = new HashMap<String, String>();
    }
  }

  public String getUrn() {
    return this.urn;
  }

  public Map<String, String> getLabels() {
    return Collections.unmodifiableMap(labelsMetadata);
  }

  @Override
  public void takeSample(long millisSinceLastSample) {
    this.totalMillis += millisSinceLastSample;
  }

  public long getTotalMillis() {
    return totalMillis;
  }

  @VisibleForTesting
  public String getLullMessage(Thread trackedThread, Duration millis) {
    // TODO(ajamato): Share getLullMessage code with DataflowExecutionState.
    String userStepName =
        this.labelsMetadata.getOrDefault(MonitoringInfoConstants.Labels.PTRANSFORM, null);
    StringBuilder message = new StringBuilder();
    message.append("Processing stuck");
    if (userStepName != null) {
      message.append(" in step ").append(userStepName);
    }
    message
        .append(" for at least ")
        .append(formatDuration(millis))
        .append(" without outputting or completing in state ")
        .append(getStateName());
    message.append("\n");

    StackTraceElement[] fullTrace = trackedThread.getStackTrace();
    for (StackTraceElement e : fullTrace) {
      message.append("  at ").append(e).append("\n");
    }
    return message.toString();
  }

  @Override
  public void reportLull(Thread trackedThread, long millis) {
    LOG.warn(getLullMessage(trackedThread, Duration.millis(millis)));
  }

  @VisibleForTesting
  static String formatDuration(Duration duration) {
    return DURATION_FORMATTER.print(duration.toPeriod());
  }
}
