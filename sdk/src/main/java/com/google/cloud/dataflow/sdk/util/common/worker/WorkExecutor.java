/*******************************************************************************
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 ******************************************************************************/

package com.google.cloud.dataflow.sdk.util.common.worker;

import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.util.common.Metric;
import com.google.cloud.dataflow.sdk.util.common.Metric.DoubleMetric;

import com.sun.management.OperatingSystemMXBean;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Abstract executor for WorkItem tasks.
 */
@SuppressWarnings("resource")
public abstract class WorkExecutor implements AutoCloseable {
  /** The output counters for this task. */
  private final CounterSet outputCounters;

  /**
   * OperatingSystemMXBean for reporting CPU usage.
   *
   * Uses com.sun.management.OperatingSystemMXBean instead of
   * java.lang.management.OperatingSystemMXBean because the former supports
   * getProcessCpuLoad().
   */
  private final OperatingSystemMXBean os;

  /**
   * Constructs a new WorkExecutor task.
   */
  public WorkExecutor(CounterSet outputCounters) {
    this.outputCounters = outputCounters;
    this.os = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
  }

  /**
   * Returns the set of output counters for this task.
   */
  public CounterSet getOutputCounters() {
    return outputCounters;
  }

  /**
   * Returns a collection of output metrics for this task.
   */
  public Collection<Metric<?>> getOutputMetrics() {
    List<Metric<?>> outputMetrics = new ArrayList<>();
    outputMetrics.add(new DoubleMetric("CPU", os.getProcessCpuLoad()));
    // More metrics as needed.
    return outputMetrics;
  }

  /**
   * Executes the task.
   */
  public abstract void execute() throws Exception;

  /**
   * Returns the worker's current progress.
   */
  public Reader.Progress getWorkerProgress() throws Exception {
    // By default, return null indicating worker progress not available.
    return null;
  }

  /**
   * See {@link Reader.ReaderIterator#requestFork}. Makes sense only for tasks which read input.
   */
  public Reader.ForkResult requestFork(Reader.ForkRequest forkRequest) throws Exception {
    // By default, fork is unsupported.
    return null;
  }

  @Override
  public void close() throws Exception {
    // By default, nothing to close or shut down.
  }
}
