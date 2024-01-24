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
package org.apache.beam.fn.harness.control;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.beam.fn.harness.control.ProcessBundleHandler.BundleProcessor;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;

/**
 * Reports metrics related to bundle processing.
 *
 * <p>Each method is guaranteed to be invoked exclusively. The call flow for a single bundle is
 * always: {@link #updateIntermediateMonitoringData}* -> {@link #updateFinalMonitoringData} ->
 * {@link #reset}. After {@link #reset}, the progress reporter will be used for another thread.
 */
public interface BundleProgressReporter {

  /** Maintains a set of {@link BundleProgressReporter}s. */
  interface Registrar {
    /** Adds the reporter to be reported on. It is an error to add a reporter more than once. */
    void register(BundleProgressReporter reporter);
  }

  /**
   * Update the monitoring data for a bundle that is currently being processed.
   *
   * <p>Must be invoked while holding the {@link BundleProcessor#getProgressRequestLock}.
   */
  void updateIntermediateMonitoringData(Map<String, ByteString> monitoringData);

  /**
   * Update the monitoring data for a bundle that has finished processing.
   *
   * <p>Must be invoked from the main bundle processing thread and while holding the {@link
   * BundleProcessor#getProgressRequestLock}.
   */
  void updateFinalMonitoringData(Map<String, ByteString> monitoringData);

  /**
   * Reset the monitoring data after a bundle has finished processing to be re-used for a future
   * bundle.
   *
   * <p>Must be invoked from the main bundle processing thread and while holding the {@link
   * BundleProcessor#getProgressRequestLock}.
   */
  void reset();

  /**
   * An in-memory bundle progress reporter and registrar.
   *
   * <p>Synchronization must be provided between {@link #register} and {@link
   * BundleProgressReporter} methods to ensure that all registered reporters are seen.
   */
  @NotThreadSafe
  class InMemory implements BundleProgressReporter, Registrar {
    private final List<BundleProgressReporter> reporters = new ArrayList<>();

    @Override
    public void register(BundleProgressReporter reporter) {
      if (reporters.contains(reporter)) {
        throw new IllegalStateException(reporter + " was being added multiple times.");
      }
      reporters.add(reporter);
    }

    @Override
    public void updateIntermediateMonitoringData(Map<String, ByteString> monitoringData) {
      for (BundleProgressReporter reporter : reporters) {
        reporter.updateIntermediateMonitoringData(monitoringData);
      }
    }

    @Override
    public void updateFinalMonitoringData(Map<String, ByteString> monitoringData) {
      for (BundleProgressReporter reporter : reporters) {
        reporter.updateFinalMonitoringData(monitoringData);
      }
    }

    @Override
    public void reset() {
      for (BundleProgressReporter reporter : reporters) {
        reporter.reset();
      }
    }
  }
}
