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
package org.apache.beam.runners.dataflow.worker.windmill.work.processing.failures;

import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.ReportStatsRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.ReportStatsResponse;
import org.apache.beam.sdk.annotations.Internal;

/** Implementation of {@link FailureTracker} that reports failures to Streaming Appliance. */
@ThreadSafe
@Internal
public final class StreamingApplianceFailureTracker extends FailureTracker {
  private final StreamingApplianceStatsReporter statsReporter;

  private StreamingApplianceFailureTracker(
      int maxFailuresToReportInUpdate,
      int maxStackTraceDepthToReport,
      StreamingApplianceStatsReporter statsReporter) {
    super(maxFailuresToReportInUpdate, maxStackTraceDepthToReport);
    this.statsReporter = statsReporter;
  }

  public static StreamingApplianceFailureTracker create(
      int maxFailuresToReportInUpdate,
      int maxStackTraceDepthToReport,
      StreamingApplianceStatsReporter statsReporter) {
    return new StreamingApplianceFailureTracker(
        maxFailuresToReportInUpdate, maxStackTraceDepthToReport, statsReporter);
  }

  @Override
  public boolean reportFailureInternal(String computationId, Windmill.WorkItem work) {
    ReportStatsResponse response =
        statsReporter.reportStats(
            ReportStatsRequest.newBuilder()
                .setComputationId(computationId)
                .setKey(work.getKey())
                .setShardingKey(work.getShardingKey())
                .setWorkToken(work.getWorkToken())
                .build());
    return !response.getFailed();
  }
}
