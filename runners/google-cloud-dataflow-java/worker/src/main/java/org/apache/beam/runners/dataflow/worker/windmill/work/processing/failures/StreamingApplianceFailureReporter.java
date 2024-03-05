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

import java.util.function.Function;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.ReportStatsRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.ReportStatsResponse;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.EvictingQueue;

/** Implementation of {@link FailureReporter} that reports failures to Streaming Appliance. */
@ThreadSafe
@Internal
public final class StreamingApplianceFailureReporter extends FailureReporter {
  private final Function<ReportStatsRequest, ReportStatsResponse> reportStatsFn;

  private StreamingApplianceFailureReporter(
      int maxStackTraceDepthToReport,
      EvictingQueue<String> pendingFailuresToReport,
      Function<ReportStatsRequest, ReportStatsResponse> reportStatsFn) {
    super(maxStackTraceDepthToReport, pendingFailuresToReport);
    this.reportStatsFn = reportStatsFn;
  }

  public static StreamingApplianceFailureReporter create(
      int maxFailuresToReportInUpdate,
      int maxStackTraceDepthToReport,
      Function<ReportStatsRequest, ReportStatsResponse> reportStatsFn) {
    return new StreamingApplianceFailureReporter(
        maxStackTraceDepthToReport,
        EvictingQueue.create(maxFailuresToReportInUpdate),
        reportStatsFn);
  }

  @Override
  public boolean shouldRetryLocally(String computationId, Windmill.WorkItem work) {
    ReportStatsResponse response =
        reportStatsFn.apply(
            ReportStatsRequest.newBuilder()
                .setComputationId(computationId)
                .setKey(work.getKey())
                .setShardingKey(work.getShardingKey())
                .setWorkToken(work.getWorkToken())
                .build());
    return !response.getFailed();
  }
}
