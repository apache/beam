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
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkItem;
import org.apache.beam.sdk.annotations.Internal;

/** Implementation of {@link FailureTracker} that reports failures to Streaming Engine. */
@ThreadSafe
@Internal
public final class StreamingEngineFailureTracker extends FailureTracker {

  private StreamingEngineFailureTracker(
      int maxFailuresToReportInUpdate, int maxStackTraceDepthToReport) {
    super(maxFailuresToReportInUpdate, maxStackTraceDepthToReport);
  }

  public static StreamingEngineFailureTracker create(
      int maxFailuresToReportInUpdate, int maxStackTraceDepthToReport) {
    return new StreamingEngineFailureTracker(
        maxFailuresToReportInUpdate, maxStackTraceDepthToReport);
  }

  @Override
  protected boolean reportFailureInternal(String computationId, WorkItem work) {
    return true;
  }
}
