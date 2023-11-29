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
package org.apache.beam.runners.dataflow.worker.windmill.work;

import java.util.Collection;
import java.util.function.Consumer;
import javax.annotation.CheckReturnValue;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.LatencyAttribution;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkItem;
import org.apache.beam.sdk.annotations.Internal;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

@FunctionalInterface
@CheckReturnValue
@Internal
public interface WorkItemProcessor {
  /**
   * Receives and processes {@link WorkItem}(s) wrapped in its {@link ProcessWorkItemClient}
   * processing context.
   *
   * @param computation the Computation that the Work belongs to.
   * @param inputDataWatermark Watermark of when the input data was received by the computation.
   * @param synchronizedProcessingTime Aggregate system watermark that also depends on each
   *     computation's received dependent system watermark value to propagate the system watermark
   *     downstream.
   * @param wrappedWorkItem A workItem and it's processing context, used to route subsequent
   *     WorkItem API (GetData, CommitWork) RPC calls to the same backend worker, where the WorkItem
   *     was returned from GetWork.
   * @param ackWorkItemQueued Called after an attempt to queue the work item for processing. Used to
   *     free up pending budget.
   * @param getWorkStreamLatencies Latencies per processing stage for the WorkItem for reporting
   *     back to Streaming Engine backend.
   */
  void processWork(
      String computation,
      @Nullable Instant inputDataWatermark,
      @Nullable Instant synchronizedProcessingTime,
      ProcessWorkItemClient wrappedWorkItem,
      Consumer<WorkItem> ackWorkItemQueued,
      Collection<LatencyAttribution> getWorkStreamLatencies);
}
