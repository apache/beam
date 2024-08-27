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
import javax.annotation.CheckReturnValue;
import org.apache.beam.runners.dataflow.worker.streaming.Watermarks;
import org.apache.beam.runners.dataflow.worker.streaming.Work;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.LatencyAttribution;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkItem;
import org.apache.beam.sdk.annotations.Internal;

@FunctionalInterface
@CheckReturnValue
@Internal
public interface WorkItemScheduler {
  /**
   * Schedule {@link WorkItem}(s).
   *
   * @param workItem {@link WorkItem} to be processed.
   * @param watermarks processing watermarks for the workItem.
   * @param processingContext for processing the workItem.
   * @param getWorkStreamLatencies Latencies per processing stage for the WorkItem for reporting
   *     back to Streaming Engine backend.
   */
  void scheduleWork(
      WorkItem workItem,
      Watermarks watermarks,
      Work.ProcessingContext processingContext,
      Collection<LatencyAttribution> getWorkStreamLatencies);
}
