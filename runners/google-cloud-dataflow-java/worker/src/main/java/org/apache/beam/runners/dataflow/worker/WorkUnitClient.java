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
package org.apache.beam.runners.dataflow.worker;

import com.google.api.services.dataflow.model.WorkItem;
import com.google.api.services.dataflow.model.WorkItemServiceState;
import com.google.api.services.dataflow.model.WorkItemStatus;
import java.io.IOException;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Optional;

/** Abstract base class describing a client for WorkItem work units. */
interface WorkUnitClient {
  /**
   * Returns a new WorkItem unit for this Worker to work on or null if no work item is available.
   */
  Optional<WorkItem> getWorkItem() throws IOException;

  /**
   * Returns a new global streaming config WorkItem, or returns {@link Optional#absent()} if no work
   * was found.
   */
  Optional<WorkItem> getGlobalStreamingConfigWorkItem() throws IOException;

  /**
   * Returns a streaming config WorkItem for the given computation, or returns {@link
   * Optional#absent()} if no work was found.
   */
  Optional<WorkItem> getStreamingConfigWorkItem(String computationId) throws IOException;

  /**
   * Reports a {@link WorkItemStatus} for an assigned {@link WorkItem}.
   *
   * @param workItemStatus the status to report
   * @return a {@link WorkItemServiceState} (e.g. a new stop position)
   */
  WorkItemServiceState reportWorkItemStatus(WorkItemStatus workItemStatus) throws IOException;
}
