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
package org.apache.beam.testinfra.pipelines.dataflow;

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import com.google.dataflow.v1beta3.GetJobRequest;
import com.google.dataflow.v1beta3.JobView;
import com.google.events.cloud.dataflow.v1beta3.Job;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.TypeDescriptor;

/** Various methods to create Dataflow API requests. */
@Internal
public final class DataflowRequests {

  /** Creates {@link GetJobRequest}s from {@link Job}s with {@link JobView#JOB_VIEW_ALL}. */
  public static MapElements<Job, GetJobRequest> jobRequestsFromEventsViewAll() {
    return jobRequests(JobView.JOB_VIEW_ALL);
  }

  /** Creates {@link GetJobRequest}s from {@link Job}s with the assigned {@link JobView}. */
  public static MapElements<Job, GetJobRequest> jobRequests(JobView view) {
    return MapElements.into(TypeDescriptor.of(GetJobRequest.class))
        .via(
            event -> {
              Job safeEvent = checkStateNotNull(event);
              return GetJobRequest.newBuilder()
                  .setJobId(safeEvent.getId())
                  .setLocation(safeEvent.getLocation())
                  .setProjectId(safeEvent.getProjectId())
                  .setView(view)
                  .build();
            });
  }
}
