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
package org.apache.beam.runners.fnexecution.jobsubmission;

import com.google.auto.value.AutoValue;
import java.time.Instant;
import org.apache.beam.model.jobmanagement.v1.JobApi;
import org.apache.beam.vendor.grpc.v1p21p0.com.google.protobuf.Timestamp;

/** A state transition event. */
@AutoValue
public abstract class JobStateEvent {
  public static Builder builder() {
    return new AutoValue_JobStateEvent.Builder();
  }

  public abstract JobApi.JobState.Enum state();

  public abstract Instant timestamp();

  /** Convert to {@link JobApi.GetJobStateResponse}. */
  public JobApi.GetJobStateResponse toProto() {
    return JobApi.GetJobStateResponse.newBuilder()
        .setState(state())
        .setTimestamp(
            Timestamp.newBuilder()
                .setSeconds(timestamp().getEpochSecond())
                .setNanos(timestamp().getNano()))
        .build();
  }

  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder setState(JobApi.JobState.Enum state);

    abstract Builder setTimestamp(Instant timestamp);

    abstract JobStateEvent build();
  }
}
