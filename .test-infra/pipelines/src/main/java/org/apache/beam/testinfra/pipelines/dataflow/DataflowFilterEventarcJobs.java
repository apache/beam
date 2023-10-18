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

import com.google.auto.value.AutoValue;
import com.google.events.cloud.dataflow.v1beta3.Job;
import com.google.events.cloud.dataflow.v1beta3.JobState;
import com.google.events.cloud.dataflow.v1beta3.JobType;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.NonNull;

/** Filters Eventarc {@link Job}s. */
@Internal
@AutoValue
public abstract class DataflowFilterEventarcJobs
    extends PTransform<@NonNull PCollection<Job>, @NonNull PCollection<Job>> {

  public static Builder builder() {
    return new AutoValue_DataflowFilterEventarcJobs.Builder();
  }

  public abstract List<JobState> getIncludeJobStates();

  public abstract List<JobState> getExcludeJobStates();

  public abstract JobType getIncludeJobType();

  abstract Builder toBuilder();

  @Override
  public @NonNull PCollection<Job> expand(PCollection<Job> input) {
    return input.apply(
        Filter.by(
            job -> {
              Job safeJob =
                  checkStateNotNull(job, "null Job input in %s", DataflowFilterEventarcJobs.class);
              boolean matchesIncludes =
                  !getIncludeJobStates().isEmpty()
                      && getIncludeJobStates().contains(safeJob.getCurrentState());
              boolean matchesExcludes =
                  !getExcludeJobStates().isEmpty()
                      && getExcludeJobStates().contains(safeJob.getCurrentState());
              boolean matchesJobType = getIncludeJobType().equals(job.getType());
              return matchesIncludes && !matchesExcludes && matchesJobType;
            }));
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setIncludeJobStates(List<JobState> newIncludeJobStates);

    abstract Optional<List<JobState>> getIncludeJobStates();

    public abstract Builder setExcludeJobStates(List<JobState> newExcludeJobStates);

    abstract Optional<List<JobState>> getExcludeJobStates();

    public abstract Builder setIncludeJobType(JobType newIncludeJobType);

    abstract JobType getIncludeJobType();

    public abstract DataflowFilterEventarcJobs autoBuild();

    public Builder terminatedOnly() {
      return setIncludeJobStates(
          ImmutableList.of(JobState.JOB_STATE_DONE, JobState.JOB_STATE_CANCELLED));
    }

    public final DataflowFilterEventarcJobs build() {
      if (!getIncludeJobStates().isPresent()) {
        setIncludeJobStates(Collections.emptyList());
      }
      if (!getExcludeJobStates().isPresent()) {
        setExcludeJobStates(Collections.emptyList());
      }
      if (getIncludeJobType().equals(JobType.JOB_TYPE_UNKNOWN)) {
        throw new IllegalStateException(
            String.format(
                "illegal %s: %s", JobType.class.getSimpleName(), JobType.JOB_TYPE_UNKNOWN));
      }

      return autoBuild();
    }
  }
}
