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

import com.google.auto.value.AutoValue;
import com.google.dataflow.v1beta3.JobMetrics;
import com.google.protobuf.Timestamp;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCaseFormat;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.CaseFormat;
import org.joda.time.Instant;

@DefaultSchema(AutoValueSchema.class)
@AutoValue
@SchemaCaseFormat(CaseFormat.LOWER_CAMEL)
public abstract class JobMetricsWithAppendedDetails {

  public static Builder builder() {
    return new AutoValue_JobMetricsWithAppendedDetails.Builder();
  }

  public abstract String getJobId();

  public abstract Instant getJobCreateTime();

  public abstract JobMetrics getJobMetrics();

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setJobId(String value);

    public abstract Builder setJobCreateTime(Instant value);

    public abstract Builder setJobMetrics(JobMetrics value);

    public abstract JobMetricsWithAppendedDetails build();
  }
}
