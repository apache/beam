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
package org.apache.beam.runners.jobsubmission;

import com.google.auto.value.AutoValue;
import org.apache.beam.model.pipeline.v1.RunnerApi.Pipeline;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.Struct;

/** A job that has been prepared, but not invoked. */
@AutoValue
public abstract class JobPreparation {
  public static Builder builder() {
    return new AutoValue_JobPreparation.Builder();
  }

  public abstract String id();

  public abstract Pipeline pipeline();

  public abstract Struct options();

  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder setId(String id);

    abstract Builder setPipeline(Pipeline pipeline);

    abstract Builder setOptions(Struct options);

    abstract JobPreparation build();
  }
}
