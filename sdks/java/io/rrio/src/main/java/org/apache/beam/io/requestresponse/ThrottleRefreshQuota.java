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
package org.apache.beam.io.requestresponse;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.joda.time.Instant;

/**
 * {@link ThrottleRefreshQuota} refreshes a quota per {@link Instant} processing events emitting any
 * errors into an {@link ApiIOError} {@link PCollection}.
 */
class ThrottleRefreshQuota extends PTransform<PCollection<Instant>, PCollection<ApiIOError>> {

  // TODO: remove suppress warnings after configuration utilized.
  @SuppressWarnings({"unused"})
  private final Configuration configuration;

  private ThrottleRefreshQuota(Configuration configuration) {
    this.configuration = configuration;
  }

  @Override
  public PCollection<ApiIOError> expand(PCollection<Instant> input) {
    // TODO(damondouglas): expand in a later PR.
    return input.getPipeline().apply(Create.empty(TypeDescriptor.of(ApiIOError.class)));
  }

  @AutoValue
  abstract static class Configuration {

    @AutoValue.Builder
    abstract static class Builder {
      abstract Configuration build();
    }
  }
}
