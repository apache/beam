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

/**
 * {@link ThrottleEnqueue} enqueues {@link RequestT} elements yielding an {@link ApiIOError} {@link
 * PCollection} of any enqueue errors.
 */
class ThrottleEnqueue<RequestT> extends PTransform<PCollection<RequestT>, PCollection<ApiIOError>> {

  @SuppressWarnings({"unused"})
  private final Configuration<RequestT> configuration;

  private ThrottleEnqueue(Configuration<RequestT> configuration) {
    this.configuration = configuration;
  }

  /** Configuration details for {@link ThrottleEnqueue}. */
  @AutoValue
  abstract static class Configuration<RequestT> {

    static <RequestT> Builder<RequestT> builder() {
      return new AutoValue_ThrottleEnqueue_Configuration.Builder<>();
    }

    abstract Builder<RequestT> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<RequestT> {

      abstract Configuration<RequestT> build();
    }
  }

  @Override
  public PCollection<ApiIOError> expand(PCollection<RequestT> input) {
    // TODO(damondouglas): expand in a future PR.
    return input.getPipeline().apply(Create.empty(TypeDescriptor.of(ApiIOError.class)));
  }
}
