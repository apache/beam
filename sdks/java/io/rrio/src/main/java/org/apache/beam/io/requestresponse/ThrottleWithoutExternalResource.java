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
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;

/**
 * {@link ThrottleWithoutExternalResource} throttles a {@link RequestT} {@link PCollection} emitting
 * a {@link RequestT} {@link PCollection} at a maximally configured rate, without using an external
 * resource.
 */
// TODO(damondouglas): expand what "without external resource" means with respect to "with external
//   resource" when the other throttle transforms implemented.
//   See: https://github.com/apache/beam/issues/28932
class ThrottleWithoutExternalResource<RequestT>
    extends PTransform<PCollection<RequestT>, PCollection<RequestT>> {

  // TODO(damondouglas): remove suppress warnings when finally utilized in a future PR.
  @SuppressWarnings({"unused"})
  private final Configuration<RequestT> configuration;

  private ThrottleWithoutExternalResource(Configuration<RequestT> configuration) {
    this.configuration = configuration;
  }

  @Override
  public PCollection<RequestT> expand(PCollection<RequestT> input) {
    // TODO(damondouglas): expand in a future PR.
    return input;
  }

  @AutoValue
  abstract static class Configuration<RequestT> {

    @AutoValue.Builder
    abstract static class Builder<RequestT> {
      abstract Configuration<RequestT> build();
    }
  }
}
