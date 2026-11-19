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
package org.apache.beam.sdk.util;

import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.joda.time.Instant;

@Internal
@FunctionalInterface
public interface OutputBuilderSupplier {
  // Returns WindowedValues.Builder so that downstream can setReceiver (when tag is specified)
  // but we need the value at a minimum in order to fix the type variable
  <OutputT> WindowedValues.Builder<OutputT> builder(OutputT value);

  /** Builds a {@link WindowedValue} for {@code value} using the supplier's template metadata. */
  default <OutputT> WindowedValue<OutputT> build(OutputT value) {
    return builder(value).build();
  }

  /**
   * Builds a {@link WindowedValue} for {@code value} with the given {@code timestamp} using the
   * supplier's template metadata.
   */
  default <OutputT> WindowedValue<OutputT> buildWithTimestamp(OutputT value, Instant timestamp) {
    return builder(value).setTimestamp(timestamp).build();
  }
}
