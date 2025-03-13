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
package org.apache.beam.runners.dataflow.worker.windmill.state;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.BoundType;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Range;
import org.checkerframework.checker.nullness.qual.Nullable;

@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
/** Coder for closed-open ranges. */
class RangeCoder<T extends Comparable<?>> extends StructuredCoder<Range<T>> {
  private final Coder<T> boundCoder;

  RangeCoder(Coder<T> boundCoder) {
    this.boundCoder = NullableCoder.of(boundCoder);
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return Lists.newArrayList(boundCoder);
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    boundCoder.verifyDeterministic();
  }

  @Override
  public void encode(Range<T> value, OutputStream outStream) throws IOException {
    Preconditions.checkState(
        value.lowerBoundType().equals(BoundType.CLOSED), "unexpected range " + value);
    Preconditions.checkState(
        value.upperBoundType().equals(BoundType.OPEN), "unexpected range " + value);
    boundCoder.encode(value.hasLowerBound() ? value.lowerEndpoint() : null, outStream);
    boundCoder.encode(value.hasUpperBound() ? value.upperEndpoint() : null, outStream);
  }

  @Override
  public Range<T> decode(InputStream inStream) throws IOException {
    @Nullable T lower = boundCoder.decode(inStream);
    @Nullable T upper = boundCoder.decode(inStream);
    if (lower == null) {
      return upper != null ? Range.lessThan(upper) : Range.all();
    } else if (upper == null) {
      return Range.atLeast(lower);
    } else {
      return Range.closedOpen(lower, upper);
    }
  }
}
