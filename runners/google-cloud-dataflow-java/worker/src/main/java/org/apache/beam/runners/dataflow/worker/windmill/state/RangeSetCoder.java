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
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.SetCoder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Range;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.RangeSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.TreeRangeSet;

class RangeSetCoder<T extends Comparable<?>> extends CustomCoder<RangeSet<T>> {
  private final SetCoder<Range<T>> rangesCoder;

  RangeSetCoder(Coder<T> boundCoder) {
    this.rangesCoder = SetCoder.of(new RangeCoder<>(boundCoder));
  }

  @Override
  public void encode(RangeSet<T> value, OutputStream outStream) throws IOException {
    rangesCoder.encode(value.asRanges(), outStream);
  }

  @Override
  public RangeSet<T> decode(InputStream inStream) throws IOException {
    return TreeRangeSet.create(rangesCoder.decode(inStream));
  }
}
