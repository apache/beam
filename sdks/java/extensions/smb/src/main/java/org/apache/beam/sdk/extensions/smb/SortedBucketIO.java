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
package org.apache.beam.sdk.extensions.smb;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.extensions.smb.SMBCoGbkResult.ToFinalResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;

public class SortedBucketIO {

  public static class TwoSourceJoinResult<ValueT1, ValueT2>
      extends ToFinalResult<KV<Iterable<ValueT1>, Iterable<ValueT2>>> {
    private final Coder<ValueT1> leftCoder;
    private final Coder<ValueT2> rightCoder;

    public TwoSourceJoinResult(Coder<ValueT1> leftCoder, Coder<ValueT2> rightCoder) {
      this.leftCoder = leftCoder;
      this.rightCoder = rightCoder;
    }

    @Override
    public KV<Iterable<ValueT1>, Iterable<ValueT2>> apply(SMBCoGbkResult input) {
      return KV.of(
          input.getValuesForTag(new TupleTag<>("left")),
          input.getValuesForTag(new TupleTag<>("right")));
    }

    @Override
    public Coder<KV<Iterable<ValueT1>, Iterable<ValueT2>>> resultCoder() {
      return KvCoder.of(
          NullableCoder.of(IterableCoder.of(leftCoder)),
          NullableCoder.of(IterableCoder.of(rightCoder)));
    }
  }
}
