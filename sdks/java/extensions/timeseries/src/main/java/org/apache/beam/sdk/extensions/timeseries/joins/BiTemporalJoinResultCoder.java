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
package org.apache.beam.sdk.extensions.timeseries.joins;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeParameter;

public class BiTemporalJoinResultCoder<K, V1, V2>
    extends StructuredCoder<BiTemporalJoinResult<K, V1, V2>> {

  private final Coder<V1> leftCoder;
  private final Coder<V2> rightCoder;
  private final Coder<K> keyCoder;

  public BiTemporalJoinResultCoder(Coder<K> keyCoder, Coder<V1> leftCoder, Coder<V2> rightCoder) {
    this.leftCoder = NullableCoder.of(leftCoder);
    this.rightCoder = NullableCoder.of(rightCoder);
    this.keyCoder = NullableCoder.of(keyCoder);
  }

  public Coder<V1> getLeftCoder() {
    return leftCoder;
  }

  public Coder<V2> getRightCoder() {
    return rightCoder;
  }

  public Coder<K> getKeyCoder() {
    return keyCoder;
  }

  public static <K, V1, V2> BiTemporalJoinResultCoder<K, V1, V2> of(
      Coder<K> keyCoder, Coder<V1> leftCoder, Coder<V2> rightCoder) {

    return new BiTemporalJoinResultCoder<K, V1, V2>(keyCoder, leftCoder, rightCoder);
  }

  @Override
  public void encode(BiTemporalJoinResult<K, V1, V2> value, OutputStream outStream)
      throws IOException {

    if (value == null) {
      throw new CoderException("cannot encode a null");
    }

    if (value.getKey() == null) {
      throw new CoderException("cannot encode a null key");
    } else {
      this.keyCoder.encode(value.getKey(), outStream);
    }

    NullableCoder.of(TimestampedValue.TimestampedValueCoder.of(this.leftCoder))
        .encode(value.getLeftData(), outStream);

    NullableCoder.of(TimestampedValue.TimestampedValueCoder.of(this.rightCoder))
        .encode(value.getRightData(), outStream);

    BooleanCoder.of().encode(value.getMatched(), outStream);
  }

  @Override
  public BiTemporalJoinResult decode(InputStream inStream) throws IOException {

    K key = this.keyCoder.decode(inStream);

    int b;

    TimestampedValue<V1> leftValue =
        NullableCoder.of(TimestampedValue.TimestampedValueCoder.of(this.leftCoder))
            .decode(inStream);
    TimestampedValue<V2> rightValue =
        NullableCoder.of(TimestampedValue.TimestampedValueCoder.of(this.rightCoder))
            .decode(inStream);

    Boolean matched = BooleanCoder.of().decode(inStream);

    return new BiTemporalJoinResult(key, leftValue, rightValue, matched);
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return Arrays.asList(this.keyCoder, this.leftCoder, this.rightCoder);
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {

    verifyDeterministic(this, "Key coder must be deterministic", this.getKeyCoder());
    verifyDeterministic(this, "LeftCoder coder must be deterministic", this.getLeftCoder());
    verifyDeterministic(this, "RightCoder coder must be deterministic", this.getRightCoder());
  }

  @Override
  public TypeDescriptor<BiTemporalJoinResult<K, V1, V2>> getEncodedTypeDescriptor() {
    return (new TypeDescriptor<BiTemporalJoinResult<K, V1, V2>>() {})
        .where(new TypeParameter<K>() {}, this.keyCoder.getEncodedTypeDescriptor())
        .where(new TypeParameter<V1>() {}, this.leftCoder.getEncodedTypeDescriptor())
        .where(new TypeParameter<V2>() {}, this.rightCoder.getEncodedTypeDescriptor());
  }
}
