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
package org.apache.beam.sdk.transforms.join;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;

/** A {@link Coder} for {@link Pair}s that defers to underlying coders. */
public class PairCoder<V1, V2> extends StructuredCoder<Pair<V1, V2>> {
  private final Coder<V1> firstCoder;
  private final Coder<V2> secondCoder;

  private PairCoder(Coder<V1> firstCoder, Coder<V2> secondCoder) {
    this.firstCoder = firstCoder;
    this.secondCoder = secondCoder;
  }

  /** Returns a {@link PairCoder} for the given underlying value coders. */
  public static <V1, V2> PairCoder<V1, V2> of(Coder<V1> firstCoder, Coder<V2> secondCoder) {
    return new PairCoder<>(firstCoder, secondCoder);
  }

  @Override
  public void encode(Pair<V1, V2> value, OutputStream outStream, Context context)
      throws IOException, CoderException {
    if (value == null) {
      throw new CoderException("cannot encode a null pair coder");
    }
    firstCoder.encode(value.getFirst(), outStream);
    secondCoder.encode(value.getSecond(), outStream, context);
  }

  @Override
  public void encode(Pair<V1, V2> value, OutputStream outStream)
      throws CoderException, IOException {
    encode(value, outStream, Context.NESTED);
  }

  @Override
  public Pair<V1, V2> decode(InputStream inStream) throws CoderException, IOException {
    return decode(inStream, Context.NESTED);
  }

  @Override
  public Pair<V1, V2> decode(InputStream inStream, Context context) throws CoderException,
      IOException {
    return Pair.of(firstCoder.decode(inStream), secondCoder.decode(inStream, context));
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return ImmutableList.of(firstCoder, secondCoder);
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    firstCoder.verifyDeterministic();
    secondCoder.verifyDeterministic();
  }
}
