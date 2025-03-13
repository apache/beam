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
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.util.VarInt;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;

/** A UnionCoder encodes RawUnionValues. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class UnionCoder extends StructuredCoder<RawUnionValue> {
  // TODO: Think about how to integrate this with a schema object (i.e.
  // a tuple of tuple tags).
  /**
   * Builds a union coder with the given list of element coders. This list corresponds to a mapping
   * of union tag to Coder. Union tags start at 0.
   */
  public static UnionCoder of(List<Coder<?>> elementCoders) {
    return new UnionCoder(elementCoders);
  }

  private int getIndexForEncoding(RawUnionValue union) {
    if (union == null) {
      throw new IllegalArgumentException("cannot encode a null tagged union");
    }
    int index = union.getUnionTag();
    if (index < 0 || index >= elementCoders.size()) {
      throw new IllegalArgumentException(
          "union value index " + index + " not in range [0.." + (elementCoders.size() - 1) + "]");
    }
    return index;
  }

  @Override
  public void encode(RawUnionValue union, OutputStream outStream)
      throws IOException, CoderException {
    encode(union, outStream, Context.NESTED);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void encode(RawUnionValue union, OutputStream outStream, Context context)
      throws IOException, CoderException {
    int index = getIndexForEncoding(union);
    // Write out the union tag.
    VarInt.encode(index, outStream);

    // Write out the actual value.
    Coder<Object> coder = (Coder<Object>) elementCoders.get(index);
    coder.encode(union.getValue(), outStream, context);
  }

  @Override
  public RawUnionValue decode(InputStream inStream) throws IOException, CoderException {
    return decode(inStream, Context.NESTED);
  }

  @Override
  public RawUnionValue decode(InputStream inStream, Context context)
      throws IOException, CoderException {
    int index = VarInt.decodeInt(inStream);
    Object value = elementCoders.get(index).decode(inStream, context);
    return new RawUnionValue(index, value);
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return Collections.emptyList();
  }

  @Override
  public List<? extends Coder<?>> getComponents() {
    return elementCoders;
  }

  public List<? extends Coder<?>> getElementCoders() {
    return elementCoders;
  }

  /**
   * Since this coder uses elementCoders.get(index) and coders that are known to run in constant
   * time, we defer the return value to that coder.
   */
  @Override
  public boolean isRegisterByteSizeObserverCheap(RawUnionValue union) {
    int index = getIndexForEncoding(union);
    @SuppressWarnings("unchecked")
    Coder<Object> coder = (Coder<Object>) elementCoders.get(index);
    return coder.isRegisterByteSizeObserverCheap(union.getValue());
  }

  /** Notifies ElementByteSizeObserver about the byte size of the encoded value using this coder. */
  @Override
  public void registerByteSizeObserver(RawUnionValue union, ElementByteSizeObserver observer)
      throws Exception {
    int index = getIndexForEncoding(union);
    // Write out the union tag.
    observer.update(VarInt.getLength(index));
    // Write out the actual value.
    @SuppressWarnings("unchecked")
    Coder<Object> coder = (Coder<Object>) elementCoders.get(index);
    coder.registerByteSizeObserver(union.getValue(), observer);
  }

  /////////////////////////////////////////////////////////////////////////////

  private final List<Coder<?>> elementCoders;

  private UnionCoder(List<Coder<?>> elementCoders) {
    this.elementCoders = elementCoders;
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    verifyDeterministic(
        this, "UnionCoder is only deterministic if all element coders are", elementCoders);
  }
}
