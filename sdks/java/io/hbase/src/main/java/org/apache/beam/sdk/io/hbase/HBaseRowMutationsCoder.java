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
package org.apache.beam.sdk.io.hbase;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderProvider;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto.MutationType;

/**
 * Row mutations coder to provide serialization support for Hbase RowMutations object, which isn't
 * natively serializable.
 */
class HBaseRowMutationsCoder extends StructuredCoder<RowMutations> implements Serializable {
  private static final HBaseRowMutationsCoder INSTANCE = new HBaseRowMutationsCoder();

  public HBaseRowMutationsCoder() {
    byteArrayCoder = ByteArrayCoder.of();
    listCoder = ListCoder.of(HBaseMutationCoder.of());
  }

  public static HBaseRowMutationsCoder of() {
    return INSTANCE;
  }

  private final ByteArrayCoder byteArrayCoder;
  private final ListCoder<Mutation> listCoder;

  @Override
  public void encode(RowMutations value, OutputStream outStream) throws IOException {
    byteArrayCoder.encode(value.getRow(), outStream);
    listCoder.encode(value.getMutations(), outStream);
  }

  @Override
  public RowMutations decode(InputStream inStream) throws IOException, IllegalArgumentException {

    byte[] rowKey = byteArrayCoder.decode(inStream);
    List<Mutation> mutations = listCoder.decode(inStream);

    RowMutations rowMutations = new RowMutations(rowKey);
    for (Mutation m : mutations) {
      MutationType type = getType(m);

      if (type == MutationType.PUT) {
        rowMutations.add((Put) m);
      } else if (type == MutationType.DELETE) {
        rowMutations.add((Delete) m);
      } else {
        throw new IllegalArgumentException("Mutation type not supported.");
      }
    }
    return rowMutations;
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return Arrays.asList(listCoder, byteArrayCoder);
  }

  /**
   * Coder is always deterministic: 1. {@link RowMutations} maintains equality by row key only,
   * which is asserted equal in this coder 2. Canonical encoding is maintained regardless of object
   * machine or time context
   *
   * @throws @UnknownKeyFor@NonNull@Initialized NonDeterministicException
   */
  @Override
  public void verifyDeterministic() {
    return;
  }

  private static MutationType getType(Mutation mutation) {
    if (mutation instanceof Put) {
      return MutationType.PUT;
    } else if (mutation instanceof Delete) {
      return MutationType.DELETE;
    } else {
      throw new IllegalArgumentException("Only Put and Delete are supported");
    }
  }

  /**
   * Returns a {@link CoderProvider} which uses the {@link HBaseRowMutationsCoder} for {@link
   * RowMutations}.
   */
  static CoderProvider getCoderProvider() {
    return HBASE_ROW_MUTATIONS_CODER_PROVIDER;
  }

  private static final CoderProvider HBASE_ROW_MUTATIONS_CODER_PROVIDER =
      new HBaseRowMutationsCoderProvider();

  /** A {@link CoderProvider} for {@link Mutation mutations}. */
  private static class HBaseRowMutationsCoderProvider extends CoderProvider {
    @Override
    public <T> Coder<T> coderFor(
        TypeDescriptor<T> typeDescriptor, List<? extends Coder<?>> componentCoders)
        throws CannotProvideCoderException {
      if (!typeDescriptor.isSubtypeOf(HBASE_ROW_MUTATIONS_TYPE_DESCRIPTOR)) {
        throw new CannotProvideCoderException(
            String.format(
                "Cannot provide %s because %s is not a subclass of %s",
                HBaseRowMutationsCoder.class.getSimpleName(),
                typeDescriptor,
                Mutation.class.getName()));
      }

      try {
        @SuppressWarnings("unchecked")
        Coder<T> coder = (Coder<T>) HBaseRowMutationsCoder.of();
        return coder;
      } catch (IllegalArgumentException e) {
        throw new CannotProvideCoderException(e);
      }
    }
  }

  private static final TypeDescriptor<RowMutations> HBASE_ROW_MUTATIONS_TYPE_DESCRIPTOR =
      new TypeDescriptor<RowMutations>() {};
}
