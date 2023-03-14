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
import java.util.List;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderProvider;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto.MutationType;

/**
 * Row mutations coder to provide serialization support for Hbase RowMutations object, which isn't
 * natively serializable.
 */
public class HBaseRowMutationsCoder extends AtomicCoder<RowMutations> implements Serializable {
  private static final HBaseRowMutationsCoder INSTANCE = new HBaseRowMutationsCoder();

  public HBaseRowMutationsCoder() {}

  public static HBaseRowMutationsCoder of() {
    return INSTANCE;
  }

  @Override
  public void encode(RowMutations value, OutputStream outStream) throws IOException {

    // encode row key
    byte[] rowKey = value.getRow();
    int rowKeyLen = rowKey.length;

    // serialize row key
    outStream.write(rowKeyLen);
    outStream.write(rowKey);

    // serialize mutation list
    List<Mutation> mutations = value.getMutations();
    int mutationsSize = mutations.size();
    outStream.write(mutationsSize);
    for (Mutation mutation : mutations) {
      MutationType type = getType(mutation);
      MutationProto proto = ProtobufUtil.toMutation(type, mutation);
      proto.writeDelimitedTo(outStream);
    }
  }

  @Override
  public RowMutations decode(InputStream inStream) throws IOException {

    int rowKeyLen = inStream.read();
    byte[] rowKey = new byte[rowKeyLen];
    inStream.read(rowKey);

    RowMutations rowMutations = new RowMutations(rowKey);
    int mutationListSize = inStream.read();
    for (int i = 0; i < mutationListSize; i++) {
      Mutation m = ProtobufUtil.toMutation(MutationProto.parseDelimitedFrom(inStream));
      MutationType type = getType(m);

      if (type == MutationType.PUT) {
        rowMutations.add((Put) m);
      } else if (type == MutationType.DELETE) {
        rowMutations.add((Delete) m);
      }
    }
    return rowMutations;
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
