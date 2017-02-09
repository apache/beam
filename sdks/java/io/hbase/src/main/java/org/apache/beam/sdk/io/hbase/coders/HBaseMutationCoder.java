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
package org.apache.beam.sdk.io.hbase.coders;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;

import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto.MutationType;

/**
 * A {@link Coder} that serializes and deserializes the {@link Mutation} objects using {@link
 * ProtobufUtil}.
 */
public class HBaseMutationCoder extends AtomicCoder<Mutation> implements Serializable {
  private static final HBaseMutationCoder INSTANCE = new HBaseMutationCoder();

  private HBaseMutationCoder() {}

  public static HBaseMutationCoder of() {
    return INSTANCE;
  }

  @Override
  public void encode(Mutation mutation, OutputStream outStream,
                     Coder.Context context) throws IOException {
    MutationType type = getType(mutation);
    MutationProto proto = ProtobufUtil.toMutation(type, mutation);
    proto.writeDelimitedTo(outStream);
  }

  @Override
  public Mutation decode(InputStream inStream,
                         Coder.Context context) throws IOException {
    return ProtobufUtil.toMutation(MutationProto.parseDelimitedFrom(inStream));
  }

  private static MutationType getType(Mutation mutation) {
    if (mutation instanceof Put) {
      return MutationType.PUT;
    } else if (mutation instanceof Delete) {
      return MutationType.DELETE;
    } else {
      // Increment and Append are not idempotent.  They should not be used in distributed jobs.
      throw new IllegalArgumentException("Only Put and Delete are supported");
    }
  }
}
