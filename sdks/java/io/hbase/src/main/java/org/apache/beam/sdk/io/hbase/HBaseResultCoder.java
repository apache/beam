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
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;

/**
 * A {@link Coder} that serializes and deserializes the {@link Result} objects using {@link
 * ProtobufUtil}.
 */
class HBaseResultCoder extends CustomCoder<Result> implements Serializable {
  private static final HBaseResultCoder INSTANCE = new HBaseResultCoder();

  private HBaseResultCoder() {}

  public static HBaseResultCoder of() {
    return INSTANCE;
  }

  @Override
  public void encode(Result value, OutputStream outputStream, Coder.Context context)
          throws IOException {
    ProtobufUtil.toResult(value).writeDelimitedTo(outputStream);
  }

  @Override
  public Result decode(InputStream inputStream, Coder.Context context)
      throws IOException {
    return ProtobufUtil.toResult(ClientProtos.Result.parseDelimitedFrom(inputStream));
  }
}
