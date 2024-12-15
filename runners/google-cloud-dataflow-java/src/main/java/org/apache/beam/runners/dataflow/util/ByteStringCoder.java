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
package org.apache.beam.runners.dataflow.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.util.VarInt;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.ByteStreams;

/**
 * A simplified {@link Coder} for {@link ByteString}, to avoid a dependency on
 * beam-java-sdk-extensions-protobuf.
 */
public class ByteStringCoder extends AtomicCoder<ByteString> {
  public static ByteStringCoder of() {
    return INSTANCE;
  }

  private static final ByteStringCoder INSTANCE = new ByteStringCoder();

  private ByteStringCoder() {}

  @Override
  public void encode(ByteString value, OutputStream os) throws IOException {
    VarInt.encode(value.size(), os);
    value.writeTo(os);
  }

  @Override
  public ByteString decode(InputStream is) throws IOException {
    int size = VarInt.decodeInt(is);
    return ByteString.readFrom(ByteStreams.limit(is, size), size);
  }
}
