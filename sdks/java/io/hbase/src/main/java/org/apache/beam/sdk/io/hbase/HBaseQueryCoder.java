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
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;

/**
 * A {@link Coder} that serializes and deserializes the {@link HBaseQuery} objects using an {@link
 * StringUtf8Coder} to represent the tableId and {@link ProtobufUtil} to represent the HBase {@link
 * Scan} object.
 */
class HBaseQueryCoder extends AtomicCoder<HBaseQuery> implements Serializable {
  private static final HBaseQueryCoder INSTANCE = new HBaseQueryCoder();

  private HBaseQueryCoder() {}

  static HBaseQueryCoder of() {
    return INSTANCE;
  }

  @Override
  public void encode(HBaseQuery query, OutputStream outputStream) throws IOException {
    StringUtf8Coder.of().encode(query.getTableId(), outputStream);
    ProtobufUtil.toScan(query.getScan()).writeDelimitedTo(outputStream);
  }

  @Override
  public HBaseQuery decode(InputStream inputStream) throws IOException {
    final String tableId = StringUtf8Coder.of().decode(inputStream);
    final Scan scan = ProtobufUtil.toScan(ClientProtos.Scan.parseDelimitedFrom(inputStream));
    return HBaseQuery.of(tableId, scan);
  }
}
