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
package org.apache.beam.sdk.io.gcp.bigtable;

import com.google.bigtable.v2.RowFilter;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.Base64;

public class RowUtils {
  public static final String KEY = "key";
  public static final String VALUE = "val";
  public static final String TIMESTAMP_MICROS = "timestampMicros";
  public static final String LABELS = "labels";
  public static final String COLUMNS_MAPPING = "columnsMapping";

  public static ByteString byteString(byte[] bytes) {
    return ByteString.copyFrom(bytes);
  }

  public static ByteString byteStringUtf8(String value) {
    return ByteString.copyFromUtf8(value);
  }

  /** Encode a row filter with Base64 encoding. */
  public static String encodeRowFilter(RowFilter filter) {
    return Base64.getEncoder().encodeToString(filter.toByteArray());
  }

  /** Decode a base64 encoded row filter string. */
  public static RowFilter decodeRowFilter(String serialized) throws InvalidProtocolBufferException {
    byte[] decoded = Base64.getDecoder().decode(serialized);
    return RowFilter.parseFrom(decoded);
  }
}
