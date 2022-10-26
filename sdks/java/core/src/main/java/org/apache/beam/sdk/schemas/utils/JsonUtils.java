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
package org.apache.beam.sdk.schemas.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.util.RowJson;
import org.apache.beam.sdk.util.RowJsonUtils;
import org.apache.beam.sdk.values.Row;

/** Utils to convert JSON records to Beam {@link Row}. */
@Experimental(Kind.SCHEMAS)
public class JsonUtils {

  /** Returns a {@link SimpleFunction} mapping JSON byte[] arrays to Beam {@link Row}s. */
  public static SimpleFunction<byte[], Row> getJsonBytesToRowFunction(Schema beamSchema) {
    return new JsonToRowFn<byte[]>(beamSchema) {
      @Override
      public Row apply(byte[] input) {
        String jsonString = byteArrayToJsonString(input);
        return RowJsonUtils.jsonToRow(objectMapper, jsonString);
      }
    };
  }

  /** Returns a {@link SimpleFunction} mapping JSON {@link String}s to Beam {@link Row}s. */
  public static SimpleFunction<String, Row> getJsonStringToRowFunction(Schema beamSchema) {
    return new JsonToRowFn<String>(beamSchema) {
      @Override
      public Row apply(String jsonString) {
        return RowJsonUtils.jsonToRow(objectMapper, jsonString);
      }
    };
  }

  /** Returns a {@link SimpleFunction} mapping Beam {@link Row}s to JSON byte[] arrays. */
  public static SimpleFunction<Row, byte[]> getRowToJsonBytesFunction(Schema beamSchema) {
    return new RowToJsonFn<byte[]>(beamSchema) {
      @Override
      public byte[] apply(Row input) {
        String jsonString = RowJsonUtils.rowToJson(objectMapper, input);
        return jsonStringToByteArray(jsonString);
      }
    };
  }

  /** Returns a {@link SimpleFunction} mapping Beam {@link Row}s to JSON {@link String}s. */
  public static SimpleFunction<Row, String> getRowToJsonStringsFunction(Schema beamSchema) {
    return new RowToJsonFn<String>(beamSchema) {
      @Override
      public String apply(Row input) {
        return RowJsonUtils.rowToJson(objectMapper, input);
      }
    };
  }

  private abstract static class JsonToRowFn<T> extends SimpleFunction<T, Row> {
    final RowJson.RowJsonDeserializer deserializer;
    final ObjectMapper objectMapper;

    private JsonToRowFn(Schema beamSchema) {
      deserializer = RowJson.RowJsonDeserializer.forSchema(beamSchema);
      objectMapper = RowJsonUtils.newObjectMapperWith(deserializer);
    }
  }

  private abstract static class RowToJsonFn<T> extends SimpleFunction<Row, T> {
    final RowJson.RowJsonSerializer serializer;
    final ObjectMapper objectMapper;

    private RowToJsonFn(Schema beamSchema) {
      serializer = RowJson.RowJsonSerializer.forSchema(beamSchema);
      objectMapper = RowJsonUtils.newObjectMapperWith(serializer);
    }
  }

  static byte[] jsonStringToByteArray(String jsonString) {
    return jsonString.getBytes(StandardCharsets.UTF_8);
  }

  static String byteArrayToJsonString(byte[] jsonBytes) {
    return new String(jsonBytes, StandardCharsets.UTF_8);
  }
}
