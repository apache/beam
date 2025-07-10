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
package org.apache.beam.sdk.util;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import java.io.IOException;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.util.RowJson.UnsupportedRowJsonException;
import org.apache.beam.sdk.values.Row;

/**
 * Utilities for working with {@link RowJson.RowJsonSerializer} and {@link
 * RowJson.RowJsonDeserializer}.
 */
@Internal
public class RowJsonUtils {

  //
  private static int defaultBufferLimit;

  /**
   * Increase the default jackson-databind stream read constraint.
   *
   * <p>StreamReadConstraints was introduced in jackson 2.15 causing string > 20MB (5MB in 2.15.0)
   * parsing failure. This has caused regressions in its dependencies include Beam. Here we
   * overwrite the default buffer size limit to 100 MB, and exposes this interface for higher limit.
   * If needed, call this method during pipeline run time, e.g. in DoFn.setup.
   */
  public static void increaseDefaultStreamReadConstraints(int newLimit) {
    if (newLimit <= defaultBufferLimit) {
      return;
    }
    try {
      Class<?> unused = Class.forName("com.fasterxml.jackson.core.StreamReadConstraints");

      com.fasterxml.jackson.core.StreamReadConstraints.overrideDefaultStreamReadConstraints(
          com.fasterxml.jackson.core.StreamReadConstraints.builder()
              .maxStringLength(newLimit)
              .build());
    } catch (ClassNotFoundException e) {
      // <2.15, do nothing
    }
    defaultBufferLimit = newLimit;
  }

  static {
    increaseDefaultStreamReadConstraints(100 * 1024 * 1024);
  }

  public static ObjectMapper newObjectMapperWith(RowJson.RowJsonDeserializer deserializer) {
    SimpleModule module = new SimpleModule("rowDeserializationModule");
    module.addDeserializer(Row.class, deserializer);

    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.registerModule(module);

    return objectMapper;
  }

  public static ObjectMapper newObjectMapperWith(RowJson.RowJsonSerializer serializer) {
    SimpleModule module = new SimpleModule("rowSerializationModule");
    module.addSerializer(Row.class, serializer);

    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.registerModule(module);

    return objectMapper;
  }

  public static Row jsonToRow(ObjectMapper objectMapper, String jsonString) {
    try {
      return objectMapper.readValue(jsonString, Row.class);
    } catch (JsonParseException | JsonMappingException jsonException) {
      throw new UnsupportedRowJsonException("Unable to parse Row", jsonException);
    } catch (IOException e) {
      throw new IllegalArgumentException("Unable to parse json object: " + jsonString, e);
    }
  }

  public static String rowToJson(ObjectMapper objectMapper, Row row) {
    try {
      return objectMapper.writeValueAsString(row);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Unable to serialize row: " + row, e);
    }
  }
}
