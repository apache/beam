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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import java.io.IOException;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.values.Row;

/**
 * JsonToRowUtils.
 */
@Internal
public class JsonToRowUtils {

  public static ObjectMapper newObjectMapperWith(RowJsonDeserializer deserializer) {
    SimpleModule module = new SimpleModule("rowDeserializationModule");
    module.addDeserializer(Row.class, deserializer);

    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.registerModule(module);

    return objectMapper;
  }

  public static Row jsonToRow(ObjectMapper objectMapper, String jsonString) {
    try {
      return objectMapper.readValue(jsonString, Row.class);
    } catch (IOException e) {
      throw new IllegalArgumentException("Unable to parse json object: " + jsonString, e);
    }
  }
}
