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
package org.apache.beam.sdk.extensions.sql;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Map;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;

public class TableUtils {
  private static ObjectMapper objectMapper =
      new ObjectMapper()
          .configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true)
          .configure(JsonParser.Feature.ALLOW_TRAILING_COMMA, true);

  private TableUtils() {
    // nothing here
  }

  @VisibleForTesting
  public static ObjectMapper getObjectMapper() {
    return objectMapper;
  }

  public static ObjectNode emptyProperties() {
    return objectMapper.createObjectNode();
  }

  public static ObjectNode parseProperties(String json) {
    try {
      return (ObjectNode) objectMapper.readTree(json);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("illegal table properties: " + json);
    }
  }

  public static Map<String, Object> convertNode2Map(JsonNode jsonNode) {
    return objectMapper.convertValue(jsonNode, new TypeReference<Map<String, Object>>() {});
  }
}
