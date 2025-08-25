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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Map;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;

public class TableUtils {
  private static final ObjectMapper objectMapper =
      JsonMapper.builder()
          .enable(
              JsonReadFeature.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER,
              JsonReadFeature.ALLOW_JAVA_COMMENTS,
              JsonReadFeature.ALLOW_MISSING_VALUES,
              JsonReadFeature.ALLOW_NON_NUMERIC_NUMBERS,
              JsonReadFeature.ALLOW_LEADING_ZEROS_FOR_NUMBERS,
              JsonReadFeature.ALLOW_SINGLE_QUOTES,
              JsonReadFeature.ALLOW_TRAILING_COMMA,
              JsonReadFeature.ALLOW_UNESCAPED_CONTROL_CHARS,
              JsonReadFeature.ALLOW_UNQUOTED_FIELD_NAMES)
          .build();

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

  public static ObjectNode parseProperties(Map<String, String> map) {
    return objectMapper.valueToTree(map);
  }

  public static Map<String, Object> convertNode2Map(JsonNode jsonNode) {
    return objectMapper.convertValue(jsonNode, new TypeReference<Map<String, Object>>() {});
  }
}
