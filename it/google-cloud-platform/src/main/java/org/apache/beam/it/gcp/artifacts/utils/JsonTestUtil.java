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
package org.apache.beam.it.gcp.artifacts.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * The {@link JsonTestUtil} class provides common utilities used for executing tests that involve
 * Json.
 */
public class JsonTestUtil {

  private static final TypeReference<Map<String, Object>> mapTypeRef =
      new TypeReference<Map<String, Object>>() {};

  /**
   * Read JSON records to a list of Maps.
   *
   * @param contents Byte array with contents to read.
   * @return A list with all records.
   */
  public static List<Map<String, Object>> readRecords(byte[] contents) throws IOException {
    List<Map<String, Object>> records = new ArrayList<>();

    JsonMapper mapper = new JsonMapper();

    try (MappingIterator<Map<String, Object>> iterator =
        mapper.readerFor(mapTypeRef).readValues(contents)) {
      while (iterator.hasNextValue()) {
        records.add(iterator.next());
      }
    }

    return records;
  }

  /**
   * Reads NDJSON (Newline Delimited JSON) data from a byte array and returns a list of parsed JSON
   * objects. Each JSON object is represented as a Map of String keys to Object values.
   *
   * @param jsonBytes A byte array containing NDJSON data.
   * @return A list of parsed JSON objects as {@code Map<String, Object>}.
   * @throws IOException if there's an issue reading or parsing the data.
   */
  public static List<Map<String, Object>> readNDJSON(byte[] jsonBytes) throws IOException {
    try (ByteArrayInputStream inputStream = new ByteArrayInputStream(jsonBytes)) {
      InputStreamReader reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
      JsonMapper mapper = new JsonMapper();

      return new BufferedReader(reader)
          .lines()
          .map(
              line -> {
                try {
                  // Deserialize each line as a Map<String, Object>
                  return mapper.readValue(line, mapTypeRef);
                } catch (IOException e) {
                  throw new RuntimeException(e);
                }
              })
          .collect(Collectors.toList());
    }
  }

  /**
   * Recursively sorts the keys of a nested JSON represented as a Map.
   *
   * @param jsonMap A {@code Map<String, Object>} representing the nested JSON.
   * @return A sorted {@code Map<String, Object>} where the keys are sorted in natural order.
   */
  public static Map<String, Object> sortJsonMap(Map<String, Object> jsonMap) {
    return jsonMap.entrySet().stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                entry -> {
                  Object value = entry.getValue();
                  if (value instanceof Map) {
                    return sortJsonMap((Map<String, Object>) value);
                  } else if (value instanceof List) {
                    return ((List<Object>) value)
                        .stream()
                            .map(
                                item ->
                                    item instanceof Map
                                        ? sortJsonMap((Map<String, Object>) item)
                                        : item)
                            .collect(Collectors.toList());
                  } else {
                    return value;
                  }
                },
                (a, b) -> a, // Merge function (not needed for a TreeMap)
                TreeMap::new // Resulting map is a TreeMap
                ));
  }

  /**
   * Read JSON records to a list of Maps.
   *
   * @param contents String with contents to read.
   * @return A list with all records.
   */
  public static List<Map<String, Object>> readRecords(String contents) throws IOException {
    return readRecords(contents.getBytes(StandardCharsets.UTF_8));
  }

  /**
   * Read JSON record to a Map.
   *
   * @param contents Byte array with contents to read.
   * @return A map with the records.
   */
  public static Map<String, Object> readRecord(byte[] contents) throws IOException {
    JsonMapper mapper = new JsonMapper();
    return mapper.readerFor(mapTypeRef).readValue(contents);
  }

  /**
   * Read JSON record to a Map.
   *
   * @param contents String with contents to read.
   * @return A map with the records.
   */
  public static Map<String, Object> readRecord(String contents) throws IOException {
    return readRecord(contents.getBytes(StandardCharsets.UTF_8));
  }

  /**
   * Parses a JSON string and returns either a List of Maps or a Map, depending on whether the JSON
   * represents an array or an object.
   *
   * @param jsonString The JSON string to parse.
   * @return A List of Maps if the JSON is an array, or a Map if it's an object.
   * @throws IOException If there's an error while parsing the JSON string.
   */
  public static Object parseJsonString(String jsonString) throws IOException {
    ObjectMapper objectMapper = new ObjectMapper();
    JsonNode jsonNode = objectMapper.readTree(jsonString);
    if (jsonNode.isArray()) {
      return parseJsonArray((ArrayNode) jsonNode);
    } else if (jsonNode.isObject()) {
      return parseJsonObject(jsonNode);
    } else {
      throw new IllegalArgumentException("Input is not a valid JSON object or array.");
    }
  }

  /**
   * Parses a JSON array represented by an ArrayNode and returns a List of Maps.
   *
   * @param arrayNode The JSON array to parse.
   * @return A List of Maps containing the parsed data.
   */
  private static List<Object> parseJsonArray(ArrayNode arrayNode) {
    List<Object> result = new ArrayList<>();
    for (JsonNode element : arrayNode) {
      if (element.isObject()) {
        result.add(parseJsonObject(element));
      } else {
        result.add(parseSimpleNode(element));
      }
    }
    return result;
  }

  /**
   * Parses a JSON object represented by a JsonNode and returns a Map.
   *
   * @param objectNode The JSON object to parse.
   * @return A Map containing the parsed data.
   */
  private static Map<String, Object> parseJsonObject(JsonNode objectNode) {
    Map<String, Object> result = new HashMap<>();
    objectNode
        .fields()
        .forEachRemaining(
            entry -> {
              String key = entry.getKey();
              JsonNode value = entry.getValue();
              if (value.isObject()) {
                result.put(key, parseJsonObject(value));
              } else if (value.isArray()) {
                result.put(key, parseJsonArray((ArrayNode) value));
              } else {
                result.put(key, parseSimpleNode(value));
              }
            });
    return result;
  }

  /** Parse following value from JSON node: text, number, boolean, null. */
  @SuppressWarnings("nullness")
  private static Object parseSimpleNode(JsonNode element) {
    if (element.isTextual()) {
      return element.asText();
    } else if (element.isNumber()) {
      return element.numberValue();
    } else if (element.isBoolean()) {
      return element.asBoolean();
    } else if (element.isNull()) {
      return null;
    } else {
      throw new IllegalArgumentException("Element is not a valid JSON object or array.");
    }
  }
}
