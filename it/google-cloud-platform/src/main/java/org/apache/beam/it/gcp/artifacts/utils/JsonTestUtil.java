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
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.json.JsonMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
}
