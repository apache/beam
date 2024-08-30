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
package org.apache.beam.it.elasticsearch;

import static org.apache.beam.it.common.utils.ResourceManagerUtils.generateResourceId;

import java.time.format.DateTimeFormatter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Utilities for {@link ElasticsearchResourceManager} implementations. */
final class ElasticsearchUtils {

  // Constraints from
  // https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-create-index.html
  private static final int MAX_INDEX_NAME_LENGTH = 255;

  // Cannot include \, /, *, ?, ", <, >, |, ` ` (space character), ,, #
  private static final Pattern ILLEGAL_INDEX_NAME_CHARS = Pattern.compile("[/\\\\ \"?*<>|,#\0]");
  private static final String REPLACE_INDEX_NAME_CHAR = "-";
  private static final DateTimeFormatter TIME_FORMAT =
      DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss-SSSSSS");

  private ElasticsearchUtils() {}

  /**
   * Generates an Elasticsearch index name from a given string.
   *
   * @param baseString The string to generate the name from.
   * @return The database name string.
   */
  static String generateIndexName(String baseString) {
    return generateResourceId(
        baseString,
        ILLEGAL_INDEX_NAME_CHARS,
        REPLACE_INDEX_NAME_CHAR,
        MAX_INDEX_NAME_LENGTH,
        TIME_FORMAT);
  }

  /**
   * Checks whether the given index name is valid according to Elasticsearch constraints.
   *
   * @param indexName the index name to check.
   * @throws IllegalArgumentException if the index name is invalid.
   */
  static void checkValidIndexName(String indexName) {
    if (indexName.length() > MAX_INDEX_NAME_LENGTH) {
      throw new IllegalArgumentException(
          "Index name "
              + indexName
              + " cannot be longer than "
              + MAX_INDEX_NAME_LENGTH
              + " characters.");
    }
    Matcher matcher = ILLEGAL_INDEX_NAME_CHARS.matcher(indexName);
    if (matcher.find()) {
      throw new IllegalArgumentException(
          "Index name "
              + indexName
              + " is not a valid name. Character \""
              + matcher.group()
              + "\" is not allowed.");
    }
    if (indexName.charAt(0) == '-' || indexName.charAt(0) == '_' || indexName.charAt(0) == '+') {
      throw new IllegalArgumentException(
          "Index name " + indexName + " can not start with -, _ or +.");
    }
  }
}
