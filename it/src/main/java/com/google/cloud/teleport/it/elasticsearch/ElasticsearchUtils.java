/*
 * Copyright (C) 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.it.elasticsearch;

import static com.google.cloud.teleport.it.common.ResourceManagerUtils.generateResourceId;

import com.google.re2j.Matcher;
import com.google.re2j.Pattern;
import java.time.format.DateTimeFormatter;

/** Utilities for {@link ElasticsearchResourceManager} implementations. */
final class ElasticsearchUtils {

  // Constraints from
  // https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-create-index.html
  private static final int MAX_INDEX_NAME_LENGTH = 255;

  // Cannot include \, /, *, ?, ", <, >, |, ` ` (space character), ,, #
  private static final Pattern ILLEGAL_INDEX_NAME_CHARS =
      Pattern.compile("[\\/\\\\ \"\\?\\*\\\"\\<\\>\\|,\\#\0]");
  private static final String REPLACE_DATABASE_NAME_CHAR = "-";
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
        REPLACE_DATABASE_NAME_CHAR,
        MAX_INDEX_NAME_LENGTH,
        TIME_FORMAT);
  }

  /**
   * Checks whether the given index name is valid according to Elasticsearch constraints.
   *
   * @param indexName the index name to check.
   * @throws IllegalArgumentException if the collection name is invalid.
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
