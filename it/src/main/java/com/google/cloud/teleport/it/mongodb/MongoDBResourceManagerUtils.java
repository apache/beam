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
package com.google.cloud.teleport.it.mongodb;

import static com.google.cloud.teleport.it.common.ResourceManagerUtils.generateResourceId;

import com.google.re2j.Pattern;
import java.time.format.DateTimeFormatter;

/**
 * Utilities for {@link com.google.cloud.teleport.it.mongodb.MongoDBResourceManager}
 * implementations.
 */
final class MongoDBResourceManagerUtils {

  // MongoDB Database and Collection naming restrictions can be found at
  // https://www.mongodb.com/docs/manual/reference/limits/#naming-restrictions
  private static final int MAX_DATABASE_NAME_LENGTH = 63;
  private static final Pattern ILLEGAL_DATABASE_NAME_CHARS =
      Pattern.compile("[\\/\\\\. \"\0$]"); // i.e. [/\. "$]
  private static final String REPLACE_DATABASE_NAME_CHAR = "-";
  private static final int MIN_COLLECTION_NAME_LENGTH = 1;
  private static final int MAX_COLLECTION_NAME_LENGTH = 99;
  private static final Pattern ILLEGAL_COLLECTION_CHARS = Pattern.compile("[$\0]");
  private static final DateTimeFormatter TIME_FORMAT =
      DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss-SSSSSS");

  private MongoDBResourceManagerUtils() {}

  /**
   * Generates a MongoDB database name from a given string.
   *
   * @param baseString The string to generate the name from.
   * @return The database name string.
   */
  static String generateDatabaseName(String baseString) {
    return generateResourceId(
        baseString,
        ILLEGAL_DATABASE_NAME_CHARS,
        REPLACE_DATABASE_NAME_CHAR,
        MAX_DATABASE_NAME_LENGTH,
        TIME_FORMAT);
  }

  /**
   * Checks whether the given collection name is valid according to MongoDB constraints.
   *
   * @param databaseName the database name associated with the collection
   * @param collectionName the collection name to check.
   * @throws IllegalArgumentException if the collection name is invalid.
   */
  static void checkValidCollectionName(String databaseName, String collectionName) {
    String fullCollectionName = databaseName + "." + collectionName;
    if (collectionName.length() < MIN_COLLECTION_NAME_LENGTH) {
      throw new IllegalArgumentException("Collection name cannot be empty.");
    }
    if (fullCollectionName.length() > MAX_COLLECTION_NAME_LENGTH) {
      throw new IllegalArgumentException(
          "Collection name "
              + fullCollectionName
              + " cannot be longer than "
              + MAX_COLLECTION_NAME_LENGTH
              + " characters, including the database name and dot.");
    }
    if (ILLEGAL_COLLECTION_CHARS.matcher(collectionName).find()) {
      throw new IllegalArgumentException(
          "Collection name "
              + collectionName
              + " is not a valid name. Only letters, numbers, hyphens, underscores and exclamation points are allowed.");
    }
    if (collectionName.charAt(0) != '_' && !Character.isLetter(collectionName.charAt(0))) {
      throw new IllegalArgumentException(
          "Collection name " + collectionName + " must start with a letter or an underscore.");
    }
    String illegalKeyword = "system.";
    if (collectionName.startsWith(illegalKeyword)) {
      throw new IllegalArgumentException(
          "Collection name "
              + collectionName
              + " cannot start with the prefix \""
              + illegalKeyword
              + "\".");
    }
  }
}
