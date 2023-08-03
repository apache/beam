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
package org.apache.beam.it.gcp.spanner.utils;

import static org.apache.beam.it.common.utils.ResourceManagerUtils.generatePadding;
import static org.apache.beam.it.common.utils.ResourceManagerUtils.generateResourceId;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.time.format.DateTimeFormatter;
import java.util.regex.Pattern;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.CharMatcher;

/** Utilities for {@link SpannerResourceManager} implementations. */
public final class SpannerResourceManagerUtils {
  private static final Pattern ILLEGAL_INSTANCE_CHARS = Pattern.compile("[^a-z0-9-]");
  private static final Pattern ILLEGAL_DATABASE_CHARS = Pattern.compile("[\\W-]");
  private static final String REPLACE_INSTANCE_CHAR = "-";
  private static final String REPLACE_DATABASE_CHAR = "_";
  public static final int MAX_INSTANCE_ID_LENGTH = 30;
  public static final int MAX_DATABASE_ID_LENGTH = 30;
  private static final DateTimeFormatter INSTANCE_TIME_FORMAT =
      DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss-SSSSSS");
  private static final DateTimeFormatter DATABASE_TIME_FORMAT =
      DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss_SSSSSS");

  private SpannerResourceManagerUtils() {}

  /**
   * Generates a database id from a given string.
   *
   * @param baseString The string to generate the id from.
   * @return The database id string
   */
  public static String generateDatabaseId(String baseString) {
    checkArgument(baseString.length() != 0, "baseString cannot be empty!");

    String databaseId =
        generateResourceId(
            baseString,
            ILLEGAL_DATABASE_CHARS,
            REPLACE_DATABASE_CHAR,
            MAX_DATABASE_ID_LENGTH,
            DATABASE_TIME_FORMAT);

    // replace hyphen with underscore, so there's no need for backticks
    String trimmed = CharMatcher.is('_').trimTrailingFrom(databaseId);

    checkArgument(
        trimmed.length() > 0,
        "Database id is empty after removing illegal characters and trailing underscores");

    // if first char is not a letter, replace with a padding letter, so it doesn't
    // violate spanner's database naming rules
    char padding = generatePadding();
    if (!Character.isLetter(trimmed.charAt(0))) {
      trimmed = padding + trimmed.substring(1);
    }
    return trimmed;
  }

  /**
   * Generates an instance id from a given string.
   *
   * @param baseString The string to generate the id from.
   * @return The instance id string.
   */
  public static String generateInstanceId(String baseString) {
    String instanceId =
        generateResourceId(
            baseString,
            ILLEGAL_INSTANCE_CHARS,
            REPLACE_INSTANCE_CHAR,
            MAX_INSTANCE_ID_LENGTH,
            INSTANCE_TIME_FORMAT);

    // if first char is not a letter, replace with letter, so it doesn't
    // violate spanner's instance naming rules
    if (!Character.isLetter(instanceId.charAt(0))) {
      char padding = generatePadding();
      instanceId = padding + instanceId.substring(1);
    }

    return instanceId;
  }
}
