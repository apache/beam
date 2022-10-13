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
package com.google.cloud.teleport.it.spanner;

import static com.google.cloud.teleport.it.common.ResourceManagerUtils.generatePadding;
import static com.google.cloud.teleport.it.common.ResourceManagerUtils.generateResourceId;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.common.base.CharMatcher;
import com.google.re2j.Pattern;
import java.time.format.DateTimeFormatter;

/**
 * Utilities for {@link com.google.cloud.teleport.it.spanner.SpannerResourceManager}
 * implementations.
 */
public final class SpannerResourceManagerUtils {
  private static final Pattern ILLEGAL_DATABASE_CHARS = Pattern.compile("[\\W-]");
  private static final Pattern ILLEGAL_INSTANCE_CHARS = Pattern.compile("[^a-z0-9-]");
  private static final String REPLACE_INSTANCE_CHAR = "-";
  public static final int MAX_INSTANCE_ID_LENGTH = 36;
  private static final DateTimeFormatter TIME_FORMAT =
      DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss-SSSSSS");

  private SpannerResourceManagerUtils() {}

  /**
   * Generates a database id from a given string.
   *
   * @param baseString The string to generate the id from.
   * @return The database id string
   */
  static String generateDatabaseId(String baseString) {
    checkArgument(baseString.length() != 0, "baseString cannot be empty!");
    String illegalCharsRemoved =
        ILLEGAL_DATABASE_CHARS.matcher(baseString.toLowerCase()).replaceAll("_");

    // replace hyphen with underscore, so there's no need for backticks
    String trimmed = CharMatcher.is('_').trimTrailingFrom(illegalCharsRemoved);

    checkArgument(
        trimmed.length() > 0,
        "Database id is empty after removing illegal characters and trailing underscores");

    // if first char is not a letter or last char is a digit, replace with letter, so it doesn't
    // violate spanner's database naming rules
    char padding = generatePadding();

    if (Character.isDigit(trimmed.charAt(trimmed.length() - 1))) {
      trimmed = trimmed.substring(0, trimmed.length() - 1) + padding;
    }
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
  static String generateInstanceId(String baseString) {
    String instanceId =
        generateResourceId(
            baseString,
            ILLEGAL_INSTANCE_CHARS,
            REPLACE_INSTANCE_CHAR,
            MAX_INSTANCE_ID_LENGTH,
            TIME_FORMAT);

    // if first char is not a letter, replace with letter, so it doesn't
    // violate spanner's database naming rules
    if (!Character.isLetter(instanceId.charAt(0))) {
      char padding = generatePadding();
      instanceId = padding + instanceId.substring(1);
    }

    return instanceId;
  }
}
