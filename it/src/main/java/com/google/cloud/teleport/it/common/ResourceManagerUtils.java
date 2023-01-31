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
package com.google.cloud.teleport.it.common;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.hash.Hashing.goodFastHash;
import static java.lang.Math.min;

import com.google.common.hash.HashFunction;
import com.google.re2j.Pattern;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Random;

/** Common utilities for ResourceManager implementations. */
public class ResourceManagerUtils {

  private static final int MIN_PROJECT_ID_LENGTH = 4;
  private static final int MAX_PROJECT_ID_LENGTH = 30;
  private static final Pattern ILLEGAL_PROJECT_CHARS = Pattern.compile("[^a-zA-Z0-9-!']");
  private static final String TIME_ZONE = "UTC";

  /**
   * Generates a new id string from an existing one.
   *
   * @param id The id string to generate a new id from.
   * @param targetLength The length of the new id to generate. Must be greater than 8.
   */
  public static String generateNewId(String id, int targetLength) {
    if (id.length() <= targetLength) {
      return id;
    }

    if (targetLength <= 8) {
      throw new IllegalArgumentException("targetLength must be greater than 8");
    }

    HashFunction hashFunction = goodFastHash(32);
    String hash = hashFunction.hashUnencodedChars(id).toString();
    return id.substring(0, targetLength - hash.length() - 1) + "-" + hash;
  }

  /**
   * Generates a generic resource id from a given string, avoiding characters specified in the
   * illegalChars Pattern. The length of the generated string ID will not exceed the length
   * specified by targetLength.
   *
   * @param baseString the base ID to generate the resource ID from.
   * @param illegalChars a pattern of characters to remove from the generated ID.
   * @param replaceChar the character to replace all illegal characters with.
   * @param targetLength the max length of the generated ID.
   * @return the generated resource ID.
   */
  public static String generateResourceId(
      String baseString,
      Pattern illegalChars,
      String replaceChar,
      int targetLength,
      DateTimeFormatter timeFormat) {
    // first, make sure the baseString, typically the test ID, is not empty
    checkArgument(baseString.length() != 0, "baseString cannot be empty.");

    // next, replace all illegal characters from given string with given replacement character
    String illegalCharsRemoved =
        illegalChars.matcher(baseString.toLowerCase()).replaceAll(replaceChar);

    // finally, append the date/time and return the substring that does not exceed the length limit
    LocalDateTime localDateTime = LocalDateTime.now(ZoneId.of(TIME_ZONE));
    String timeAddOn = localDateTime.format(timeFormat);
    return illegalCharsRemoved.subSequence(
            0, min(targetLength - timeAddOn.length() - 1, illegalCharsRemoved.length()))
        + replaceChar
        + localDateTime.format(timeFormat);
  }

  /** Generates random letter for padding. */
  public static char generatePadding() {
    Random random = new Random();
    return (char) ('a' + random.nextInt(26));
  }

  /**
   * Checks whether the given project ID is valid according to GCP constraints.
   *
   * @param idToCheck the project ID to check.
   * @throws IllegalArgumentException if the project ID is invalid.
   */
  public static void checkValidProjectId(String idToCheck) {
    if (idToCheck.length() < MIN_PROJECT_ID_LENGTH) {
      throw new IllegalArgumentException("Project ID " + idToCheck + " cannot be empty.");
    }
    if (idToCheck.length() > MAX_PROJECT_ID_LENGTH) {
      throw new IllegalArgumentException(
          "Project ID "
              + idToCheck
              + " cannot be longer than "
              + MAX_PROJECT_ID_LENGTH
              + " characters.");
    }
    if (ILLEGAL_PROJECT_CHARS.matcher(idToCheck).find()) {
      throw new IllegalArgumentException(
          "Project ID "
              + idToCheck
              + " is not a valid ID. Only letters, numbers, hyphens, single quotes and exclamation"
              + " points are allowed.");
    }
  }
}
