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
package org.apache.beam.it.common.utils;

import static java.lang.Math.min;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.hash.Hashing.goodFastHash;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Random;
import java.util.regex.Pattern;
import org.apache.beam.it.common.ResourceManager;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.hash.HashFunction;
import org.apache.commons.lang3.RandomStringUtils;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Common utilities for ResourceManager implementations. */
public class ResourceManagerUtils {

  private static final Logger LOG = LoggerFactory.getLogger(ResourceManagerUtils.class);

  private static final int MIN_PROJECT_ID_LENGTH = 4;
  private static final int MAX_PROJECT_ID_LENGTH = 30;
  private static final Pattern ILLEGAL_PROJECT_CHARS = Pattern.compile("[^a-zA-Z0-9-!:\\.']");
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
              + " is not a valid ID. Only letters, numbers, hyphens, single quotes, colon, dot and"
              + " exclamation points are allowed.");
    }
  }

  /**
   * Cleanup Resources from the given ResourceManagers. It will guarantee that all the cleanups are
   * invoked, but still throws / bubbles the first exception at the end if something went wrong.
   *
   * @param managers Varargs of the managers to clean
   */
  public static void cleanResources(ResourceManager... managers) {

    if (managers == null || managers.length == 0) {
      return;
    }

    Exception bubbleException = null;

    for (ResourceManager manager : managers) {
      if (manager == null) {
        continue;
      }
      try {
        LOG.info("Cleaning up resource manager {}", manager.getClass().getSimpleName());
        manager.cleanupAll();
      } catch (Exception e) {
        LOG.error("Error cleaning the resource manager {}", manager.getClass().getSimpleName());
        if (bubbleException == null) {
          bubbleException = e;
        }
      }
    }

    if (bubbleException != null) {
      throw new RuntimeException("Error cleaning up resources", bubbleException);
    }
  }

  /**
   * Generates a password using random characters for tests.
   *
   * <p>Note: The password generated is not cryptographically secure and should only be used in
   * tests.
   *
   * @param minLength minimum length of password
   * @param maxLength maximum length of password
   * @param numLower number of lower case letters
   * @param numUpper number of upper case letters
   * @param numSpecial number of special characters
   * @param specialChars special characters to use
   * @return
   */
  public static String generatePassword(
      int minLength,
      int maxLength,
      int numLower,
      int numUpper,
      int numSpecial,
      @Nullable List<Character> specialChars) {
    StringBuilder password = new StringBuilder();
    password.append(
        RandomStringUtils.randomAlphanumeric(minLength, maxLength - numSpecial).toUpperCase());
    for (int i = 0; i < numSpecial && specialChars != null; i++) {
      password.insert(
          new Random().nextInt(password.length()),
          specialChars.get(new Random().nextInt(specialChars.size())));
    }
    for (int i = 0; i < numLower; i++) {
      password.insert(
          new Random().nextInt(password.length()),
          RandomStringUtils.randomAlphabetic(1).toLowerCase());
    }
    for (int i = 0; i < numUpper; i++) {
      password.insert(
          new Random().nextInt(password.length()),
          RandomStringUtils.randomAlphabetic(1).toUpperCase());
    }
    return password.toString();
  }
}
