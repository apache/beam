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

import static com.google.cloud.teleport.it.common.ResourceManagerUtils.checkValidProjectId;
import static com.google.cloud.teleport.it.common.ResourceManagerUtils.generateNewId;
import static com.google.cloud.teleport.it.common.ResourceManagerUtils.generateResourceId;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.re2j.Pattern;
import java.time.format.DateTimeFormatter;
import org.junit.Test;

/** Unit tests for {@link com.google.cloud.teleport.it.common.ResourceManagerUtils}. */
public class ResourceManagerUtilsTest {

  private static final Pattern ILLEGAL_INSTANCE_CHARS = Pattern.compile("[^a-z0-9-]");
  private static final String REPLACE_INSTANCE_CHAR = "-";
  public static final int MAX_INSTANCE_ID_LENGTH = 36;
  private static final DateTimeFormatter TIME_FORMAT =
      DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss-SSSSSS");

  @Test
  public void testGenerateResourceIdShouldReplaceDollarSignWithHyphen() {
    String testBaseString = "test$instance";

    String actual =
        generateResourceId(
            testBaseString,
            ILLEGAL_INSTANCE_CHARS,
            REPLACE_INSTANCE_CHAR,
            MAX_INSTANCE_ID_LENGTH,
            TIME_FORMAT);

    assertThat(actual).matches("test-instance-\\d{8}-\\d{6}-\\d{6}");
  }

  @Test
  public void testGenerateResourceIdShouldReplaceDotWithHyphen() {
    String testBaseString = "test.instance";

    String actual =
        generateResourceId(
            testBaseString,
            ILLEGAL_INSTANCE_CHARS,
            REPLACE_INSTANCE_CHAR,
            MAX_INSTANCE_ID_LENGTH,
            TIME_FORMAT);

    assertThat(actual).matches("test-instance-\\d{8}-\\d{6}-\\d{6}");
  }

  @Test
  public void testGenerateResourceIdShouldReplaceUnderscoreWithHyphen() {
    String testBaseString = "test_inst";

    String actual =
        generateResourceId(
            testBaseString,
            ILLEGAL_INSTANCE_CHARS,
            REPLACE_INSTANCE_CHAR,
            MAX_INSTANCE_ID_LENGTH,
            TIME_FORMAT);

    assertThat(actual).matches("test-inst-\\d{8}-\\d{6}-\\d{6}");
  }

  @Test
  public void testGenerateResourceIdShouldReplaceUpperCaseLettersWithHyphen() {
    String testBaseString = "Test-Instance";

    String actual =
        generateResourceId(
            testBaseString,
            ILLEGAL_INSTANCE_CHARS,
            REPLACE_INSTANCE_CHAR,
            MAX_INSTANCE_ID_LENGTH,
            TIME_FORMAT);

    assertThat(actual).matches("-est--nstance-\\d{8}-\\d{6}-\\d{6}");
  }

  @Test
  public void testGenerateResourceIdShouldThrowErrorWithEmptyInput() {
    String testBaseString = "";

    assertThrows(
        IllegalArgumentException.class,
        () ->
            generateResourceId(
                testBaseString,
                ILLEGAL_INSTANCE_CHARS,
                REPLACE_INSTANCE_CHAR,
                MAX_INSTANCE_ID_LENGTH,
                TIME_FORMAT));
  }

  @Test
  public void testGenerateResourceIdShouldThrowErrorWithSingleLetterInput() {
    String testBaseString = "";

    assertThrows(
        IllegalArgumentException.class,
        () ->
            generateResourceId(
                testBaseString,
                ILLEGAL_INSTANCE_CHARS,
                REPLACE_INSTANCE_CHAR,
                MAX_INSTANCE_ID_LENGTH,
                TIME_FORMAT));
  }

  @Test
  public void testGenerateResourceIdWhenInputLengthIsLongerThanTargetLength() {
    String longId = "test_instance_long";

    String actual =
        generateResourceId(
            longId,
            ILLEGAL_INSTANCE_CHARS,
            REPLACE_INSTANCE_CHAR,
            MAX_INSTANCE_ID_LENGTH,
            TIME_FORMAT);

    assertThat(actual).matches("test-instance-\\d{8}-\\d{6}-\\d{6}");
  }

  @Test
  public void testGenerateNewIdShouldReturnNewIdWhenInputLengthIsLongerThanTargetLength() {
    String longId = "long-test-id-string";

    String actual = generateNewId(longId, 13);

    assertThat(actual).matches("long-([a-zA-Z0-9]){8}");
  }

  @Test
  public void testGenerateNewIdShouldReturnOldIdWhenInputLengthIsNotLongerThanTargetLength() {
    String shortId = "test-id";

    String actual = generateNewId(shortId, shortId.length());

    assertThat(actual).isEqualTo(shortId);
  }

  @Test
  public void testGenerateNewIdShouldThrowExceptionWhenTargetLengthIsNotGreaterThanEight() {
    String id = "long-test-id";

    assertThrows(IllegalArgumentException.class, () -> generateNewId(id, 8));
  }

  @Test
  public void testCheckValidProjectIdWhenIdIsEmpty() {
    assertThrows(IllegalArgumentException.class, () -> checkValidProjectId(""));
  }

  @Test
  public void testCheckValidProjectIdWhenIdIsTooShort() {
    assertThrows(IllegalArgumentException.class, () -> checkValidProjectId("abc"));
  }

  @Test
  public void testCheckValidProjectIdWhenIdIsTooLong() {
    assertThrows(
        IllegalArgumentException.class,
        () -> checkValidProjectId("really-really-really-really-long-project-id"));
  }

  @Test
  public void testCheckValidProjectIdWhenIdContainsIllegalCharacter() {
    assertThrows(IllegalArgumentException.class, () -> checkValidProjectId("%pr$oject-id%"));
  }

  @Test
  public void testCheckValidProjectIdWhenIdIsValid() {
    checkValidProjectId("'project-id-9'!");
  }
}
