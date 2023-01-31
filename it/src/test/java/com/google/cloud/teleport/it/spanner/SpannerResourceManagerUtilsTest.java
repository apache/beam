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

import static com.google.cloud.teleport.it.spanner.SpannerResourceManagerUtils.generateDatabaseId;
import static com.google.cloud.teleport.it.spanner.SpannerResourceManagerUtils.generateInstanceId;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.cloud.teleport.it.common.ResourceManagerUtilsTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link com.google.cloud.teleport.it.spanner.SpannerResourceManagerUtils}. */
@RunWith(JUnit4.class)
public final class SpannerResourceManagerUtilsTest extends ResourceManagerUtilsTest {

  @Test
  public void testGenerateInstanceIdShouldReplaceNonLetterFirstCharWithLetter() {
    String testBaseString = "0-test";

    String actual = generateInstanceId(testBaseString);

    assertThat(actual).matches("[a-z]-test-\\d{8}-\\d{6}-\\d{6}");
  }

  @Test
  public void testGenerateDatabaseIdShouldReplaceDigitLastCharWithLetter() {
    String testBaseString = "test_database_0";

    String actual = generateDatabaseId(testBaseString);

    assertThat(actual).matches("test_database_[a-z]");
  }

  @Test
  public void testGenerateDatabaseIdShouldReplaceDollarSignWithUnderscore() {
    String testBaseString = "test$database";

    String actual = generateDatabaseId(testBaseString);

    assertThat(actual).isEqualTo("test_database");
  }

  @Test
  public void testGenerateDatabaseIdShouldReplaceDotWithUnderscore() {
    String testBaseString = "test.database";

    String actual = generateDatabaseId(testBaseString);

    assertThat(actual).isEqualTo("test_database");
  }

  @Test
  public void testGenerateDatabaseIdShouldReplaceHyphenWithUnderscore() {
    String testBaseString = "test-database";

    String actual = generateDatabaseId(testBaseString);

    assertThat(actual).isEqualTo("test_database");
  }

  @Test
  public void testGenerateDatabaseIdShouldReplaceNonLetterFirstCharWithLetter() {
    String testBaseString = "0_test_database";

    String actual = generateDatabaseId(testBaseString);

    assertThat(actual).matches("[a-z]_test_database");
  }

  @Test
  public void testGenerateDatabaseIdShouldReplaceUpperCaseLettersWithLowerCase() {
    String testBaseString = "Test_Database";

    String actual = generateDatabaseId(testBaseString);

    assertThat(actual).isEqualTo("test_database");
  }

  @Test
  public void testGenerateDatabaseIdShouldTrimTrailingHyphen() {
    String testBaseString = "test_database---";

    String actual = generateDatabaseId(testBaseString);

    assertThat(actual).isEqualTo("test_database");
  }

  @Test
  public void testGenerateDatabaseIdShouldTrimTrailingUnderscore() {
    String testBaseString = "test_database___";

    String actual = generateDatabaseId(testBaseString);

    assertThat(actual).isEqualTo("test_database");
  }

  @Test
  public void testGenerateDatabaseIdShouldThrowErrorWithEmptyInput() {
    String testBaseString = "";

    assertThrows(IllegalArgumentException.class, () -> generateDatabaseId(testBaseString));
  }

  @Test
  public void testGenerateDatabaseIdShouldThrowErrorWhenInputContainsNoLettersOrNumbers() {
    String testBaseString = "---___$...__";

    assertThrows(IllegalArgumentException.class, () -> generateDatabaseId(testBaseString));
  }
}
