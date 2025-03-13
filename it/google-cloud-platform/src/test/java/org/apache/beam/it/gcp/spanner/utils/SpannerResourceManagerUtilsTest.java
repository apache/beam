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

import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.gcp.spanner.utils.SpannerResourceManagerUtils.generateDatabaseId;
import static org.apache.beam.it.gcp.spanner.utils.SpannerResourceManagerUtils.generateInstanceId;
import static org.junit.Assert.assertThrows;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link SpannerResourceManagerUtils}. */
@RunWith(JUnit4.class)
public final class SpannerResourceManagerUtilsTest {

  @Test
  public void testGenerateInstanceIdShouldReplaceNonLetterFirstCharWithLetter() {
    String testBaseString = "0-test";

    String actual = generateInstanceId(testBaseString);

    assertThat(actual).matches("[a-z]-test-\\d{8}-\\d{6}-\\d{6}");
  }

  @Test
  public void testGenerateDatabaseIdShouldNotReplaceDigitLastCharWithLetter() {
    String testBaseString = "db_0";

    String actual = generateDatabaseId(testBaseString);

    assertThat(actual).matches("db_0_\\d{8}_\\d{6}_\\d{6}");
  }

  @Test
  public void testGenerateDatabaseIdShouldReplaceDollarSignWithUnderscore() {
    String testBaseString = "t$db";

    String actual = generateDatabaseId(testBaseString);

    assertThat(actual).matches("t_db_\\d{8}_\\d{6}_\\d{6}");
  }

  @Test
  public void testGenerateDatabaseIdShouldReplaceDotWithUnderscore() {
    String testBaseString = "test.database";

    String actual = generateDatabaseId(testBaseString);

    assertThat(actual).matches("test_da_\\d{8}_\\d{6}_\\d{6}");
  }

  @Test
  public void testGenerateDatabaseIdShouldReplaceHyphenWithUnderscore() {
    String testBaseString = "test-database";

    String actual = generateDatabaseId(testBaseString);

    assertThat(actual).matches("test_da_\\d{8}_\\d{6}_\\d{6}");
  }

  @Test
  public void testGenerateDatabaseIdShouldReplaceNonLetterFirstCharWithLetter() {
    String testBaseString = "0_database";

    String actual = generateDatabaseId(testBaseString);

    assertThat(actual).matches("[a-z]_datab_\\d{8}_\\d{6}_\\d{6}");
  }

  @Test
  public void testGenerateDatabaseIdShouldReplaceUpperCaseLettersWithLowerCase() {
    String testBaseString = "TDa";

    String actual = generateDatabaseId(testBaseString);

    assertThat(actual).matches("tda_\\d{8}_\\d{6}_\\d{6}");
  }

  @Test
  public void testGenerateDatabaseIdShouldTrimTrailingUnderscore() {
    String testBaseString = "test_database___";

    String actual = generateDatabaseId(testBaseString);

    assertThat(actual).matches("test_da_\\d{8}_\\d{6}_\\d{6}");
  }

  @Test
  public void testGenerateDatabaseIdShouldThrowErrorWithEmptyInput() {
    String testBaseString = "";

    assertThrows(IllegalArgumentException.class, () -> generateDatabaseId(testBaseString));
  }
}
