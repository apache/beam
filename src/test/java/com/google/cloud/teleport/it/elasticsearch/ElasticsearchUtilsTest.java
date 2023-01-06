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

import static com.google.cloud.teleport.it.elasticsearch.ElasticsearchUtils.checkValidIndexName;
import static com.google.cloud.teleport.it.elasticsearch.ElasticsearchUtils.generateIndexName;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link ElasticsearchUtils}. */
@RunWith(JUnit4.class)
public class ElasticsearchUtilsTest {

  @Test
  public void testGenerateIndexNameShouldReplaceForwardSlash() {
    String testBaseString = "Test/DB/Name";
    String actual = generateIndexName(testBaseString);
    assertThat(actual).matches("test-db-name-\\d{8}-\\d{6}-\\d{6}");
  }

  @Test
  public void testGenerateIndexNameShouldReplaceBackwardSlash() {
    String testBaseString = "Test\\DB\\Name";
    String actual = generateIndexName(testBaseString);
    assertThat(actual).matches("test-db-name-\\d{8}-\\d{6}-\\d{6}");
  }

  @Test
  public void testGenerateIndexNameShouldReplaceComma() {
    String testBaseString = "Test,DB,Name";
    String actual = generateIndexName(testBaseString);
    assertThat(actual).matches("test-db-name-\\d{8}-\\d{6}-\\d{6}");
  }

  @Test
  public void testGenerateIndexNameShouldReplaceSpace() {
    String testBaseString = "Test DB Name";
    String actual = generateIndexName(testBaseString);
    assertThat(actual).matches("test-db-name-\\d{8}-\\d{6}-\\d{6}");
  }

  @Test
  public void testGenerateIndexNameShouldReplaceDoubleQuotes() {
    String testBaseString = "Test\"DB\"Name";
    String actual = generateIndexName(testBaseString);
    assertThat(actual).matches("test-db-name-\\d{8}-\\d{6}-\\d{6}");
  }

  @Test
  public void testGenerateIndexNameShouldReplaceStar() {
    String testBaseString = "Test*DB*Name";
    String actual = generateIndexName(testBaseString);
    assertThat(actual).matches("test-db-name-\\d{8}-\\d{6}-\\d{6}");
  }

  @Test
  public void testGenerateIndexNameShouldReplaceNullCharacter() {
    String testBaseString = "Test\0DB\0Name";
    String actual = generateIndexName(testBaseString);
    assertThat(actual).matches("test-db-name-\\d{8}-\\d{6}-\\d{6}");
  }

  @Test
  public void testCheckValidIndexNameThrowsErrorWhenNameIsTooLong() {
    assertThrows(IllegalArgumentException.class, () -> checkValidIndexName("a".repeat(300)));
  }

  @Test
  public void testCheckValidIndexNameThrowsErrorWhenNameContainsPoundSymbol() {
    assertThrows(IllegalArgumentException.class, () -> checkValidIndexName("test#collection"));
  }

  @Test
  public void testCheckValidIndexNameThrowsErrorWhenNameContainsNull() {
    assertThrows(IllegalArgumentException.class, () -> checkValidIndexName("test\0collection"));
  }

  @Test
  public void testCheckValidIndexNameThrowsErrorWhenNameBeginsWithUnderscore() {
    assertThrows(IllegalArgumentException.class, () -> checkValidIndexName("_test-index"));
  }

  @Test
  public void testCheckValidIndexNameDoesNotThrowErrorWhenNameIsValid() {
    checkValidIndexName("a_collection-name_valid.Test1");
    checkValidIndexName("123_a_collection-name_valid.Test1");
  }
}
