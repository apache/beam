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

import static com.google.cloud.teleport.it.mongodb.MongoDBResourceManagerUtils.checkValidCollectionName;
import static com.google.cloud.teleport.it.mongodb.MongoDBResourceManagerUtils.generateDatabaseName;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link com.google.cloud.teleport.it.mongodb.MongoDBResourceManagerUtils}. */
@RunWith(JUnit4.class)
public class MongoDBResourceManagerUtilsTest {

  @Test
  public void testGenerateDatabaseNameShouldReplaceForwardSlash() {
    String testBaseString = "Test/DB/Name";
    String actual = generateDatabaseName(testBaseString);
    assertThat(actual).matches("Test-DB-Name-\\d{8}-\\d{6}-\\d{6}");
  }

  @Test
  public void testGenerateDatabaseNameShouldReplaceBackwardSlash() {
    String testBaseString = "Test\\DB\\Name";
    String actual = generateDatabaseName(testBaseString);
    assertThat(actual).matches("Test-DB-Name-\\d{8}-\\d{6}-\\d{6}");
  }

  @Test
  public void testGenerateDatabaseNameShouldReplacePeriod() {
    String testBaseString = "Test.DB.Name";
    String actual = generateDatabaseName(testBaseString);
    assertThat(actual).matches("Test-DB-Name-\\d{8}-\\d{6}-\\d{6}");
  }

  @Test
  public void testGenerateDatabaseNameShouldReplaceSpace() {
    String testBaseString = "Test DB Name";
    String actual = generateDatabaseName(testBaseString);
    assertThat(actual).matches("Test-DB-Name-\\d{8}-\\d{6}-\\d{6}");
  }

  @Test
  public void testGenerateDatabaseNameShouldReplaceDoubleQuotes() {
    String testBaseString = "Test\"DB\"Name";
    String actual = generateDatabaseName(testBaseString);
    assertThat(actual).matches("Test-DB-Name-\\d{8}-\\d{6}-\\d{6}");
  }

  @Test
  public void testGenerateDatabaseNameShouldReplaceDollarSign() {
    String testBaseString = "Test$DB$Name";
    String actual = generateDatabaseName(testBaseString);
    assertThat(actual).matches("Test-DB-Name-\\d{8}-\\d{6}-\\d{6}");
  }

  @Test
  public void testGenerateDatabaseNameShouldReplaceNullCharacter() {
    String testBaseString = "Test\0DB\0Name";
    String actual = generateDatabaseName(testBaseString);
    assertThat(actual).matches("Test-DB-Name-\\d{8}-\\d{6}-\\d{6}");
  }

  @Test
  public void testCheckValidCollectionNameThrowsErrorWhenNameIsTooShort() {
    assertThrows(
        IllegalArgumentException.class, () -> checkValidCollectionName("test-database", ""));
  }

  @Test
  public void testCheckValidCollectionNameThrowsErrorWhenNameIsTooLong() {
    assertThrows(
        IllegalArgumentException.class,
        () -> checkValidCollectionName("a".repeat(1), "b".repeat(100)));
    assertThrows(
        IllegalArgumentException.class,
        () -> checkValidCollectionName("a".repeat(50), "b".repeat(50)));
    assertThrows(
        IllegalArgumentException.class,
        () -> checkValidCollectionName("a".repeat(100), "b".repeat(1)));
  }

  @Test
  public void testCheckValidCollectionNameThrowsErrorWhenNameContainsDollarSign() {
    assertThrows(
        IllegalArgumentException.class,
        () -> checkValidCollectionName("test-database", "test$collection"));
  }

  @Test
  public void testCheckValidCollectionNameThrowsErrorWhenNameContainsNull() {
    assertThrows(
        IllegalArgumentException.class,
        () -> checkValidCollectionName("test-database", "test\0collection"));
  }

  @Test
  public void testCheckValidCollectionNameThrowsErrorWhenNameBeginsWithSystemKeyword() {
    assertThrows(
        IllegalArgumentException.class,
        () -> checkValidCollectionName("test-database", "system.test-collection"));
  }

  @Test
  public void testCheckValidCollectionNameThrowsErrorWhenNameDoesNotBeginWithLetterOrUnderscore() {
    assertThrows(
        IllegalArgumentException.class,
        () -> checkValidCollectionName("test-database", "1test-collection"));
  }

  @Test
  public void testCheckValidCollectionNameDoesNotThrowErrorWhenNameIsValid() {
    checkValidCollectionName("test-database", "a collection-name_valid.Test1");
    checkValidCollectionName("test-database", "_a collection-name_valid.Test1");
  }
}
