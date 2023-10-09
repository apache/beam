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
package org.apache.beam.it.jdbc;

import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.jdbc.JDBCResourceManagerUtils.ALLOWED_SPECIAL_CHARS;
import static org.apache.beam.it.jdbc.JDBCResourceManagerUtils.checkValidTableName;
import static org.apache.beam.it.jdbc.JDBCResourceManagerUtils.generateDatabaseName;
import static org.apache.beam.it.jdbc.JDBCResourceManagerUtils.generateJdbcPassword;
import static org.junit.Assert.assertThrows;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link JDBCResourceManagerUtils}. */
@RunWith(JUnit4.class)
public class JDBCResourceManagerUtilsTest {

  @Test
  public void testGenerateDatabaseNameShouldReplaceHyphen() {
    String testBaseString = "test-id";
    String actual = generateDatabaseName(testBaseString);
    assertThat(actual).matches("test_id_\\d{8}_\\d{6}_\\d{6}");
  }

  @Test
  public void testGenerateDatabaseNameShouldReplaceIllegalCharacters() {
    String testBaseString = "!@#_()";
    String actual = generateDatabaseName(testBaseString);
    assertThat(actual).matches("d___#___\\d{8}_\\d{6}_\\d{6}");
  }

  @Test
  public void testGeneratePasswordMeetsRequirements() {
    for (int i = 0; i < 10000; i++) {
      String password = generateJdbcPassword();
      int lower = 0;
      int upper = 0;
      int special = 0;

      for (int j = 0; j < password.length(); j++) {
        char c = password.charAt(j);
        String s = String.valueOf(c);
        lower += s.toLowerCase().equals(s) ? 1 : 0;
        upper += s.toUpperCase().equals(s) ? 1 : 0;
        special += ALLOWED_SPECIAL_CHARS.contains(c) ? 1 : 0;
      }

      assertThat(lower).isAtLeast(2);
      assertThat(upper).isAtLeast(2);
      assertThat(special).isAtLeast(2);
    }
  }

  @Test
  public void testCheckValidTableNameThrowsErrorWhenNameIsTooShort() {
    assertThrows(IllegalArgumentException.class, () -> checkValidTableName(""));
  }

  @Test
  public void testCheckValidTableNameThrowsErrorWhenNameIsTooLong() {
    assertThrows(
        IllegalArgumentException.class, () -> checkValidTableName(StringUtils.repeat("a", 31)));
  }

  @Test
  public void testCheckValidTableNameThrowsErrorWhenContainsBackslash() {
    assertThrows(IllegalArgumentException.class, () -> checkValidTableName("table/name"));
  }

  @Test
  public void testCheckValidTableNameThrowsErrorWhenContainsPeriod() {
    assertThrows(IllegalArgumentException.class, () -> checkValidTableName("table.name"));
  }

  @Test
  public void testCheckValidTableNameDoesNotThrowErrorWhenNameIsValid() {
    checkValidTableName("A-l3gal_t4ble NAME!");
  }
}
