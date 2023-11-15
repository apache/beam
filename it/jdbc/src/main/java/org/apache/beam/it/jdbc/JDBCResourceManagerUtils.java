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

import static org.apache.beam.it.common.utils.ResourceManagerUtils.generatePassword;
import static org.apache.beam.it.common.utils.ResourceManagerUtils.generateResourceId;

import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.primitives.Chars;

/** Utilities for {@link JDBCResourceManager} implementations. */
final class JDBCResourceManagerUtils {

  // Naming restrictions can be found at:
  // mySQL -
  // https://documentation.sas.com/doc/en/pgmsascdc/9.4_3.5/acreldb/n0rfg6x1shw0ppn1cwhco6yn09f7.htm
  // postgreSQL -
  // https://documentation.sas.com/doc/en/pgmsascdc/9.4_3.5/acreldb/p1iw263fz6wvnbn1d6nyw71a9sf2.htm
  // oracle -
  // https://documentation.sas.com/doc/en/pgmsascdc/9.4_3.5/acreldb/p0t80fm3l1okawn1x3mvo098svir.htm
  //
  // The tightest restrictions were used across all flavors of JDBC for simplicity.
  private static final int MAX_DATABASE_NAME_LENGTH = 30;
  private static final Pattern ILLEGAL_DATABASE_NAME_CHARS = Pattern.compile("[^a-zA-Z0-9_$#]");
  private static final String REPLACE_DATABASE_NAME_CHAR = "_";
  private static final int MIN_PASSWORD_LENGTH = 9;
  private static final int MAX_PASSWORD_LENGTH = 25;
  private static final int MIN_TABLE_ID_LENGTH = 1;
  private static final int MAX_TABLE_ID_LENGTH = 30;
  private static final Pattern ILLEGAL_TABLE_CHARS = Pattern.compile("[/.]"); // i.e. [/.]
  private static final DateTimeFormatter TIME_FORMAT =
      DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss_SSSSSS");

  public static final List<Character> ALLOWED_SPECIAL_CHARS =
      Chars.asList("~!@#$%^&*()_-+={}[]/<>,.?:|".toCharArray());

  private JDBCResourceManagerUtils() {}

  /**
   * Generates a JDBC database name from a given string.
   *
   * @param baseString The string to generate the name from.
   * @return The database name string.
   */
  static String generateDatabaseName(String baseString) {
    baseString = Character.isLetter(baseString.charAt(0)) ? baseString : "d_" + baseString;
    return generateResourceId(
        baseString,
        ILLEGAL_DATABASE_NAME_CHARS,
        REPLACE_DATABASE_NAME_CHAR,
        MAX_DATABASE_NAME_LENGTH,
        TIME_FORMAT);
  }

  /**
   * Generates a secure, valid JDBC password.
   *
   * @return The generated password.
   */
  static String generateJdbcPassword() {
    int numLower = 2;
    int numUpper = 2;
    int numSpecial = 2;
    return generatePassword(
        MIN_PASSWORD_LENGTH,
        MAX_PASSWORD_LENGTH,
        numLower,
        numUpper,
        numSpecial,
        ALLOWED_SPECIAL_CHARS);
  }

  /**
   * Checks whether the given table name is valid according to JDBC constraints.
   *
   * @param nameToCheck the table name to check.
   * @throws IllegalArgumentException if the table name is invalid.
   */
  static void checkValidTableName(String nameToCheck) {
    if (nameToCheck.length() < MIN_TABLE_ID_LENGTH) {
      throw new IllegalArgumentException("Table name cannot be empty. ");
    }
    if (nameToCheck.length() > MAX_TABLE_ID_LENGTH) {
      throw new IllegalArgumentException(
          "Table name "
              + nameToCheck
              + " cannot be longer than "
              + MAX_TABLE_ID_LENGTH
              + " characters.");
    }
    if (ILLEGAL_TABLE_CHARS.matcher(nameToCheck).find()) {
      throw new IllegalArgumentException(
          "Table name "
              + nameToCheck
              + " is not a valid name. Periods and forward slashes are not allowed.");
    }
  }
}
