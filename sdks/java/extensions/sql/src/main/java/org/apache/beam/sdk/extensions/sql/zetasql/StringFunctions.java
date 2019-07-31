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
package org.apache.beam.sdk.extensions.sql.zetasql;

import java.util.regex.Pattern;
import org.apache.calcite.linq4j.function.Strict;
import org.apache.calcite.runtime.SqlFunctions;

/** StringFunctions. */
public class StringFunctions {
  public static final String SUBSTR_PARAMETER_EXCEED_INTEGER =
      "SUBSTR function only allows: "
          + Integer.MIN_VALUE
          + " <= position or length <= "
          + Integer.MAX_VALUE;

  @Strict
  public static Boolean startsWith(String str1, String str2) {
    return str1.startsWith(str2);
  }

  @Strict
  public static Boolean endsWith(String str1, String str2) {
    return str1.endsWith(str2);
  }

  @Strict
  public static String concat(String arg) {
    return arg;
  }

  @Strict
  public static String concat(String arg1, String arg2) {
    return concatIfNotIncludeNull(arg1, arg2);
  }

  @Strict
  public static String concat(String arg1, String arg2, String arg3) {
    return concatIfNotIncludeNull(arg1, arg2, arg3);
  }

  @Strict
  public static String concat(String arg1, String arg2, String arg3, String arg4) {
    return concatIfNotIncludeNull(arg1, arg2, arg3, arg4);
  }

  @Strict
  public static String concat(String arg1, String arg2, String arg3, String arg4, String arg5) {
    return concatIfNotIncludeNull(arg1, arg2, arg3, arg4, arg5);
  }

  @Strict
  private static String concatIfNotIncludeNull(String... args) {
    return String.join("", args);
  }

  // https://jira.apache.org/jira/browse/CALCITE-2889
  // public static String concat(String... args) {
  //   StringBuilder stringBuilder = new StringBuilder();
  //   for (String arg : args) {
  //     stringBuilder.append(arg);
  //   }
  //   return stringBuilder.toString();
  // }

  @Strict
  public static String replace(String origin, String target, String replacement) {
    // Java's string.replace behaves differently when target = "". When target = "",
    // Java's replace function replace every character in origin with replacement,
    // while origin value should not be changed is expected in SQL.
    if (target.length() == 0) {
      return origin;
    }

    return origin.replace(target, replacement);
  }

  public static String trim(String str) {
    return trim(str, " ");
  }

  @Strict
  public static String trim(String str, String seek) {
    return SqlFunctions.trim(true, true, seek, str, false);
  }

  public static String ltrim(String str) {
    return ltrim(str, " ");
  }

  @Strict
  public static String ltrim(String str, String seek) {
    return SqlFunctions.trim(true, false, seek, str, false);
  }

  public static String rtrim(String str) {
    return rtrim(str, " ");
  }

  @Strict
  public static String rtrim(String str, String seek) {
    return SqlFunctions.trim(false, true, seek, str, false);
  }

  public static String substr(String str, long from, long len) {
    if (from > Integer.MAX_VALUE
        || len > Integer.MAX_VALUE
        || from < Integer.MIN_VALUE
        || len < Integer.MIN_VALUE) {
      throw new RuntimeException(SUBSTR_PARAMETER_EXCEED_INTEGER);
    }
    return SqlFunctions.substring(str, (int) from, (int) len);
  }

  @Strict
  public static String reverse(String str) {
    return new StringBuilder(str).reverse().toString();
  }

  @Strict
  public static Long charLength(String str) {
    return (long) str.length();
  }

  // ZetaSQL's LIKE statement does not support the ESCAPE clause. Instead it
  // always uses \ as an escape character.
  @Strict
  public static Boolean like(String s, String pattern) {
    String regex = sqlToRegexLike(pattern, '\\');
    return Pattern.matches(regex, s);
  }

  private static final String JAVA_REGEX_SPECIALS = "[]()|^-+*?{}$\\.";

  /**
   * Translates a SQL LIKE pattern to Java regex pattern. Modified from Apache Calcite's
   * Like.sqlToRegexLike
   */
  private static String sqlToRegexLike(String sqlPattern, char escapeChar) {
    int i;
    final int len = sqlPattern.length();
    final StringBuilder javaPattern = new StringBuilder(len + len);
    for (i = 0; i < len; i++) {
      char c = sqlPattern.charAt(i);
      if (c == escapeChar) {
        if (i == (sqlPattern.length() - 1)) {
          throw new RuntimeException("LIKE pattern ends with a backslash");
        }
        char nextChar = sqlPattern.charAt(++i);
        if (JAVA_REGEX_SPECIALS.indexOf(nextChar) >= 0) {
          javaPattern.append('\\');
        }
        javaPattern.append(nextChar);
      } else if (c == '_') {
        javaPattern.append('.');
      } else if (c == '%') {
        javaPattern.append("(?s:.*)");
      } else {
        if (JAVA_REGEX_SPECIALS.indexOf(c) >= 0) {
          javaPattern.append('\\');
        }
        javaPattern.append(c);
      }
    }
    return javaPattern.toString();
  }
}
