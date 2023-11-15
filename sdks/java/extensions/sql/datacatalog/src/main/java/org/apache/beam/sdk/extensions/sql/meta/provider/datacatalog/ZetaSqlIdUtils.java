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
package org.apache.beam.sdk.extensions.sql.meta.provider.datacatalog;

import static java.util.stream.Collectors.joining;

import java.util.List;
import java.util.regex.Pattern;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

/** Utils to work with ZetaSQL-compatible IDs. */
class ZetaSqlIdUtils {

  /**
   * Some special characters we explicitly handle.
   *
   * <p>Everything else is ignored, e.g. tabs, newlines, etc.
   */
  private static final Pattern SPECIAL_CHARS_ESCAPE =
      Pattern.compile(
          "(?<SpecialChar>["
              + "\\\\" // slash
              + "`" //    backtick
              + "'" //    single quote
              + "\"" //   double quote
              + "?" //    question mark
              + "])");

  private static final ImmutableMap<String, String> WHITESPACES =
      ImmutableMap.of(
          "\n", "\\\\n",
          "\t", "\\\\t",
          "\r", "\\\\r",
          "\f", "\\\\f");

  private static final Pattern SIMPLE_ID = Pattern.compile("[A-Za-z_][A-Za-z_0-9]*");

  /**
   * Joins parts into a single compound ZetaSQL identifier.
   *
   * <p>Escapes backticks, slashes, double and single quotes, doesn't handle other special
   * characters for now.
   */
  static String escapeAndJoin(List<String> parts) {
    return parts.stream()
        .map(ZetaSqlIdUtils::escapeSpecialChars)
        .map(ZetaSqlIdUtils::replaceWhitespaces)
        .map(ZetaSqlIdUtils::backtickIfNeeded)
        .collect(joining("."));
  }

  private static String escapeSpecialChars(String str) {
    return SPECIAL_CHARS_ESCAPE.matcher(str).replaceAll("\\\\${SpecialChar}");
  }

  private static String replaceWhitespaces(String s) {
    return WHITESPACES.keySet().stream()
        .reduce(s, (str, whitespace) -> str.replaceAll(whitespace, WHITESPACES.get(whitespace)));
  }

  private static String backtickIfNeeded(String s) {
    return SIMPLE_ID.matcher(s).matches() ? s : ("`" + s + "`");
  }
}
