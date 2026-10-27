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
package org.apache.beam.sdk.io.iceberg;

import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.beam.sdk.io.FileSystemUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Matches a file path against a glob with {@code {name}} placeholders, and reports what each
 * placeholder captured.
 *
 * <p>Pattern {@code gs://lake/export/{table}/**} over path {@code
 * gs://lake/export/orders/part-0.parquet} captures {@code table=orders}. The whole path has to
 * match, so the pattern decides both which table a file belongs to and whether it belongs at all.
 *
 * <p>A pattern may hold at most one {@code **}, before or after its placeholders. A second one can
 * leave a placeholder between the two with nothing fixing which directory it names, so two are
 * refused. {@code *} and {@code ?} are unrestricted: neither crosses a directory, so neither can
 * move a placeholder.
 *
 * <p>Two edges follow from Beam's glob translation. A {@code **} matches one directory or more,
 * never none, so a pattern with a {@code **} above its placeholder does not match a file with no
 * directory in that position. And a placeholder with nothing after it captures the file name rather
 * than a directory.
 */
final class PathPattern implements Serializable {
  private static final Pattern PLACEHOLDER = Pattern.compile("\\{(.+?)}");

  /** A name must survive rendering into a dotted table template: no dots, slashes or blanks. */
  private static final Pattern NAME = Pattern.compile("[A-Za-z0-9_-]+");

  private final Pattern regex;
  private final List<String> captureNames;

  private PathPattern(Pattern regex, List<String> captureNames) {
    this.regex = regex;
    this.captureNames = Collections.unmodifiableList(captureNames);
  }

  /**
   * Translates the path pattern to a regex, each placeholder becoming a capturing group. {@code
   * gs://lake/{db}/{tbl}/**} becomes {@code gs://lake/([^/]+)/([^/]+)/.*}, which a path has to
   * match in full.
   */
  static PathPattern compile(String pathPattern) {
    List<String> names = placeholders(pathPattern);
    checkNamesAreBindable(names, pathPattern);
    // the -1 keeps a trailing empty part, so "gs://lake/{tbl}" still yields a group
    String[] fixedParts = PLACEHOLDER.split(pathPattern, -1);
    checkAtMostOneDoubleStar(pathPattern);
    checkArgument(
        !pathPattern.endsWith("/"),
        "'%s' ends with '/', so it names a folder and matches no file; end it with a selector for"
            + " the files, such as '**'.",
        pathPattern);
    Pattern regex = Pattern.compile(toRegex(fixedParts));
    checkState(
        regex.matcher("").groupCount() == names.size(),
        "'%s' translated to a regex with %s groups for %s placeholders.",
        pathPattern,
        regex.matcher("").groupCount(),
        names.size());
    return new PathPattern(regex, names);
  }

  private static void checkNamesAreBindable(List<String> names, String pathPattern) {
    for (int i = 0; i < names.size(); i++) {
      String name = names.get(i);
      checkArgument(
          NAME.matcher(name).matches(),
          "Placeholder '%s' in '%s' is not a name; use letters, digits, '_' or '-'.",
          name,
          pathPattern);
      checkArgument(
          names.indexOf(name) == i,
          "Placeholder '%s' appears twice in '%s'; a name can only be captured once.",
          name,
          pathPattern);
    }
  }

  // Two '**' can sit either side of a placeholder, and then nothing fixes which directory it
  // names: gs://lake/**/{tbl}/** binds tbl to a different folder for every path depth. One '**'
  // cannot do that wherever it sits, since it is only ever on one side of a given placeholder,
  // so one is the limit. A '*' or '?' never crosses a directory and is not restricted.
  private static void checkAtMostOneDoubleStar(String pathPattern) {
    int first = pathPattern.indexOf("**");
    checkArgument(
        first < 0 || pathPattern.indexOf("**", first + 2) < 0,
        "'%s' holds more than one '**'. One is allowed, before or after the placeholders; a"
            + " second can leave a placeholder between them with no directory fixing it. Use '*'"
            + " for a single directory level.",
        pathPattern);
  }

  /**
   * Splits the pattern at its placeholders, expands the fixed text in between as a glob, and joins
   * those parts back with a capturing group wherever a placeholder was.
   *
   * <p>Globbing is Beam's own, through {@link FileSystemUtils}: {@code *} stays inside one
   * directory level, {@code **} crosses them, {@code ?} is one character. None of them capture, so
   * group numbers line up with {@code captureNames}.
   */
  private static String toRegex(String[] fixedParts) {
    List<String> expanded = new ArrayList<>();
    for (String fixedText : fixedParts) {
      expanded.add(FileSystemUtils.wildcardToRegexp(fixedText));
    }
    return String.join("([^/]+)", expanded);
  }

  List<String> captureNames() {
    return captureNames;
  }

  /** Captures by name plus the path below them, or null when the path does not match. */
  @Nullable Match match(String path) {
    Matcher matcher = regex.matcher(path);
    if (!matcher.matches()) {
      return null;
    }
    Map<String, String> captures = new LinkedHashMap<>();
    for (int i = 0; i < captureNames.size(); i++) {
      captures.put(captureNames.get(i), checkStateNotNull(matcher.group(i + 1)));
    }
    return new Match(captures, remainderBelow(matcher, path));
  }

  /**
   * The path below the last capture, file name included, so a {@code {db}/{tbl}/**} pattern over
   * {@code gs://lake/sales/orders/y=2024/part-0.parquet} leaves {@code y=2024/part-0.parquet}.
   *
   * <p>Empty when the last capture is not followed by a separator, which is the {@code
   * {table}.parquet} layout: there are no folders below the table, so there is nothing to inspect.
   */
  private String remainderBelow(Matcher matcher, String path) {
    if (captureNames.isEmpty()) {
      return "";
    }
    int end = matcher.end(captureNames.size());
    if (end >= path.length() || path.charAt(end) != '/') {
      return "";
    }
    return path.substring(end + 1);
  }

  /** What one path matched. */
  static final class Match {
    private final Map<String, String> captures;
    private final String remainder;

    private Match(Map<String, String> captures, String remainder) {
      this.captures = Collections.unmodifiableMap(captures);
      this.remainder = remainder;
    }

    /** In the order the placeholders appear, so a message about them reads like the pattern. */
    Map<String, String> captures() {
      return captures;
    }

    String remainder() {
      return remainder;
    }
  }

  /**
   * Fills a table template from a path's captures, so {@code warehouse.{db}.{tbl}} with {@code
   * db=sales, tbl=orders} gives {@code warehouse.sales.orders}.
   */
  static String render(String template, Map<String, String> captures) {
    // split with -1 gives one more literal part than there are placeholders, so literals[i + 1]
    // is always the text following names.get(i)
    String[] literals = PLACEHOLDER.split(template, -1);
    List<String> names = placeholders(template);
    StringBuilder rendered = new StringBuilder(literals[0]);
    for (int i = 0; i < names.size(); i++) {
      String name = names.get(i);
      String value =
          checkArgumentNotNull(
              captures.get(name),
              "Table '%s' uses {%s}, which the path did not capture; captured %s.",
              template,
              name,
              captures.keySet());
      rendered.append(value).append(literals[i + 1]);
    }
    return rendered.toString();
  }

  /**
   * Refuses a template this pattern cannot fill. Meant to run once at construction, so that a
   * {@code table} naming a placeholder the pattern never captures is a config error instead of one
   * error row per file.
   */
  void checkRenderable(String template) {
    for (String name : placeholders(template)) {
      checkArgument(
          captureNames.contains(name),
          "Table '%s' uses {%s}, which the path pattern does not capture; it captures %s.",
          template,
          name,
          captureNames);
    }
  }

  /**
   * Placeholder names in the order they appear, so {@code sales.{db}.{tbl}} gives {@code [db,
   * tbl]}.
   */
  static List<String> placeholders(String text) {
    List<String> names = new ArrayList<>();
    Matcher matcher = PLACEHOLDER.matcher(text);
    while (matcher.find()) {
      names.add(checkStateNotNull(matcher.group(1)));
    }
    return names;
  }
}
