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
package org.apache.beam.sdk.schemas.variant;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Parser for Spark-style Variant JSON paths: {@code $}, followed by {@code .name}, {@code ['name']}
 * (or {@code ["name"]}) and {@code [index]} segments.
 */
final class VariantPath {
  private VariantPath() {}

  /** Returns the path segments: {@link String} for object keys, {@link Integer} for indexes. */
  static List<Object> parse(String path) {
    if (path == null || path.isEmpty() || path.charAt(0) != '$') {
      throw invalid(path, "path must start with '$'");
    }
    List<Object> segments = new ArrayList<>();
    int i = 1;
    int n = path.length();
    while (i < n) {
      char c = path.charAt(i);
      if (c == '.') {
        int start = ++i;
        while (i < n && path.charAt(i) != '.' && path.charAt(i) != '[') {
          i++;
        }
        if (i == start) {
          throw invalid(path, "empty key after '.'");
        }
        segments.add(path.substring(start, i));
      } else if (c == '[') {
        i++;
        if (i >= n) {
          throw invalid(path, "unterminated '['");
        }
        char q = path.charAt(i);
        if (q == '\'' || q == '"') {
          int end = path.indexOf(q, i + 1);
          if (end < 0 || end + 1 >= n || path.charAt(end + 1) != ']') {
            throw invalid(path, "unterminated quoted key");
          }
          segments.add(path.substring(i + 1, end));
          i = end + 2;
        } else {
          int end = path.indexOf(']', i);
          if (end < 0) {
            throw invalid(path, "unterminated '['");
          }
          String digits = path.substring(i, end);
          if (digits.isEmpty() || !digits.chars().allMatch(Character::isDigit)) {
            throw invalid(path, "array index must be a non-negative integer");
          }
          try {
            segments.add(Integer.parseInt(digits));
          } catch (NumberFormatException e) {
            throw invalid(path, "array index too large");
          }
          i = end + 1;
        }
      } else {
        throw invalid(path, "unexpected character '" + c + "' at position " + i);
      }
    }
    return Collections.unmodifiableList(segments);
  }

  private static IllegalArgumentException invalid(String path, String reason) {
    return new IllegalArgumentException("Invalid Variant path '" + path + "': " + reason);
  }
}
