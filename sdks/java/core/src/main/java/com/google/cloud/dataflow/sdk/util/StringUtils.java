/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.util;

import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utilities for working with JSON and other human-readable string formats.
 */
public class StringUtils {
  /**
   * Converts the given array of bytes into a legal JSON string.
   *
   * <p>Uses a simple strategy of converting each byte to a single char,
   * except for non-printable chars, non-ASCII chars, and '%', '\',
   * and '"', which are encoded as three chars in '%xx' format, where
   * 'xx' is the hexadecimal encoding of the byte.
   */
  public static String byteArrayToJsonString(byte[] bytes) {
    StringBuilder sb = new StringBuilder(bytes.length * 2);
    for (byte b : bytes) {
      if (b >= 32 && b < 127) {
        // A printable ascii character.
        char c = (char) b;
        if (c != '%' && c != '\\' && c != '\"') {
          // Not an escape prefix or special character, either.
          // Send through unchanged.
          sb.append(c);
          continue;
        }
      }
      // Send through escaped.  Use '%xx' format.
      sb.append(String.format("%%%02x", b));
    }
    return sb.toString();
  }

  /**
   * Converts the given string, encoded using {@link #byteArrayToJsonString},
   * into a byte array.
   *
   * @throws IllegalArgumentException if the argument string is not legal
   */
  public static byte[] jsonStringToByteArray(String string) {
    List<Byte> bytes = new ArrayList<>();
    for (int i = 0; i < string.length(); ) {
      char c = string.charAt(i);
      Byte b;
      if (c == '%') {
        // Escaped.  Expect '%xx' format.
        try {
          b = (byte) Integer.parseInt(string.substring(i + 1, i + 3), 16);
        } catch (IndexOutOfBoundsException | NumberFormatException exn) {
          throw new IllegalArgumentException(
              "not in legal encoded format; " +
              "substring [" + i + ".." + (i + 2) + "] not in format \"%xx\"",
              exn);
        }
        i += 3;
      } else {
        // Send through unchanged.
        b = (byte) c;
        i++;
      }
      bytes.add(b);
    }
    byte[] byteArray = new byte[bytes.size()];
    int i = 0;
    for (Byte b : bytes) {
      byteArray[i++] = b;
    }
    return byteArray;
  }

  private static final String[] STANDARD_NAME_SUFFIXES =
      new String[]{"DoFn", "Fn"};

  /**
   * Pattern to match a non-anonymous inner class.
   * Eg, matches "Foo$Bar", or even "Foo$1$Bar", but not "Foo$1" or "Foo$1$2".
   */
  private static final Pattern NAMED_INNER_CLASS =
      Pattern.compile(".+\\$(?<INNER>[^0-9].*)");

  private static final String ANONYMOUS_CLASS_REGEX = "\\$[0-9]+\\$";

  /**
   * Returns a simple name for a class.
   *
   * <p>Note: this is non-invertible - the name may be simplified to an
   * extent that it cannot be mapped back to the original class.
   *
   * <p>This can be used to generate human-readable names. It
   * removes the package and outer classes from the name,
   * and removes common suffixes.
   *
   * <p>Examples:
   * <ul>
   *   <li>{@code some.package.Word.SummaryDoFn} -> "Summary"
   *   <li>{@code another.package.PairingFn} -> "Pairing"
   * </ul>
   *
   * @throws IllegalArgumentException if the class is anonymous
   */
  public static String approximateSimpleName(Class<?> clazz) {
    return approximateSimpleName(clazz, /* dropOuterClassNames */ true);
  }

  /**
   * Returns a name for a PTransform class.
   *
   * <p>This can be used to generate human-readable transform names. It
   * removes the package from the name, and removes common suffixes.
   *
   * <p>It is different than approximateSimpleName:
   * <ul>
   *   <li>1. It keeps the outer classes names.
   *   <li>2. It removes the common transform inner class: "Bound".
   * </ul>
   *
   * <p>Examples:
   * <ul>
   *   <li>{@code some.package.Word.Summary} -> "Word.Summary"
   *   <li>{@code another.package.Pairing.Bound} -> "Pairing"
   * </ul>
   */
  public static String approximatePTransformName(Class<?> clazz) {
    Preconditions.checkArgument(PTransform.class.isAssignableFrom(clazz));
    return approximateSimpleName(clazz, /* dropOuterClassNames */ false)
        .replaceFirst("\\.Bound$", "");
  }

  /**
   * Calculate the Levenshtein distance between two strings.
   *
   * <p>The Levenshtein distance between two words is the minimum number of single-character edits
   * (i.e. insertions, deletions or substitutions) required to change one string into the other.
   */
  public static int getLevenshteinDistance(final String s, final String t) {
    Preconditions.checkNotNull(s);
    Preconditions.checkNotNull(t);

    // base cases
    if (s.equals(t)) {
      return 0;
    }
    if (s.length() == 0) {
      return t.length();
    }
    if (t.length() == 0) {
      return s.length();
    }

    // create two work arrays to store integer distances
    final int[] v0 = new int[t.length() + 1];
    final int[] v1 = new int[t.length() + 1];

    // initialize v0 (the previous row of distances)
    // this row is A[0][i]: edit distance for an empty s
    // the distance is just the number of characters to delete from t
    for (int i = 0; i < v0.length; i++) {
      v0[i] = i;
    }

    for (int i = 0; i < s.length(); i++) {
      // calculate v1 (current row distances) from the previous row v0

      // first element of v1 is A[i+1][0]
      //   edit distance is delete (i+1) chars from s to match empty t
      v1[0] = i + 1;

      // use formula to fill in the rest of the row
      for (int j = 0; j < t.length(); j++) {
        int cost = (s.charAt(i) == t.charAt(j)) ? 0 : 1;
        v1[j + 1] = Math.min(Math.min(v1[j] + 1, v0[j + 1] + 1), v0[j] + cost);
      }

      // copy v1 (current row) to v0 (previous row) for next iteration
      System.arraycopy(v1, 0, v0, 0, v0.length);
    }

    return v1[t.length()];
  }

  private static String approximateSimpleName(Class<?> clazz, boolean dropOuterClassNames) {
    Preconditions.checkArgument(!clazz.isAnonymousClass(),
        "Attempted to get simple name of anonymous class");

    String fullName = clazz.getName();
    String shortName = fullName.substring(fullName.lastIndexOf('.') + 1);

    // Drop common suffixes for each named component.
    String[] names = shortName.split("\\$");
    for (int i = 0; i < names.length; i++) {
      names[i] = simplifyNameComponent(names[i]);
    }
    shortName = Joiner.on('$').join(names);

    if (dropOuterClassNames) {
      // Simplify inner class name by dropping outer class prefixes.
      Matcher m = NAMED_INNER_CLASS.matcher(shortName);
      if (m.matches()) {
        shortName = m.group("INNER");
      }
    } else {
      // Dropping anonymous outer classes
      shortName = shortName.replaceAll(ANONYMOUS_CLASS_REGEX, ".");
      shortName = shortName.replaceAll("\\$", ".");
    }
    return shortName;
  }

  private static String simplifyNameComponent(String name) {
    for (String suffix : STANDARD_NAME_SUFFIXES) {
      if (name.endsWith(suffix) && name.length() > suffix.length()) {
        return name.substring(0, name.length() - suffix.length());
      }
    }
    return name;
  }
}
