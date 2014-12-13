/*
 * Copyright (C) 2014 Google Inc.
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

import com.google.common.base.Joiner;

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
   * Uses a simple strategy of converting each byte to a single char,
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

  /**
   * Returns a simple name for a class.
   *
   * <p> Note: this is non-invertible - the name may be simplified to an
   * extent that it cannot be mapped back to the original class.
   *
   * <p> This can be used to generate human-readable transform names.  It
   * removes the package from the name, and removes common suffixes.
   *
   * <p> Examples:
   * <ul>
   *   <li>{@code some.package.WordSummaryDoFn} -> "WordSummary"
   *   <li>{@code another.package.PairingFn} -> "Pairing"
   * </ul>
   */
  public static String approximateSimpleName(Class<?> clazz) {
    String fullName = clazz.getName();
    String shortName = fullName.substring(fullName.lastIndexOf('.') + 1);

    // Simplify inner class name by dropping outer class prefixes.
    Matcher m = NAMED_INNER_CLASS.matcher(shortName);
    if (m.matches()) {
      shortName = m.group("INNER");
    }

    // Drop common suffixes for each named component.
    String[] names = shortName.split("\\$");
    for (int i = 0; i < names.length; i++) {
      names[i] = simplifyNameComponent(names[i]);
    }

    return Joiner.on('$').join(names);
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
