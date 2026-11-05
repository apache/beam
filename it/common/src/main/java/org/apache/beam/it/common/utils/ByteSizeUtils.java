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
package org.apache.beam.it.common.utils;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import java.io.IOException;
import java.util.Locale;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

/**
 * Utilities to read and print byte counts, so that a load test can take a size such as {@code
 * "40G"} on its command line and report it back in a human readable form.
 */
public final class ByteSizeUtils {

  private static final ImmutableMap<String, Long> UNIT_MAP =
      ImmutableMap.of(
          "k", 1024L, "m", 1024L * 1024, "g", 1024L * 1024 * 1024, "t", 1024L * 1024 * 1024 * 1024);

  /** Units {@link #formatBytes} prints, in increasing order. */
  private static final String[] DISPLAY_UNITS = {"B", "KB", "MB", "GB", "TB"};

  private ByteSizeUtils() {}

  /**
   * Parses a size into a number of bytes. A size is a number of bytes, optionally suffixed with one
   * of K, M, G or T, e.g. {@code "1024"}, {@code "64K"}, {@code "500M"} or {@code "40G"}. The
   * suffixes are binary, i.e. {@code 1K} is 1024 bytes.
   *
   * @param raw the size to parse, case insensitive and possibly padded with spaces
   * @return the number of bytes the size describes
   * @throws IllegalArgumentException if the size is not a number with a known suffix
   */
  public static long parseSizeToBytes(String raw) {
    String trimmed = raw.trim();
    int len = trimmed.length();
    if (len >= 2) {
      String suffix = trimmed.substring(len - 1).toLowerCase(Locale.ROOT);
      if (UNIT_MAP.containsKey(suffix)) {
        return Long.parseLong(trimmed.substring(0, len - 1)) * UNIT_MAP.get(suffix);
      }
    }
    return Long.parseLong(trimmed);
  }

  /**
   * Formats a number of bytes for a report, e.g. {@code 10,000,000,000 B (9.31 GB)}. The exact byte
   * count is always printed, the rounded form is only added when there is a unit to round to.
   *
   * @param bytes the number of bytes to format
   * @return the formatted byte count
   */
  public static String formatBytes(long bytes) {
    double value = bytes;
    int unit = 0;
    while (value >= 1024.0 && unit < DISPLAY_UNITS.length - 1) {
      value /= 1024.0;
      unit++;
    }
    return unit == 0
        ? String.format(Locale.US, "%,d B", bytes)
        : String.format(Locale.US, "%,d B (%.2f %s)", bytes, value, DISPLAY_UNITS[unit]);
  }

  /**
   * Jackson deserializer for a byte count that is either a number or a size such as {@code "10G"},
   * see {@link #parseSizeToBytes}. It lets a configuration json stay readable: {@code
   * "totalBytes":"10G"} instead of {@code "totalBytes":10737418240}.
   */
  public static final class Deserializer extends JsonDeserializer<Long> {
    @Override
    public Long deserialize(JsonParser parser, DeserializationContext context) throws IOException {
      JsonToken token = parser.currentToken();
      if (token == JsonToken.VALUE_NUMBER_INT || token == JsonToken.VALUE_NUMBER_FLOAT) {
        return parser.getLongValue();
      }
      return parseSizeToBytes(parser.getText());
    }
  }

  /**
   * Same as {@link Deserializer}, for the options that are declared as an {@code int}. A size that
   * does not fit in an {@code int} is rejected rather than silently truncated.
   */
  public static final class IntDeserializer extends JsonDeserializer<Integer> {
    @Override
    public Integer deserialize(JsonParser parser, DeserializationContext context)
        throws IOException {
      JsonToken token = parser.currentToken();
      if (token == JsonToken.VALUE_NUMBER_INT || token == JsonToken.VALUE_NUMBER_FLOAT) {
        return parser.getIntValue();
      }
      return Math.toIntExact(parseSizeToBytes(parser.getText()));
    }
  }
}
