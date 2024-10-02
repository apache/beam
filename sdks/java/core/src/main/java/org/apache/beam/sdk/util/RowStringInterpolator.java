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
package org.apache.beam.sdk.util;

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * A utility that interpolates values in a pre-determined {@link String} using an input Beam {@link
 * Row}.
 *
 * <p>The {@link RowStringInterpolator} looks for field names specified inside {curly braces}. For
 * example, if the interpolator is configured with the String {@code "unified {foo} and streaming"},
 * it will look for a field name {@code "foo"} in the input {@link Row} and substitute in that
 * value. If a {@link RowStringInterpolator} is configured with a template String that contains no
 * placeholders (i.e. no curly braces), it will simply return that String, untouched.
 *
 * <p>Nested fields can be specified using dot-notation (e.g. {@code "top.middle.nested"}).
 *
 * <p>Configure a {@link RowStringInterpolator} like so:
 *
 * <pre>{@code
 * String template = "unified {foo} and {bar.baz}!";
 * Row inputRow = {foo: "batch", bar: {baz: "streaming"}, ...};
 *
 * RowStringInterpolator interpolator = new RowStringInterpolator(template, beamSchema);
 * String output = interpolator.interpolate(inputRow, window, paneInfo, timestamp);
 * // output --> "unified batch and streaming!"
 * }</pre>
 *
 * <p>Additionally, {@link #interpolate(Row, BoundedWindow, PaneInfo, Instant)} can be used in
 * streaming scenarios to substitute windowing metadata into the template String. To make use of
 * this, use the relevant placeholder:
 *
 * <ul>
 *   <li>$WINDOW: the window's string representation
 *   <li>$PANE_INDEX: the pane's index
 *   <li>$YYYY: the element timestamp's year
 *   <li>$MM: the element timestamp's month
 *   <li>$DD: the element timestamp's day
 * </ul>
 *
 * <p>For example, your String template can look like:
 *
 * <pre>{@code "unified {foo} and {bar} since {$YYYY}-{$MM}!"}</pre>
 */
public class RowStringInterpolator implements Serializable {
  private final String template;
  private final Set<String> fieldsToReplace;
  // Represents the string representation of the element's window
  public static final String WINDOW = "$WINDOW";
  public static final String PANE_INDEX = "$PANE_INDEX";
  // Represents the element's pane index
  public static final String YYYY = "$YYYY";
  public static final String MM = "$MM";
  public static final String DD = "$DD";
  private static final Set<String> WINDOWING_METADATA =
      Sets.newHashSet(WINDOW, PANE_INDEX, YYYY, MM, DD);
  private static final Pattern TEMPLATE_PATTERN = Pattern.compile("\\{(.+?)}");

  /**
   * @param template a String template, potentially with placeholders in the form of curly braces,
   *     e.g. {@code "my {foo} template"}. During interpolation, these placeholders are replaced
   *     with values in the Beam Row. For more details and examples, refer to the top-level
   *     documentation.
   * @param rowSchema {@link Row}s used for interpolation are expected to be compatible with this
   *     {@link Schema}.
   */
  public RowStringInterpolator(String template, Schema rowSchema) {
    this.template = template;

    Matcher m = TEMPLATE_PATTERN.matcher(template);
    fieldsToReplace = new HashSet<>();
    while (m.find()) {
      fieldsToReplace.add(checkStateNotNull(m.group(1)));
    }

    List<String> rowFields =
        fieldsToReplace.stream()
            .filter(f -> !WINDOWING_METADATA.contains(f))
            .collect(Collectors.toList());

    RowFilter.validateSchemaContainsFields(rowSchema, rowFields, "string interpolation");
  }

  /**
   * Performs string interpolation on the template using values from the input {@link Row} and its
   * windowing metadata.
   */
  public String interpolate(ValueInSingleWindow<Row> element) {
    String interpolated = this.template;
    for (String field : fieldsToReplace) {
      Object val;
      Instant timestamp = element.getTimestamp();
      switch (field) {
        case WINDOW:
          val = element.getWindow().toString();
          break;
        case PANE_INDEX:
          val = element.getPane().getIndex();
          break;
        case YYYY:
          val = timestamp.getChronology().year().get(timestamp.getMillis());
          break;
        case MM:
          val = timestamp.getChronology().monthOfYear().get(timestamp.getMillis());
          break;
        case DD:
          val = timestamp.getChronology().dayOfMonth().get(timestamp.getMillis());
          break;
        default:
          val = MoreObjects.firstNonNull(getValue(element.getValue(), field), "");
          break;
      }

      interpolated = interpolated.replace("{" + field + "}", String.valueOf(val));
    }
    return interpolated;
  }

  private @Nullable Object getValue(@Nullable Row row, String fieldPath) {
    if (row == null) {
      return null;
    }
    int dotIndex = fieldPath.indexOf('.');
    String field = dotIndex == -1 ? fieldPath : fieldPath.substring(0, dotIndex);
    Preconditions.checkArgument(
        row.getSchema().hasField(field), "Invalid row does not contain field '%s'.", field);

    if (dotIndex == -1) {
      return row.getValue(field);
    }
    return getValue(row.getRow(field), fieldPath.substring(dotIndex + 1));
  }
}
