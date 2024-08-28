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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Splitter;
import org.apache.commons.lang3.StringUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A utility that interpolates values in a pre-determined {@link String} using an input Beam {@link
 * Row}.
 *
 * <p>The {@link RowStringInterpolator} looks for field names specified inside {curly braces}. For
 * example, if the interpolator is configured with the String {@code "unified {foo} and streaming"},
 * it will look for a field name {@code "foo"} in the input {@link Row} and substitute in that
 * value. A {@link RowStringInterpolator} configured with a template String that contains no placeholders
 * (i.e. no curly braces), it will always return that String, untouched.
 *
 * <p>Nested fields can be specified using dot-notation (e.g. {@code "top.middle.nested"}).
 *
 * <p>Configure a {@link RowStringInterpolator} like so:
 *
 * <pre>{@code
 * String template = "unified {foo} and {bar.baz}!"
 * Row inputRow = ...
 *
 * RowStringInterpolator interpolator = new RowStringInterpolator(template, beamSchema);
 * String output = interpolator.interpolate(inputRow);
 * // output --> "unified batch and streaming!"
 * }</pre>
 */
public class RowStringInterpolator implements Serializable {
  private final String template;
  private final Set<String> fieldsToReplace;

  public RowStringInterpolator(String template, Schema rowSchema) {
    this.template = template;

    Matcher m = Pattern.compile("\\{(.+?)}").matcher(template);
    fieldsToReplace = new HashSet<>();
    while (m.find()) {
      fieldsToReplace.add(StringUtils.strip(m.group(), "{}"));
    }

    RowFilter.validateSchemaContainsFields(
        rowSchema, new ArrayList<>(fieldsToReplace), "string interpolation");
  }

  /** Performs string interpolation on the template using values from the input {@link Row}. */
  public String interpolate(Row row) {
    String interpolated = this.template;
    for (String field : fieldsToReplace) {
      List<String> levels = Splitter.on(".").splitToList(field);

      Object val = MoreObjects.firstNonNull(getValue(row, levels, 0), "");

      interpolated = interpolated.replace("{" + field + "}", String.valueOf(val));
    }
    return interpolated;
  }

  private @Nullable Object getValue(@Nullable Row row, List<String> fieldLevels, int index) {
    if (row == null || fieldLevels.isEmpty()) {
      return null;
    }
    Preconditions.checkState(
        index < fieldLevels.size(),
        "'%s' only goes %s levels deep. Invalid attempt to check for depth at level %s",
        String.join(".", fieldLevels),
        fieldLevels.size(),
        index);

    String currentField = fieldLevels.get(index);
    Object val;
    try {
      val = row.getValue(currentField);
    } catch (IllegalArgumentException e) {
      throw new RuntimeException(
          String.format(
              "Invalid row does not contain field '%s'.",
              String.join(".", fieldLevels.subList(0, index + 1))),
          e);
    }

    // base case: we've reached the target
    if (index == fieldLevels.size() - 1) {
      return val;
    }

    return getValue((Row) val, fieldLevels, ++index);
  }
}
