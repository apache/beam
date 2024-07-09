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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Splitter;
import org.apache.commons.lang3.StringUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

public class RowInterpolation {
  private static final Pattern REPLACE_PATTERN = Pattern.compile("\\{(.+?)}");
  private final String template;
  private final Set<String> fieldsToReplace;

  public RowInterpolation(String template, Schema rowSchema) {
    this.template = template;

    Matcher m = REPLACE_PATTERN.matcher(template);
    fieldsToReplace = new HashSet<>();
    while (m.find()) {
      fieldsToReplace.add(StringUtils.strip(m.group(), "{}"));
    }

    RowFilter.validateSchemaContainsFields(
        rowSchema, new ArrayList<>(fieldsToReplace), "string interpolation");
  }

  /** Performs string interpolation on the template using values from the input {@link Row}. */
  public String interpolateFrom(Row row) {
    String interpolated = this.template;
    for (String field : fieldsToReplace) {
      List<String> levels = Splitter.on(".").splitToList(field);

      Object val = MoreObjects.firstNonNull(getValue(row, levels), "");

      interpolated = interpolated.replace("{" + field + "}", String.valueOf(val));
    }
    return interpolated;
  }

  private @Nullable Object getValue(@Nullable Row row, List<String> fieldLevels) {
    if (row == null) {
      return null;
    }
    if (fieldLevels.size() == 1) {
      return row.getValue(fieldLevels.get(0));
    }
    return getValue(row.getRow(fieldLevels.get(0)), fieldLevels.subList(1, fieldLevels.size()));
  }
}
