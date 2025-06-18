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

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.checkerframework.checker.nullness.qual.Nullable;

class PartitionUtils {
  private static final Pattern HOUR = Pattern.compile("^hour\\(([a-zA-Z0-9_-]+)\\)$");
  private static final Pattern DAY = Pattern.compile("^day\\(([a-zA-Z0-9_-]+)\\)$");
  private static final Pattern MONTH = Pattern.compile("^month\\(([a-zA-Z0-9_-]+)\\)$");
  private static final Pattern YEAR = Pattern.compile("^year\\(([a-zA-Z0-9_-]+)\\)$");
  private static final Pattern TRUNCATE =
      Pattern.compile("^truncate\\(([a-zA-Z0-9_-]+),\\s*(\\d+)\\)$");
  private static final Pattern BUCKET =
      Pattern.compile("^bucket\\(([a-zA-Z0-9_-]+),\\s*(\\d+)\\)$");
  private static final Pattern VOID = Pattern.compile("^void\\(([^)]+)\\)$");
  private static final Pattern IDENTITY = Pattern.compile("^([a-zA-Z0-9_-]+)$");

  private static final Map<
          Pattern, BiFunction<PartitionSpec.Builder, Matcher, PartitionSpec.Builder>>
      TRANSFORMATIONS =
          ImmutableMap.of(
              HOUR, (builder, matcher) -> builder.hour(checkStateNotNull(matcher.group(1))),
              DAY, (builder, matcher) -> builder.day(checkStateNotNull(matcher.group(1))),
              MONTH, (builder, matcher) -> builder.month(checkStateNotNull(matcher.group(1))),
              YEAR, (builder, matcher) -> builder.year(checkStateNotNull(matcher.group(1))),
              TRUNCATE,
                  (builder, matcher) ->
                      builder.truncate(
                          checkStateNotNull(matcher.group(1)),
                          Integer.parseInt(checkStateNotNull(matcher.group(2)))),
              BUCKET,
                  (builder, matcher) ->
                      builder.bucket(
                          checkStateNotNull(matcher.group(1)),
                          Integer.parseInt(checkStateNotNull(matcher.group(2)))),
              VOID, (builder, matcher) -> builder.alwaysNull(checkStateNotNull(matcher.group(1))),
              IDENTITY,
                  (builder, matcher) -> builder.identity(checkStateNotNull(matcher.group(1))));

  static PartitionSpec toPartitionSpec(
      @Nullable List<String> fields, org.apache.beam.sdk.schemas.Schema beamSchema) {
    if (fields == null) {
      return PartitionSpec.unpartitioned();
    }
    Schema schema = IcebergUtils.beamSchemaToIcebergSchema(beamSchema);
    PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);

    for (String field : fields) {
      boolean matched = false;
      for (Map.Entry<Pattern, BiFunction<PartitionSpec.Builder, Matcher, PartitionSpec.Builder>>
          entry : TRANSFORMATIONS.entrySet()) {
        Matcher matcher = entry.getKey().matcher(field);
        if (matcher.find()) {
          builder = entry.getValue().apply(builder, matcher);
          matched = true;
          break;
        }
      }
      if (!matched) {
        throw new IllegalArgumentException(
            "Could not find a partition transform for '" + field + "'.");
      }
    }

    return builder.build();
  }
}
