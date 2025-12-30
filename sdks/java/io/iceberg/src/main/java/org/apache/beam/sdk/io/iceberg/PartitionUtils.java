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
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
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

  /**
   * Copied over from Apache Iceberg's <a
   * href="https://github.com/apache/iceberg/blob/main/core/src/main/java/org/apache/iceberg/util/PartitionUtil.java">PartitionUtil</a>.
   */
  public static Map<Integer, ?> constantsMap(
      PartitionSpec spec, ContentFile<?> file, BiFunction<Type, Object, Object> convertConstant) {
    StructLike partitionData = file.partition();

    // use java.util.HashMap because partition data may contain null values
    Map<Integer, Object> idToConstant = Maps.newHashMap();

    // add first_row_id as _row_id
    if (file.firstRowId() != null) {
      idToConstant.put(
          MetadataColumns.ROW_ID.fieldId(),
          convertConstant.apply(Types.LongType.get(), file.firstRowId()));
    }

    idToConstant.put(
        MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.fieldId(),
        convertConstant.apply(Types.LongType.get(), file.fileSequenceNumber()));

    // add _file
    idToConstant.put(
        MetadataColumns.FILE_PATH.fieldId(),
        convertConstant.apply(Types.StringType.get(), file.location()));

    // add _spec_id
    idToConstant.put(
        MetadataColumns.SPEC_ID.fieldId(),
        convertConstant.apply(Types.IntegerType.get(), file.specId()));

    List<Types.NestedField> partitionFields = spec.partitionType().fields();
    List<PartitionField> fields = spec.fields();
    for (int pos = 0; pos < fields.size(); pos += 1) {
      PartitionField field = fields.get(pos);
      if (field.transform().isIdentity()) {
        Object converted =
            convertConstant.apply(
                partitionFields.get(pos).type(), partitionData.get(pos, Object.class));
        idToConstant.put(field.sourceId(), converted);
      }
    }

    return idToConstant;
  }
}
