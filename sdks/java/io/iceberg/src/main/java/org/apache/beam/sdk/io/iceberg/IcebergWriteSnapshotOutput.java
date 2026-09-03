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

import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;

/**
 * Output-row helpers for the Iceberg CDC write SchemaTransform provider ({@link
 * IcebergCdcWriteSchemaTransformProvider}): the {@code snapshots} output schema, the {@code
 * SnapshotInfo}-to-Row mapping, and the configuration-row conversion.
 *
 * <p>These deliberately mirror {@link IcebergWriteSchemaTransformProvider}'s private equivalents —
 * both providers expose an identical {@code snapshots} output shape — but the append provider is
 * intentionally left untouched by the CDC change, so it keeps its own copies rather than sharing
 * this helper (switching it over would modify a long-stable file for zero behavior change).
 */
final class IcebergWriteSnapshotOutput {

  private IcebergWriteSnapshotOutput() {}

  /**
   * The {@code snapshots} output schema: a {@code table} string plus {@link SnapshotInfo}'s fields.
   */
  static final Schema OUTPUT_SCHEMA =
      Schema.builder()
          .addStringField("table")
          .addFields(SnapshotInfo.getSchema().getFields())
          .build();

  /**
   * Maps a committed {@code (table, SnapshotInfo)} to a {@link Row} matching {@link
   * #OUTPUT_SCHEMA}.
   */
  static class SnapshotToRow extends SimpleFunction<KV<String, SnapshotInfo>, Row> {
    @Override
    public Row apply(KV<String, SnapshotInfo> input) {
      return Row.withSchema(OUTPUT_SCHEMA)
          .addValue(input.getKey())
          .addValues(input.getValue().toRow().getValues())
          .build();
    }
  }

  /**
   * Converts a SchemaTransform {@code Configuration} to its config {@link Row}, sorted
   * lexicographically and snake_cased to match SchemaTransform config naming conventions.
   */
  static <T> Row configurationRow(T configuration, Class<T> configurationClass) {
    try {
      return SchemaRegistry.createDefault()
          .getToRowFunction(configurationClass)
          .apply(configuration)
          .sorted()
          .toSnakeCase();
    } catch (NoSuchSchemaException e) {
      throw new RuntimeException(e);
    }
  }
}
