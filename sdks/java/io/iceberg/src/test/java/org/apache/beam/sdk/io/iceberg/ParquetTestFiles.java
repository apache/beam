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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.parquet.Parquet;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.NanoTime;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type.Repetition;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Writes Parquet files for the AddFiles metrics tests, with exactly the columns, field ids, row
 * groups and statistics a test asks for.
 */
final class ParquetTestFiles {
  /** 2024-01-01T00:00:00Z. */
  static final long EPOCH_SECONDS = 1704067200L;

  static final Map<String, String> FULL_METRICS =
      ImmutableMap.of("write.metadata.metrics.default", "full");

  static final PrimitiveType ID = column("id", PrimitiveTypeName.INT32, null);
  static final PrimitiveType FLAG = column("flag", PrimitiveTypeName.BOOLEAN, null);
  static final PrimitiveType NAME =
      column("name", PrimitiveTypeName.BINARY, LogicalTypeAnnotation.stringType());
  static final PrimitiveType TS_MICROS =
      column(
          "ts",
          PrimitiveTypeName.INT64,
          LogicalTypeAnnotation.timestampType(true, TimeUnit.MICROS));
  static final PrimitiveType TS_MILLIS =
      column(
          "ts",
          PrimitiveTypeName.INT64,
          LogicalTypeAnnotation.timestampType(true, TimeUnit.MILLIS));
  static final PrimitiveType TS_NANOS =
      column(
          "ts", PrimitiveTypeName.INT64, LogicalTypeAnnotation.timestampType(true, TimeUnit.NANOS));
  static final PrimitiveType TS_INT96 = column("ts", PrimitiveTypeName.INT96, null);
  static final PrimitiveType UNSIGNED =
      column("u", PrimitiveTypeName.INT32, LogicalTypeAnnotation.intType(32, false));

  private final File dir;

  ParquetTestFiles(File dir) {
    this.dir = dir;
  }

  static PrimitiveType column(
      String name, PrimitiveTypeName physical, @Nullable LogicalTypeAnnotation annotation) {
    org.apache.parquet.schema.Types.PrimitiveBuilder<PrimitiveType> builder =
        org.apache.parquet.schema.Types.primitive(physical, Repetition.OPTIONAL);
    if (annotation != null) {
      builder = builder.as(annotation);
    }
    return builder.named(name);
  }

  static Object[] row(@Nullable Object... values) {
    return values;
  }

  /** One row per array, one value per column; a null value leaves the column unset. */
  String write(String name, boolean statistics, List<PrimitiveType> columns, Object[]... rows)
      throws IOException {
    return write(name, statistics, 0, columns, rows);
  }

  /** Cuts a row group every {@code rowsPerGroup} rows; 0 writes a single row group. */
  String write(
      String name,
      boolean statistics,
      int rowsPerGroup,
      List<PrimitiveType> columns,
      Object[]... rows)
      throws IOException {
    MessageType type = new MessageType("root", new ArrayList<>(columns));
    File file = new File(dir, name);
    SimpleGroupFactory factory = new SimpleGroupFactory(type);
    ExampleParquetWriter.Builder builder =
        ExampleParquetWriter.builder(new Path(file.getAbsolutePath()))
            .withType(type)
            .withStatisticsEnabled(statistics);
    if (rowsPerGroup > 0) {
      builder =
          builder
              .withRowGroupSize(1L)
              .withMinRowCountForPageSizeCheck(rowsPerGroup)
              .withMaxRowCountForPageSizeCheck(rowsPerGroup);
    }
    try (ParquetWriter<Group> writer = builder.build()) {
      for (Object[] values : rows) {
        Group group = factory.newGroup();
        for (int i = 0; i < columns.size(); i++) {
          add(group, columns.get(i).getName(), values[i]);
        }
        writer.write(group);
      }
    }
    return file.getAbsolutePath();
  }

  private static void add(Group group, String column, @Nullable Object value) {
    if (value instanceof Long) {
      group.add(column, (Long) value);
    } else if (value instanceof Integer) {
      group.add(column, (Integer) value);
    } else if (value instanceof Boolean) {
      group.add(column, (Boolean) value);
    } else if (value instanceof String) {
      group.add(column, (String) value);
    } else if (value instanceof NanoTime) {
      group.add(column, (NanoTime) value);
    }
  }

  /** Writes with Iceberg's own writer, so every column carries {@code fileSchema}'s field id. */
  String writeWithIds(String name, Schema fileSchema, Record... records) throws IOException {
    String file = new File(dir, name).getAbsolutePath();
    DataWriter<Record> writer =
        Parquet.writeData(org.apache.iceberg.Files.localOutput(file))
            .schema(fileSchema)
            .withSpec(PartitionSpec.unpartitioned())
            .createWriterFunc(GenericParquetWriter::create)
            .build();
    try {
      for (Record record : records) {
        writer.write(record);
      }
    } finally {
      writer.close();
    }
    return file;
  }
}
