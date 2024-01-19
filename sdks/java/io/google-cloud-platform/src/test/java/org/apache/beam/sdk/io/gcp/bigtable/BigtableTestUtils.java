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
package org.apache.beam.sdk.io.gcp.bigtable;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.beam.sdk.io.gcp.bigtable.RowUtils.KEY;
import static org.apache.beam.sdk.io.gcp.bigtable.RowUtils.LABELS;
import static org.apache.beam.sdk.io.gcp.bigtable.RowUtils.TIMESTAMP_MICROS;
import static org.apache.beam.sdk.io.gcp.bigtable.RowUtils.VALUE;
import static org.apache.beam.sdk.io.gcp.testing.BigtableUtils.booleanToByteArray;
import static org.apache.beam.sdk.io.gcp.testing.BigtableUtils.doubleToByteArray;

import com.google.bigtable.v2.Cell;
import com.google.bigtable.v2.Column;
import com.google.bigtable.v2.Family;
import com.google.bigtable.v2.Mutation;
import com.google.protobuf.ByteString;
import java.util.List;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.BigtableClientOverride;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.primitives.Longs;
import org.joda.time.Instant;

public class BigtableTestUtils {

  static final String BOOL_COLUMN = "boolColumn";
  static final String LONG_COLUMN = "longColumn";
  static final String STRING_COLUMN = "stringColumn";
  static final String DOUBLE_COLUMN = "doubleColumn";
  static final String FAMILY_TEST = "familyTest";

  static final Schema LONG_COLUMN_SCHEMA =
      Schema.builder()
          .addInt64Field(VALUE)
          .addInt64Field(TIMESTAMP_MICROS)
          .addArrayField(LABELS, Schema.FieldType.STRING)
          .build();

  static final Schema TEST_FAMILY_SCHEMA =
      Schema.builder()
          .addBooleanField(BOOL_COLUMN)
          .addRowField(LONG_COLUMN, LONG_COLUMN_SCHEMA)
          .addArrayField(STRING_COLUMN, Schema.FieldType.STRING)
          .addDoubleField(DOUBLE_COLUMN)
          .build();

  static final Schema TEST_SCHEMA =
      Schema.builder().addStringField(KEY).addRowField(FAMILY_TEST, TEST_FAMILY_SCHEMA).build();

  static final Schema TEST_FLAT_SCHEMA =
      Schema.builder()
          .addStringField(KEY)
          .addBooleanField(BOOL_COLUMN)
          .addInt64Field(LONG_COLUMN)
          .addStringField(STRING_COLUMN)
          .addDoubleField(DOUBLE_COLUMN)
          .build();

  static final long NOW = 5_000_000_000L;
  static final long LATER = NOW + 1_000L;

  static com.google.bigtable.v2.Row bigtableRow(long i) {
    return com.google.bigtable.v2.Row.newBuilder()
        .setKey(ByteString.copyFromUtf8("key" + i))
        .addFamilies(bigtableFamily())
        .build();
  }

  static KV<ByteString, Iterable<Mutation>> rowMutation(long i) {
    List<Mutation> mutations =
        ImmutableList.of(
            mutation("boolColumn", ByteString.copyFrom(booleanToByteArray(false))),
            mutation("doubleColumn", ByteString.copyFrom(doubleToByteArray(5.5))),
            mutation("longColumn", ByteString.copyFrom(Longs.toByteArray(2L))),
            mutation("stringColumn", ByteString.copyFromUtf8("value1")));
    return KV.of(ByteString.copyFrom(("key" + i).getBytes(UTF_8)), mutations);
  }

  private static Mutation mutation(String column, ByteString value) {
    return Mutation.newBuilder()
        .setSetCell(
            Mutation.SetCell.newBuilder()
                .setFamilyName(FAMILY_TEST)
                .setColumnQualifier(ByteString.copyFromUtf8(column))
                .setValue(value)
                .build())
        .build();
  }

  private static Family bigtableFamily() {
    return Family.newBuilder()
        .setName("familyTest")
        .addColumns(
            Column.newBuilder()
                .setQualifier(ByteString.copyFromUtf8("boolColumn"))
                .addCells(createCell(ByteString.copyFrom(booleanToByteArray(true)), NOW))
                .addCells(createCell(ByteString.copyFrom(booleanToByteArray(false)), LATER)))
        .addColumns(
            Column.newBuilder()
                .setQualifier(ByteString.copyFromUtf8("longColumn"))
                .addCells(createCell(ByteString.copyFrom(Longs.toByteArray(5L)), NOW, "label1"))
                .addCells(createCell(ByteString.copyFrom(Longs.toByteArray(2L)), LATER, "label1"))
                .build())
        .addColumns(
            Column.newBuilder()
                .setQualifier(ByteString.copyFromUtf8("stringColumn"))
                .addCells(createCell(ByteString.copyFromUtf8("value2"), NOW, "label1"))
                .addCells(createCell(ByteString.copyFromUtf8("value1"), LATER, "label1"))
                .build())
        .addColumns(
            Column.newBuilder()
                .setQualifier(ByteString.copyFromUtf8("doubleColumn"))
                .addCells(createCell(ByteString.copyFrom(doubleToByteArray(0.66)), NOW, "label1"))
                .addCells(createCell(ByteString.copyFrom(doubleToByteArray(5.5)), LATER, "label1"))
                .build())
        .addColumns(
            Column.newBuilder()
                .setQualifier(ByteString.copyFromUtf8("binaryColumn"))
                .addCells(createCell(ByteString.copyFrom(new byte[] {0, 1, 2}), NOW, "label1"))
                .addCells(createCell(ByteString.copyFrom(new byte[] {2, 1, 0}), LATER, "label2"))
                .build())
        .build();
  }

  private static Cell createCell(ByteString value, long timestamp, String... labels) {
    Cell.Builder builder = Cell.newBuilder().setValue(value).setTimestampMicros(timestamp);
    if (labels.length > 0) {
      builder.addAllLabels(ImmutableList.copyOf(labels));
    }
    return builder.build();
  }

  // We have to build the pipeline at this package level and not changestreams package because
  // endTime is package private and we can only create a pipeline with endTime here. Setting endTime
  // allows the tests to predictably terminate.
  public static BigtableIO.ReadChangeStream buildTestPipelineInput(
      String projectId,
      String instanceId,
      String tableId,
      String appProfileId,
      String metadataTableName,
      Instant startTime,
      Instant endTime,
      BigtableClientOverride clientOverride) {
    return BigtableIO.readChangeStream()
        .withProjectId(projectId)
        .withInstanceId(instanceId)
        .withTableId(tableId)
        .withAppProfileId(appProfileId)
        .withMetadataTableTableId(metadataTableName)
        .withStartTime(startTime)
        .withEndTime(endTime)
        .withBigtableClientOverride(clientOverride);
  }
}
