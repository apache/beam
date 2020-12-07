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
import static org.apache.beam.sdk.io.gcp.testing.BigtableTestUtils.FAMILY_TEST;
import static org.apache.beam.sdk.io.gcp.testing.BigtableTestUtils.LATER;
import static org.apache.beam.sdk.io.gcp.testing.BigtableTestUtils.NOW;
import static org.apache.beam.sdk.io.gcp.testing.BigtableTestUtils.booleanToByteArray;
import static org.apache.beam.sdk.io.gcp.testing.BigtableTestUtils.doubleToByteArray;

import com.google.bigtable.v2.Cell;
import com.google.bigtable.v2.Column;
import com.google.bigtable.v2.Family;
import com.google.bigtable.v2.Mutation;
import com.google.protobuf.ByteString;
import java.util.List;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.primitives.Longs;

public class TestUtils {

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
}
