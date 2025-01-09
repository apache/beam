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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;

public class TestFixtures {
  public static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()), optional(2, "data", Types.StringType.get()));

  public static final List<Map<String, Object>> FILE1SNAPSHOT1_DATA =
      ImmutableList.of(
          ImmutableMap.of("id", 0L, "data", "clarification"),
          ImmutableMap.of("id", 1L, "data", "risky"),
          ImmutableMap.of("id", 2L, "data", "falafel"));
  public static final List<Map<String, Object>> FILE1SNAPSHOT2_DATA =
      ImmutableList.of(
          ImmutableMap.of("id", 3L, "data", "obscure"),
          ImmutableMap.of("id", 4L, "data", "secure"),
          ImmutableMap.of("id", 5L, "data", "feta"));
  public static final List<Map<String, Object>> FILE1SNAPSHOT3_DATA =
      ImmutableList.of(
          ImmutableMap.of("id", 6L, "data", "brainy"),
          ImmutableMap.of("id", 7L, "data", "film"),
          ImmutableMap.of("id", 8L, "data", "feta"));
  public static final List<Map<String, Object>> FILE2SNAPSHOT1_DATA =
      ImmutableList.of(
          ImmutableMap.of("id", 10L, "data", "clammy"),
          ImmutableMap.of("id", 11L, "data", "evacuate"),
          ImmutableMap.of("id", 12L, "data", "tissue"));
  public static final List<Map<String, Object>> FILE2SNAPSHOT2_DATA =
      ImmutableList.of(
          ImmutableMap.of("id", 14L, "data", "radical"),
          ImmutableMap.of("id", 15L, "data", "collocation"),
          ImmutableMap.of("id", 16L, "data", "book"));
  public static final List<Map<String, Object>> FILE2SNAPSHOT3_DATA =
      ImmutableList.of(
          ImmutableMap.of("id", 16L, "data", "cake"),
          ImmutableMap.of("id", 17L, "data", "intrinsic"),
          ImmutableMap.of("id", 18L, "data", "paper"));
  public static final List<Map<String, Object>> FILE3SNAPSHOT1_DATA =
      ImmutableList.of(
          ImmutableMap.of("id", 20L, "data", "ocean"),
          ImmutableMap.of("id", 21L, "data", "holistic"),
          ImmutableMap.of("id", 22L, "data", "preventative"));
  public static final List<Map<String, Object>> FILE3SNAPSHOT2_DATA =
      ImmutableList.of(
          ImmutableMap.of("id", 24L, "data", "cloud"),
          ImmutableMap.of("id", 25L, "data", "zen"),
          ImmutableMap.of("id", 26L, "data", "sky"));
  public static final List<Map<String, Object>> FILE3SNAPSHOT3_DATA =
      ImmutableList.of(
          ImmutableMap.of("id", 26L, "data", "belleview"),
          ImmutableMap.of("id", 27L, "data", "overview"),
          ImmutableMap.of("id", 28L, "data", "tender"));

  /* First file in test table */
  public static final List<Record> FILE1SNAPSHOT1 =
      Lists.transform(FILE1SNAPSHOT1_DATA, d -> createRecord(SCHEMA, d));
  public static final List<Record> FILE1SNAPSHOT2 =
      Lists.transform(FILE1SNAPSHOT2_DATA, d -> createRecord(SCHEMA, d));
  public static final List<Record> FILE1SNAPSHOT3 =
      Lists.transform(FILE1SNAPSHOT3_DATA, d -> createRecord(SCHEMA, d));

  /* Second file in test table */
  public static final List<Record> FILE2SNAPSHOT1 =
      Lists.transform(FILE2SNAPSHOT1_DATA, d -> createRecord(SCHEMA, d));
  public static final List<Record> FILE2SNAPSHOT2 =
      Lists.transform(FILE2SNAPSHOT2_DATA, d -> createRecord(SCHEMA, d));
  public static final List<Record> FILE2SNAPSHOT3 =
      Lists.transform(FILE2SNAPSHOT3_DATA, d -> createRecord(SCHEMA, d));

  /* Third file in test table */
  public static final List<Record> FILE3SNAPSHOT1 =
      Lists.transform(FILE3SNAPSHOT1_DATA, d -> createRecord(SCHEMA, d));
  public static final List<Record> FILE3SNAPSHOT2 =
      Lists.transform(FILE3SNAPSHOT2_DATA, d -> createRecord(SCHEMA, d));
  public static final List<Record> FILE3SNAPSHOT3 =
      Lists.transform(FILE3SNAPSHOT3_DATA, d -> createRecord(SCHEMA, d));

  public static final ImmutableList<Row> asRows(Iterable<Record> records) {
    ArrayList<Row> rows = new ArrayList<>();
    for (Record record : records) {
      rows.add(
          Row.withSchema(IcebergUtils.icebergSchemaToBeamSchema(SCHEMA))
              .withFieldValue("id", record.getField("id"))
              .withFieldValue("data", record.getField("data"))
              .build());
    }
    return ImmutableList.copyOf(rows);
  }

  public static Record createRecord(org.apache.iceberg.Schema schema, Map<String, Object> values) {
    return org.apache.iceberg.data.GenericRecord.create(schema).copy(values);
  }
}
