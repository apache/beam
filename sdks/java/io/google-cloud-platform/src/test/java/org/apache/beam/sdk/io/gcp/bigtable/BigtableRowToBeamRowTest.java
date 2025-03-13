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

import static java.util.stream.Collectors.toList;
import static org.apache.beam.sdk.io.gcp.bigtable.BigtableTestUtils.FAMILY_TEST;
import static org.apache.beam.sdk.io.gcp.bigtable.BigtableTestUtils.LATER;
import static org.apache.beam.sdk.io.gcp.bigtable.BigtableTestUtils.LONG_COLUMN_SCHEMA;
import static org.apache.beam.sdk.io.gcp.bigtable.BigtableTestUtils.STRING_COLUMN;
import static org.apache.beam.sdk.io.gcp.bigtable.BigtableTestUtils.TEST_FAMILY_SCHEMA;
import static org.apache.beam.sdk.io.gcp.bigtable.BigtableTestUtils.TEST_SCHEMA;
import static org.apache.beam.sdk.io.gcp.bigtable.BigtableTestUtils.bigtableRow;

import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.junit.Rule;
import org.junit.Test;

public class BigtableRowToBeamRowTest {

  @Rule public final TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testBigtableRowToBeamRow() {
    PCollection<Row> rows =
        pipeline
            .apply(Create.of(bigtableRow(1), bigtableRow(2)))
            .setCoder(ProtoCoder.of(TypeDescriptor.of(com.google.bigtable.v2.Row.class)))
            .apply(new BigtableRowToBeamRow(TEST_SCHEMA))
            .setRowSchema(TEST_SCHEMA)
            .apply(MapElements.via(new SortStringColumn()))
            .setRowSchema(TEST_SCHEMA);

    PAssert.that(rows).containsInAnyOrder(row(1), row(2));
    pipeline.run().waitUntilFinish();
  }

  private Row row(long i) {
    return Row.withSchema(TEST_SCHEMA).attachValues("key" + i, familyRow());
  }

  private Row familyRow() {
    return Row.withSchema(TEST_FAMILY_SCHEMA)
        .attachValues(
            false,
            Row.withSchema(LONG_COLUMN_SCHEMA).attachValues(2L, LATER, ImmutableList.of("label1")),
            ImmutableList.of("value1", "value2"),
            5.5);
  }

  private static class SortStringColumn extends SimpleFunction<Row, Row> {
    @Override
    public Row apply(Row row) {
      Row familyRow = row.getRow(FAMILY_TEST);
      return Row.fromRow(row)
          .withFieldValue(
              FAMILY_TEST,
              Row.fromRow(familyRow)
                  .withFieldValue(
                      STRING_COLUMN,
                      familyRow.getArray(STRING_COLUMN).stream().sorted().collect(toList()))
                  .build())
          .build();
    }
  }
}
