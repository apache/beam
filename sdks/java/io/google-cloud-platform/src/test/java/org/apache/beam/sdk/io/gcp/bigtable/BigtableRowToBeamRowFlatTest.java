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

import static org.apache.beam.sdk.io.gcp.bigtable.BigtableTestUtils.BOOL_COLUMN;
import static org.apache.beam.sdk.io.gcp.bigtable.BigtableTestUtils.DOUBLE_COLUMN;
import static org.apache.beam.sdk.io.gcp.bigtable.BigtableTestUtils.FAMILY_TEST;
import static org.apache.beam.sdk.io.gcp.bigtable.BigtableTestUtils.LONG_COLUMN;
import static org.apache.beam.sdk.io.gcp.bigtable.BigtableTestUtils.STRING_COLUMN;
import static org.apache.beam.sdk.io.gcp.bigtable.BigtableTestUtils.TEST_FLAT_SCHEMA;
import static org.apache.beam.sdk.io.gcp.bigtable.BigtableTestUtils.bigtableRow;

import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.junit.Rule;
import org.junit.Test;

public class BigtableRowToBeamRowFlatTest {
  @Rule public final TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testBigtableRowToBeamRowFlat() {
    Map<String, Set<String>> columnsMapping =
        ImmutableMap.of(
            FAMILY_TEST, ImmutableSet.of(BOOL_COLUMN, LONG_COLUMN, STRING_COLUMN, DOUBLE_COLUMN));

    PCollection<Row> rows =
        pipeline
            .apply(Create.of(bigtableRow(1), bigtableRow(2)))
            .setCoder(ProtoCoder.of(TypeDescriptor.of(com.google.bigtable.v2.Row.class)))
            .apply(new BigtableRowToBeamRowFlat(TEST_FLAT_SCHEMA, columnsMapping))
            .setRowSchema(TEST_FLAT_SCHEMA);

    PAssert.that(rows).containsInAnyOrder(row(1), row(2));
    pipeline.run().waitUntilFinish();
  }

  private Row row(long i) {
    return Row.withSchema(TEST_FLAT_SCHEMA).attachValues("key" + i, false, 2L, "value1", 5.5);
  }
}
