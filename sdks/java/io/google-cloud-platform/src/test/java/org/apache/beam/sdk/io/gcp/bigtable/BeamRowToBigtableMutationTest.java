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
import static org.apache.beam.sdk.io.gcp.bigtable.BigtableTestUtils.BOOL_COLUMN;
import static org.apache.beam.sdk.io.gcp.bigtable.BigtableTestUtils.DOUBLE_COLUMN;
import static org.apache.beam.sdk.io.gcp.bigtable.BigtableTestUtils.FAMILY_TEST;
import static org.apache.beam.sdk.io.gcp.bigtable.BigtableTestUtils.LONG_COLUMN;
import static org.apache.beam.sdk.io.gcp.bigtable.BigtableTestUtils.STRING_COLUMN;
import static org.apache.beam.sdk.io.gcp.bigtable.BigtableTestUtils.TEST_FLAT_SCHEMA;
import static org.apache.beam.sdk.io.gcp.bigtable.BigtableTestUtils.rowMutation;

import com.google.bigtable.v2.Mutation;
import com.google.protobuf.ByteString;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.junit.Rule;
import org.junit.Test;

public class BeamRowToBigtableMutationTest {
  @Rule public final TestPipeline pipeline = TestPipeline.create();

  @Test
  @SuppressWarnings("unchecked")
  public void testBeamRowToBigtableMutation() {
    Map<String, Set<String>> columnsMapping =
        ImmutableMap.of(
            FAMILY_TEST, ImmutableSet.of(BOOL_COLUMN, LONG_COLUMN, STRING_COLUMN, DOUBLE_COLUMN));

    PCollection<KV<ByteString, Iterable<Mutation>>> rows =
        pipeline
            .apply(Create.of(row(1), row(2)))
            .setRowSchema(TEST_FLAT_SCHEMA)
            .apply(new BeamRowToBigtableMutation(columnsMapping))
            .apply(MapElements.via(new SortMutationsByColumns()));

    PAssert.that(rows).containsInAnyOrder(rowMutation(1), rowMutation(2));
    pipeline.run().waitUntilFinish();
  }

  private Row row(long i) {
    return Row.withSchema(TEST_FLAT_SCHEMA).attachValues("key" + i, false, 2L, "value1", 5.5);
  }

  private static class SortMutationsByColumns
      extends SimpleFunction<
          KV<ByteString, Iterable<Mutation>>, KV<ByteString, Iterable<Mutation>>> {
    @Override
    public KV<ByteString, Iterable<Mutation>> apply(KV<ByteString, Iterable<Mutation>> input) {
      return KV.of(
          input.getKey(),
          ImmutableList.copyOf(input.getValue()).stream()
              .sorted(
                  (mutation1, mutation2) -> {
                    String qualifier = mutation1.getSetCell().getColumnQualifier().toStringUtf8();
                    String other = mutation2.getSetCell().getColumnQualifier().toStringUtf8();
                    return qualifier.compareTo(other);
                  })
              .collect(toList()));
    }
  }
}
