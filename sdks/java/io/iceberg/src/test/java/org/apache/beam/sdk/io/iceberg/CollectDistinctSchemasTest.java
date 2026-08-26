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
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.iceberg.CollectDistinctSchemas.Group;
import org.apache.beam.sdk.io.iceberg.CollectDistinctSchemas.SchemaGroup;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.types.Types;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class CollectDistinctSchemasTest {
  @Rule public final TestPipeline pipeline = TestPipeline.create();

  private static final String ID_NAME =
      json(
          new Schema(
              required(1, "id", Types.IntegerType.get()),
              optional(2, "name", Types.StringType.get())));
  private static final String NAME_ID =
      json(
          new Schema(
              optional(1, "name", Types.StringType.get()),
              required(2, "id", Types.IntegerType.get())));
  private static final String ID_LONG_NAME =
      json(
          new Schema(
              required(1, "id", Types.LongType.get()),
              optional(2, "name", Types.StringType.get())));

  private static final List<String> NONE = Collections.emptyList();

  private final CollectDistinctSchemas fn = new CollectDistinctSchemas();

  @Test
  public void testDedupsIdenticalSchemas() {
    assertEquals(
        Arrays.asList(group(ID_NAME, 3, NONE)),
        combine(group(ID_NAME, 1, NONE), group(ID_NAME, 1, NONE), group(ID_NAME, 1, NONE)));
  }

  /** Schemas are compared as strings; canonicalization is ReadFooterSchema's job. */
  @Test
  public void testDifferentStringsAreDistinct() {
    List<SchemaGroup> out =
        combine(group(ID_NAME, 1, NONE), group(NAME_ID, 1, NONE), group(ID_LONG_NAME, 1, NONE));
    assertEquals(3, out.size());
    for (SchemaGroup entry : out) {
      assertEquals(1L, entry.files);
    }
  }

  @Test
  public void testMostCommonFirstThenJson() {
    List<SchemaGroup> out =
        combine(
            group(NAME_ID, 1, NONE),
            group(ID_LONG_NAME, 1, NONE),
            group(ID_NAME, 1, NONE),
            group(ID_LONG_NAME, 1, NONE),
            group(NAME_ID, 1, NONE));
    assertEquals(
        Arrays.asList(
            group(ID_LONG_NAME, 2, NONE), group(NAME_ID, 2, NONE), group(ID_NAME, 1, NONE)),
        out);
  }

  /**
   * A column counts as proven for the group only if every file proved it: one file with nulls in a
   * column is enough to make the table relax that column.
   */
  @Test
  public void testNullFreeColumnsIntersect() {
    List<SchemaGroup> out =
        combine(
            group(ID_NAME, 1, Arrays.asList("id", "name")),
            group(ID_NAME, 1, Arrays.asList("id")),
            group(NAME_ID, 1, Arrays.asList("name")));
    assertEquals(
        Arrays.asList(
            group(ID_NAME, 2, Arrays.asList("id")), group(NAME_ID, 1, Arrays.asList("name"))),
        out);
  }

  @Test
  public void testNullFreeColumnsIntersectAcrossMergedAccumulators() {
    Map<String, Group> first =
        fn.addInput(fn.createAccumulator(), group(ID_NAME, 1, Arrays.asList("id", "name")));
    Map<String, Group> second =
        fn.addInput(fn.createAccumulator(), group(ID_NAME, 1, Arrays.asList("name")));
    second = fn.addInput(second, group(NAME_ID, 1, Arrays.asList("id")));
    List<SchemaGroup> out = fn.extractOutput(fn.mergeAccumulators(Arrays.asList(first, second)));
    assertEquals(
        Arrays.asList(
            group(ID_NAME, 2, Arrays.asList("name")), group(NAME_ID, 1, Arrays.asList("id"))),
        out);
  }

  @Test
  public void testEmptyInput() {
    assertEquals(Arrays.asList(), combine());
  }

  @Test
  public void testAccumulatorCoderRoundTrip() throws Exception {
    Coder<Map<String, Group>> coder = fn.getAccumulatorCoder(null, null);
    Map<String, Group> accumulator =
        fn.addInput(fn.createAccumulator(), group(ID_NAME, 1, Arrays.asList("id")));
    accumulator = fn.addInput(accumulator, group(NAME_ID, 1, NONE));
    CoderProperties.coderDecodeEncodeEqual(coder, accumulator);
  }

  /** Coders from separate calls must compare equal, or coder inference treats them as different. */
  @Test
  public void testCodersFromSeparateCallsAreEqual() throws Exception {
    assertEquals(CollectDistinctSchemas.outputCoder(), CollectDistinctSchemas.outputCoder());
    assertEquals(fn.getAccumulatorCoder(null, null), fn.getAccumulatorCoder(null, null));
    // The output coder is deterministic; the accumulator coder is not required to be (MapCoder).
    CollectDistinctSchemas.outputCoder().verifyDeterministic();
  }

  @Test
  public void testPipeline() {
    PCollection<List<SchemaGroup>> out =
        pipeline
            .apply(
                Create.of(
                        group(ID_NAME, 1, Arrays.asList("id", "name")),
                        group(NAME_ID, 1, NONE),
                        group(ID_NAME, 1, Arrays.asList("id")))
                    .withCoder(CollectDistinctSchemas.groupCoder()))
            .apply(Combine.globally(new CollectDistinctSchemas()));
    PAssert.that(out)
        .containsInAnyOrder(
            Arrays.asList(group(ID_NAME, 2, Arrays.asList("id")), group(NAME_ID, 1, NONE)));
    pipeline.run();
  }

  private List<SchemaGroup> combine(SchemaGroup... files) {
    Map<String, Group> accumulator = fn.createAccumulator();
    for (SchemaGroup file : files) {
      accumulator = fn.addInput(accumulator, file);
    }
    return fn.extractOutput(accumulator);
  }

  private static SchemaGroup group(String json, long files, List<String> proven) {
    return new SchemaGroup(json, files, proven);
  }

  private static String json(Schema schema) {
    return SchemaParser.toJson(schema);
  }
}
