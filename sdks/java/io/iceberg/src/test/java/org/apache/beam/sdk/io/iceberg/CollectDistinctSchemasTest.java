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
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
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

  private final CollectDistinctSchemas fn = new CollectDistinctSchemas();

  @Test
  public void testDedupsIdenticalSchemas() {
    assertEquals(Arrays.asList(KV.of(ID_NAME, 3L)), combine(ID_NAME, ID_NAME, ID_NAME));
  }

  /** Inputs are compared as strings; canonicalization is ReadFooterSchema's job. */
  @Test
  public void testDifferentStringsAreDistinct() {
    List<KV<String, Long>> out = combine(ID_NAME, NAME_ID, ID_LONG_NAME);
    assertEquals(3, out.size());
    for (KV<String, Long> entry : out) {
      assertEquals(Long.valueOf(1L), entry.getValue());
    }
  }

  @Test
  public void testMostCommonFirstThenJson() {
    List<KV<String, Long>> out = combine(NAME_ID, ID_LONG_NAME, ID_NAME, ID_LONG_NAME, NAME_ID);
    assertEquals(
        Arrays.asList(KV.of(ID_LONG_NAME, 2L), KV.of(NAME_ID, 2L), KV.of(ID_NAME, 1L)), out);
  }

  @Test
  public void testMergeSumsCounts() {
    Map<String, Long> first = fn.addInput(fn.createAccumulator(), ID_NAME);
    Map<String, Long> second = fn.addInput(fn.createAccumulator(), ID_NAME);
    second = fn.addInput(second, NAME_ID);
    List<KV<String, Long>> out =
        fn.extractOutput(fn.mergeAccumulators(Arrays.asList(first, second)));
    assertEquals(Arrays.asList(KV.of(ID_NAME, 2L), KV.of(NAME_ID, 1L)), out);
  }

  @Test
  public void testEmptyInput() {
    assertEquals(Arrays.asList(), combine());
  }

  @Test
  public void testAccumulatorCoderRoundTrip() throws Exception {
    Coder<Map<String, Long>> coder = fn.getAccumulatorCoder(null, null);
    Map<String, Long> accumulator = fn.addInput(fn.createAccumulator(), ID_NAME);
    accumulator = fn.addInput(accumulator, NAME_ID);
    CoderProperties.coderDecodeEncodeEqual(coder, accumulator);
  }

  @Test
  public void testPipeline() {
    PCollection<List<KV<String, Long>>> out =
        pipeline
            .apply(Create.of(ID_NAME, NAME_ID, ID_NAME))
            .apply(Combine.globally(new CollectDistinctSchemas()));
    PAssert.that(out).containsInAnyOrder(Arrays.asList(KV.of(ID_NAME, 2L), KV.of(NAME_ID, 1L)));
    pipeline.run();
  }

  private List<KV<String, Long>> combine(String... schemaJsons) {
    Map<String, Long> accumulator = fn.createAccumulator();
    for (String schemaJson : schemaJsons) {
      accumulator = fn.addInput(accumulator, schemaJson);
    }
    return fn.extractOutput(accumulator);
  }

  private static String json(Schema schema) {
    return SchemaParser.toJson(schema);
  }
}
