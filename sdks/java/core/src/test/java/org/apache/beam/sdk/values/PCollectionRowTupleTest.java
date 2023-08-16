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
package org.apache.beam.sdk.values;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.testing.EqualsTester;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link PCollectionRowTuple}. */
@RunWith(JUnit4.class)
public final class PCollectionRowTupleTest implements Serializable {

  public static final Schema INT_SCHEMA = Schema.of(Field.of("int", FieldType.INT32));
  public static final Schema STRING_SCHEMA = Schema.of(Field.of("str", FieldType.STRING));
  public static final Schema BOOL_SCHEMA = Schema.of(Field.of("str", FieldType.BOOLEAN));

  @Rule
  public final transient TestPipeline pipeline =
      TestPipeline.create().enableAbandonedNodeEnforcement(false);

  List<Row> toRows(List<Object> ints, Schema schema) {
    return ints.stream()
        .map((value) -> Row.withSchema(schema).addValue(value).build())
        .collect(Collectors.toList());
  }

  @Test
  public void testOfThenHas() {
    PCollection<Row> pCollection =
        PCollection.createPrimitiveOutputInternal(
            pipeline,
            WindowingStrategy.globalDefault(),
            IsBounded.BOUNDED,
            RowCoder.of(INT_SCHEMA));
    String tag = "collection1";
    assertTrue(PCollectionRowTuple.of(tag, pCollection).has(tag));
  }

  @Test
  public void testEmpty() {
    String tag = "collection1";
    assertFalse(PCollectionRowTuple.empty(pipeline).has(tag));
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testComposePCollectionRowTuple() {
    pipeline.enableAbandonedNodeEnforcement(true);

    List<Row> inputs = toRows(Arrays.asList(3, -42, 77), INT_SCHEMA);

    PCollection<Row> mainInput = pipeline.apply("main", Create.of(inputs));
    PCollection<Row> secondInput = pipeline.apply("second", Create.of(inputs));

    PCollectionRowTuple tuple = PCollectionRowTuple.empty(pipeline);
    tuple = tuple.and("main", mainInput);
    tuple = tuple.and("second", secondInput);

    PAssert.that(tuple.get("main")).containsInAnyOrder(inputs);
    PAssert.that(tuple.get("second")).containsInAnyOrder(inputs);

    pipeline.run();
  }

  @Test
  public void testEquals() {
    TestPipeline p = TestPipeline.create();
    String intTag = "int";
    String strTag = "strs";

    PCollection<Row> ints =
        p.apply("ints", Create.of(toRows(Arrays.asList(3, -42, 77), INT_SCHEMA)));
    PCollection<Row> strs =
        p.apply("strs", Create.of(toRows(Arrays.asList("ab", "cd", "ef"), STRING_SCHEMA)));

    EqualsTester tester = new EqualsTester();
    // Empty tuples in the same pipeline are equal
    tester.addEqualityGroup(PCollectionRowTuple.empty(p), PCollectionRowTuple.empty(p));

    tester.addEqualityGroup(
        PCollectionRowTuple.of(intTag, ints).and(strTag, strs),
        PCollectionRowTuple.of(intTag, ints).and(strTag, strs));

    tester.addEqualityGroup(PCollectionRowTuple.of(intTag, ints));
    tester.addEqualityGroup(PCollectionRowTuple.of(strTag, strs));

    TestPipeline otherPipeline = TestPipeline.create();
    // Empty tuples in different pipelines are not equal
    tester.addEqualityGroup(PCollectionRowTuple.empty(otherPipeline));
    tester.testEquals();
  }

  @Test
  public void testExpandHasMatchingTags() {
    String intTag = "ints";
    String strTag = "strs";
    String boolTag = "bools";

    Pipeline p = TestPipeline.create();
    PCollection<Row> ints =
        p.apply("ints", Create.of(toRows(Arrays.asList(3, -42, 77), INT_SCHEMA)));
    PCollection<Row> strs =
        p.apply("strs", Create.of(toRows(Arrays.asList("ab", "cd", "ef"), STRING_SCHEMA)));
    PCollection<Row> bools =
        ints.apply(
            MapElements.via(
                new SimpleFunction<Row, Row>() {
                  @Override
                  public Row apply(Row input) {
                    Boolean result = input.getInt32(0) % 2 == 0;
                    return Row.withSchema(BOOL_SCHEMA).addValue(result).build();
                  }
                }));

    Map<String, PCollection<Row>> pcsByTag =
        ImmutableMap.<String, PCollection<Row>>builder()
            .put(strTag, strs)
            .put(intTag, ints)
            .put(boolTag, bools)
            .build();
    PCollectionRowTuple tuple =
        PCollectionRowTuple.of(intTag, ints).and(boolTag, bools).and(strTag, strs);
    assertThat(tuple.getAll(), equalTo(pcsByTag));
    PCollectionRowTuple reconstructed = PCollectionRowTuple.empty(p);
    for (Entry<TupleTag<?>, PValue> taggedValue : tuple.expand().entrySet()) {
      TupleTag<?> tag = taggedValue.getKey();
      PValue value = taggedValue.getValue();
      assertThat("The tag should map back to the value", tuple.get(tag.getId()), equalTo(value));
      assertThat(value, equalTo(pcsByTag.get(tag.getId())));
      reconstructed = reconstructed.and(tag.getId(), (PCollection) value);
    }

    assertThat(reconstructed, equalTo(tuple));
  }
}
