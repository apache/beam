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
package org.apache.beam.sdk.schemas.transforms;

import static junit.framework.TestCase.assertEquals;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.List;
import org.apache.beam.sdk.TestUtils.KvMatcher;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

/** Tests for {@link CoGroup}. */
public class CoGroupTest {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  @Rule public transient ExpectedException thrown = ExpectedException.none();

  private static final Schema CG_SCHEMA_1 =
      Schema.builder()
          .addStringField("user")
          .addInt32Field("count")
          .addStringField("country")
          .build();

  private static final Schema SIMPLE_CG_KEY_SCHEMA =
      Schema.builder().addStringField("user").addStringField("country").build();
  private static final Schema SIMPLE_CG_OUTPUT_SCHEMA =
      Schema.builder()
          .addArrayField("pc1", FieldType.row(CG_SCHEMA_1))
          .addArrayField("pc2", FieldType.row(CG_SCHEMA_1))
          .addArrayField("pc3", FieldType.row(CG_SCHEMA_1))
          .build();

  @Test
  @Category(NeedsRunner.class)
  public void testCoGroupByFieldNames() {
    PCollection<Row> pc1 =
        pipeline
            .apply(
                "Create1",
                Create.of(
                    Row.withSchema(CG_SCHEMA_1).addValues("user1", 1, "us").build(),
                    Row.withSchema(CG_SCHEMA_1).addValues("user1", 2, "us").build(),
                    Row.withSchema(CG_SCHEMA_1).addValues("user1", 3, "il").build(),
                    Row.withSchema(CG_SCHEMA_1).addValues("user1", 4, "il").build(),
                    Row.withSchema(CG_SCHEMA_1).addValues("user2", 5, "fr").build(),
                    Row.withSchema(CG_SCHEMA_1).addValues("user2", 6, "fr").build(),
                    Row.withSchema(CG_SCHEMA_1).addValues("user2", 7, "ar").build(),
                    Row.withSchema(CG_SCHEMA_1).addValues("user2", 8, "ar").build()))
            .setRowSchema(CG_SCHEMA_1);
    PCollection<Row> pc2 =
        pipeline
            .apply(
                "Create2",
                Create.of(
                    Row.withSchema(CG_SCHEMA_1).addValues("user1", 9, "us").build(),
                    Row.withSchema(CG_SCHEMA_1).addValues("user1", 10, "us").build(),
                    Row.withSchema(CG_SCHEMA_1).addValues("user1", 11, "il").build(),
                    Row.withSchema(CG_SCHEMA_1).addValues("user1", 12, "il").build(),
                    Row.withSchema(CG_SCHEMA_1).addValues("user2", 13, "fr").build(),
                    Row.withSchema(CG_SCHEMA_1).addValues("user2", 14, "fr").build(),
                    Row.withSchema(CG_SCHEMA_1).addValues("user2", 15, "ar").build(),
                    Row.withSchema(CG_SCHEMA_1).addValues("user2", 16, "ar").build()))
            .setRowSchema(CG_SCHEMA_1);
    PCollection<Row> pc3 =
        pipeline
            .apply(
                "Create3",
                Create.of(
                    Row.withSchema(CG_SCHEMA_1).addValues("user1", 17, "us").build(),
                    Row.withSchema(CG_SCHEMA_1).addValues("user1", 18, "us").build(),
                    Row.withSchema(CG_SCHEMA_1).addValues("user1", 19, "il").build(),
                    Row.withSchema(CG_SCHEMA_1).addValues("user1", 20, "il").build(),
                    Row.withSchema(CG_SCHEMA_1).addValues("user2", 21, "fr").build(),
                    Row.withSchema(CG_SCHEMA_1).addValues("user2", 22, "fr").build(),
                    Row.withSchema(CG_SCHEMA_1).addValues("user2", 23, "ar").build(),
                    Row.withSchema(CG_SCHEMA_1).addValues("user2", 24, "ar").build()))
            .setRowSchema(CG_SCHEMA_1);

    Row key1 = Row.withSchema(SIMPLE_CG_KEY_SCHEMA).addValues("user1", "us").build();
    Row key1Joined =
        Row.withSchema(SIMPLE_CG_OUTPUT_SCHEMA)
            .addValue(
                Lists.newArrayList(
                    Row.withSchema(CG_SCHEMA_1).addValues("user1", 1, "us").build(),
                    Row.withSchema(CG_SCHEMA_1).addValues("user1", 2, "us").build()))
            .addValue(
                Lists.newArrayList(
                    Row.withSchema(CG_SCHEMA_1).addValues("user1", 9, "us").build(),
                    Row.withSchema(CG_SCHEMA_1).addValues("user1", 10, "us").build()))
            .addValue(
                Lists.newArrayList(
                    Row.withSchema(CG_SCHEMA_1).addValues("user1", 17, "us").build(),
                    Row.withSchema(CG_SCHEMA_1).addValues("user1", 18, "us").build()))
            .build();

    Row key2 = Row.withSchema(SIMPLE_CG_KEY_SCHEMA).addValues("user1", "il").build();
    Row key2Joined =
        Row.withSchema(SIMPLE_CG_OUTPUT_SCHEMA)
            .addValue(
                Lists.newArrayList(
                    Row.withSchema(CG_SCHEMA_1).addValues("user1", 3, "il").build(),
                    Row.withSchema(CG_SCHEMA_1).addValues("user1", 4, "il").build()))
            .addValue(
                Lists.newArrayList(
                    Row.withSchema(CG_SCHEMA_1).addValues("user1", 11, "il").build(),
                    Row.withSchema(CG_SCHEMA_1).addValues("user1", 12, "il").build()))
            .addValue(
                Lists.newArrayList(
                    Row.withSchema(CG_SCHEMA_1).addValues("user1", 19, "il").build(),
                    Row.withSchema(CG_SCHEMA_1).addValues("user1", 20, "il").build()))
            .build();

    Row key3 = Row.withSchema(SIMPLE_CG_KEY_SCHEMA).addValues("user2", "fr").build();
    Row key3Joined =
        Row.withSchema(SIMPLE_CG_OUTPUT_SCHEMA)
            .addValue(
                Lists.newArrayList(
                    Row.withSchema(CG_SCHEMA_1).addValues("user2", 5, "fr").build(),
                    Row.withSchema(CG_SCHEMA_1).addValues("user2", 6, "fr").build()))
            .addValue(
                Lists.newArrayList(
                    Row.withSchema(CG_SCHEMA_1).addValues("user2", 13, "fr").build(),
                    Row.withSchema(CG_SCHEMA_1).addValues("user2", 14, "fr").build()))
            .addValue(
                Lists.newArrayList(
                    Row.withSchema(CG_SCHEMA_1).addValues("user2", 21, "fr").build(),
                    Row.withSchema(CG_SCHEMA_1).addValues("user2", 22, "fr").build()))
            .build();

    Row key4 = Row.withSchema(SIMPLE_CG_KEY_SCHEMA).addValues("user2", "ar").build();
    Row key4Joined =
        Row.withSchema(SIMPLE_CG_OUTPUT_SCHEMA)
            .addValue(
                Lists.newArrayList(
                    Row.withSchema(CG_SCHEMA_1).addValues("user2", 7, "ar").build(),
                    Row.withSchema(CG_SCHEMA_1).addValues("user2", 8, "ar").build()))
            .addValue(
                Lists.newArrayList(
                    Row.withSchema(CG_SCHEMA_1).addValues("user2", 15, "ar").build(),
                    Row.withSchema(CG_SCHEMA_1).addValues("user2", 16, "ar").build()))
            .addValue(
                Lists.newArrayList(
                    Row.withSchema(CG_SCHEMA_1).addValues("user2", 23, "ar").build(),
                    Row.withSchema(CG_SCHEMA_1).addValues("user2", 24, "ar").build()))
            .build();

    PCollection<KV<Row, Row>> joined =
        PCollectionTuple.of(new TupleTag<>("pc1"), pc1)
            .and(new TupleTag<>("pc2"), pc2)
            .and(new TupleTag<>("pc3"), pc3)
            .apply("CoGroup", CoGroup.byFieldNames("user", "country"));
    List<KV<Row, Row>> expected =
        ImmutableList.of(
            KV.of(key1, key1Joined),
            KV.of(key2, key2Joined),
            KV.of(key3, key3Joined),
            KV.of(key4, key4Joined));
    PAssert.that(joined).satisfies(actual -> containsJoinedFields(expected, actual));
    pipeline.run();
  }

  private static final Schema CG_SCHEMA_2 =
      Schema.builder()
          .addStringField("user2")
          .addInt32Field("count2")
          .addStringField("country2")
          .build();

  private static final Schema CG_SCHEMA_3 =
      Schema.builder()
          .addStringField("user3")
          .addInt32Field("count3")
          .addStringField("country3")
          .build();

  @Test
  @Category(NeedsRunner.class)
  public void testCoGroupByDifferentFields() {
    PCollection<Row> pc1 =
        pipeline
            .apply(
                "Create1",
                Create.of(
                    Row.withSchema(CG_SCHEMA_1).addValues("user1", 1, "us").build(),
                    Row.withSchema(CG_SCHEMA_1).addValues("user1", 2, "us").build(),
                    Row.withSchema(CG_SCHEMA_1).addValues("user1", 3, "il").build(),
                    Row.withSchema(CG_SCHEMA_1).addValues("user1", 4, "il").build(),
                    Row.withSchema(CG_SCHEMA_1).addValues("user2", 5, "fr").build(),
                    Row.withSchema(CG_SCHEMA_1).addValues("user2", 6, "fr").build(),
                    Row.withSchema(CG_SCHEMA_1).addValues("user2", 7, "ar").build(),
                    Row.withSchema(CG_SCHEMA_1).addValues("user2", 8, "ar").build()))
            .setRowSchema(CG_SCHEMA_1);
    PCollection<Row> pc2 =
        pipeline
            .apply(
                "Create2",
                Create.of(
                    Row.withSchema(CG_SCHEMA_2).addValues("user1", 9, "us").build(),
                    Row.withSchema(CG_SCHEMA_2).addValues("user1", 10, "us").build(),
                    Row.withSchema(CG_SCHEMA_2).addValues("user1", 11, "il").build(),
                    Row.withSchema(CG_SCHEMA_2).addValues("user1", 12, "il").build(),
                    Row.withSchema(CG_SCHEMA_2).addValues("user2", 13, "fr").build(),
                    Row.withSchema(CG_SCHEMA_2).addValues("user2", 14, "fr").build(),
                    Row.withSchema(CG_SCHEMA_2).addValues("user2", 15, "ar").build(),
                    Row.withSchema(CG_SCHEMA_2).addValues("user2", 16, "ar").build()))
            .setRowSchema(CG_SCHEMA_2);
    PCollection<Row> pc3 =
        pipeline
            .apply(
                "Create3",
                Create.of(
                    Row.withSchema(CG_SCHEMA_3).addValues("user1", 17, "us").build(),
                    Row.withSchema(CG_SCHEMA_3).addValues("user1", 18, "us").build(),
                    Row.withSchema(CG_SCHEMA_3).addValues("user1", 19, "il").build(),
                    Row.withSchema(CG_SCHEMA_3).addValues("user1", 20, "il").build(),
                    Row.withSchema(CG_SCHEMA_3).addValues("user2", 21, "fr").build(),
                    Row.withSchema(CG_SCHEMA_3).addValues("user2", 22, "fr").build(),
                    Row.withSchema(CG_SCHEMA_3).addValues("user2", 23, "ar").build(),
                    Row.withSchema(CG_SCHEMA_3).addValues("user2", 24, "ar").build()))
            .setRowSchema(CG_SCHEMA_3);

    Row key1 = Row.withSchema(SIMPLE_CG_KEY_SCHEMA).addValues("user1", "us").build();
    Row key1Joined =
        Row.withSchema(SIMPLE_CG_OUTPUT_SCHEMA)
            .addValue(
                Lists.newArrayList(
                    Row.withSchema(CG_SCHEMA_1).addValues("user1", 1, "us").build(),
                    Row.withSchema(CG_SCHEMA_1).addValues("user1", 2, "us").build()))
            .addValue(
                Lists.newArrayList(
                    Row.withSchema(CG_SCHEMA_2).addValues("user1", 9, "us").build(),
                    Row.withSchema(CG_SCHEMA_2).addValues("user1", 10, "us").build()))
            .addValue(
                Lists.newArrayList(
                    Row.withSchema(CG_SCHEMA_3).addValues("user1", 17, "us").build(),
                    Row.withSchema(CG_SCHEMA_3).addValues("user1", 18, "us").build()))
            .build();

    Row key2 = Row.withSchema(SIMPLE_CG_KEY_SCHEMA).addValues("user1", "il").build();
    Row key2Joined =
        Row.withSchema(SIMPLE_CG_OUTPUT_SCHEMA)
            .addValue(
                Lists.newArrayList(
                    Row.withSchema(CG_SCHEMA_1).addValues("user1", 3, "il").build(),
                    Row.withSchema(CG_SCHEMA_1).addValues("user1", 4, "il").build()))
            .addValue(
                Lists.newArrayList(
                    Row.withSchema(CG_SCHEMA_2).addValues("user1", 11, "il").build(),
                    Row.withSchema(CG_SCHEMA_2).addValues("user1", 12, "il").build()))
            .addValue(
                Lists.newArrayList(
                    Row.withSchema(CG_SCHEMA_3).addValues("user1", 19, "il").build(),
                    Row.withSchema(CG_SCHEMA_3).addValues("user1", 20, "il").build()))
            .build();

    Row key3 = Row.withSchema(SIMPLE_CG_KEY_SCHEMA).addValues("user2", "fr").build();
    Row key3Joined =
        Row.withSchema(SIMPLE_CG_OUTPUT_SCHEMA)
            .addValue(
                Lists.newArrayList(
                    Row.withSchema(CG_SCHEMA_1).addValues("user2", 5, "fr").build(),
                    Row.withSchema(CG_SCHEMA_1).addValues("user2", 6, "fr").build()))
            .addValue(
                Lists.newArrayList(
                    Row.withSchema(CG_SCHEMA_2).addValues("user2", 13, "fr").build(),
                    Row.withSchema(CG_SCHEMA_2).addValues("user2", 14, "fr").build()))
            .addValue(
                Lists.newArrayList(
                    Row.withSchema(CG_SCHEMA_3).addValues("user2", 21, "fr").build(),
                    Row.withSchema(CG_SCHEMA_3).addValues("user2", 22, "fr").build()))
            .build();

    Row key4 = Row.withSchema(SIMPLE_CG_KEY_SCHEMA).addValues("user2", "ar").build();
    Row key4Joined =
        Row.withSchema(SIMPLE_CG_OUTPUT_SCHEMA)
            .addValue(
                Lists.newArrayList(
                    Row.withSchema(CG_SCHEMA_1).addValues("user2", 7, "ar").build(),
                    Row.withSchema(CG_SCHEMA_1).addValues("user2", 8, "ar").build()))
            .addValue(
                Lists.newArrayList(
                    Row.withSchema(CG_SCHEMA_2).addValues("user2", 15, "ar").build(),
                    Row.withSchema(CG_SCHEMA_2).addValues("user2", 16, "ar").build()))
            .addValue(
                Lists.newArrayList(
                    Row.withSchema(CG_SCHEMA_3).addValues("user2", 23, "ar").build(),
                    Row.withSchema(CG_SCHEMA_3).addValues("user2", 24, "ar").build()))
            .build();

    TupleTag<Row> pc1Tag = new TupleTag<>("pc1");
    TupleTag<Row> pc2Tag = new TupleTag<>("pc2");
    TupleTag<Row> pc3Tag = new TupleTag<>("pc3");

    PCollection<KV<Row, Row>> joined =
        PCollectionTuple.of(pc1Tag, pc1)
            .and(pc2Tag, pc2)
            .and(pc3Tag, pc3)
            .apply(
                "CoGroup",
                CoGroup.byFieldNames(pc1Tag, "user", "country")
                    .byFieldNames(pc2Tag, "user2", "country2")
                    .byFieldNames(pc3Tag, "user3", "country3"));

    List<KV<Row, Row>> expected =
        ImmutableList.of(
            KV.of(key1, key1Joined),
            KV.of(key2, key2Joined),
            KV.of(key3, key3Joined),
            KV.of(key4, key4Joined));
    PAssert.that(joined).satisfies(actual -> containsJoinedFields(expected, actual));
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testUnderspecifiedCoGroup() {
    PCollection<Row> pc1 =
        pipeline
            .apply(
                "Create1",
                Create.of(Row.withSchema(CG_SCHEMA_1).addValues("user1", 1, "us").build()))
            .setRowSchema(CG_SCHEMA_1);
    PCollection<Row> pc2 =
        pipeline
            .apply(
                "Create2",
                Create.of(Row.withSchema(CG_SCHEMA_2).addValues("user1", 9, "us").build()))
            .setRowSchema(CG_SCHEMA_2);
    PCollection<Row> pc3 =
        pipeline.apply(
            "Create3", Create.of(Row.withSchema(CG_SCHEMA_3).addValues("user1", 17, "us").build()));
    TupleTag<Row> pc1Tag = new TupleTag<>("pc1");
    TupleTag<Row> pc2Tag = new TupleTag<>("pc2");
    TupleTag<Row> pc3Tag = new TupleTag<>("pc3");

    thrown.expect(IllegalStateException.class);
    PCollection<KV<Row, Row>> joined =
        PCollectionTuple.of(pc1Tag, pc1)
            .and(pc2Tag, pc2)
            .and(pc3Tag, pc3)
            .apply(
                "CoGroup",
                CoGroup.byFieldNames(pc1Tag, "user", "country")
                    .byFieldNames(pc2Tag, "user2", "country2"));
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testMismatchingKeys() {
    PCollection<Row> pc1 =
        pipeline
            .apply(
                "Create1",
                Create.of(Row.withSchema(CG_SCHEMA_1).addValues("user1", 1, "us").build()))
            .setRowSchema(CG_SCHEMA_1);
    PCollection<Row> pc2 =
        pipeline
            .apply(
                "Create2",
                Create.of(Row.withSchema(CG_SCHEMA_1).addValues("user1", 9, "us").build()))
            .setRowSchema(CG_SCHEMA_1);

    TupleTag<Row> pc1Tag = new TupleTag<>("pc1");
    TupleTag<Row> pc2Tag = new TupleTag<>("pc2");
    thrown.expect(IllegalStateException.class);
    PCollection<KV<Row, Row>> joined =
        PCollectionTuple.of(pc1Tag, pc1)
            .and(pc2Tag, pc2)
            .apply("CoGroup", CoGroup.byFieldNames(pc1Tag, "user").byFieldNames(pc2Tag, "count"));
    pipeline.run();
  }

  private static Void containsJoinedFields(
      List<KV<Row, Row>> expected, Iterable<KV<Row, Row>> actual) {
    List<Matcher<? super KV<Row, Row>>> matchers = Lists.newArrayList();
    for (KV<Row, Row> row : expected) {
      List<Matcher> fieldMatchers = Lists.newArrayList();
      Row value = row.getValue();
      Schema valueSchema = value.getSchema();
      for (int i = 0; i < valueSchema.getFieldCount(); ++i) {
        assertEquals(TypeName.ARRAY, valueSchema.getField(i).getType().getTypeName());
        fieldMatchers.add(new ArrayFieldMatchesAnyOrder(i, value.getArray(i)));
      }
      matchers.add(
          KvMatcher.isKv(equalTo(row.getKey()), allOf(fieldMatchers.toArray(new Matcher[0]))));
    }
    assertThat(actual, containsInAnyOrder(matchers.toArray(new Matcher[0])));
    return null;
  }

  static class ArrayFieldMatchesAnyOrder extends BaseMatcher<Row> {
    int fieldIndex;
    Row[] expected;

    ArrayFieldMatchesAnyOrder(int fieldIndex, List<Row> expected) {
      this.fieldIndex = fieldIndex;
      this.expected = expected.toArray(new Row[0]);
    }

    @Override
    public boolean matches(Object item) {
      if (!(item instanceof Row)) {
        return false;
      }
      Row row = (Row) item;
      List<Row> actual = row.getArray(fieldIndex);
      return containsInAnyOrder(expected).matches(actual);
    }

    @Override
    public void describeTo(Description description) {
      description.appendText("arrayFieldMatchesAnyOrder");
    }
  }
}
