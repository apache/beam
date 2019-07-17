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

import java.util.List;
import org.apache.beam.sdk.TestUtils.KvMatcher;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.schemas.transforms.CoGroup.By;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.UsesSchema;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link CoGroup}. */
@RunWith(JUnit4.class)
@Category(UsesSchema.class)
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
        PCollectionTuple.of("pc1", pc1, "pc2", pc2, "pc3", pc3)
            .apply("CoGroup", CoGroup.join(By.fieldNames("user", "country")));
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

    PCollection<KV<Row, Row>> joined =
        PCollectionTuple.of("pc1", pc1, "pc2", pc2, "pc3", pc3)
            .apply(
                "CoGroup",
                CoGroup.join("pc1", By.fieldNames("user", "country"))
                    .join("pc2", By.fieldNames("user2", "country2"))
                    .join("pc3", By.fieldNames("user3", "country3")));

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

    thrown.expect(IllegalArgumentException.class);
    PCollection<KV<Row, Row>> joined =
        PCollectionTuple.of("pc1", pc1, "pc2", pc2, "pc3", pc3)
            .apply(
                "CoGroup",
                CoGroup.join("pc1", By.fieldNames("user", "country"))
                    .join("pc2", By.fieldNames("user2", "country2")));
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

    thrown.expect(IllegalStateException.class);
    PCollection<KV<Row, Row>> joined =
        PCollectionTuple.of("pc1", pc1, "pc2", pc2)
            .apply(
                "CoGroup",
                CoGroup.join("pc1", By.fieldNames("user")).join("pc2", By.fieldNames("count")));
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testInnerJoin() {
    List<Row> pc1Rows =
        Lists.newArrayList(
            Row.withSchema(CG_SCHEMA_1).addValues("user1", 1, "us").build(),
            Row.withSchema(CG_SCHEMA_1).addValues("user1", 2, "us").build(),
            Row.withSchema(CG_SCHEMA_1).addValues("user1", 3, "il").build(),
            Row.withSchema(CG_SCHEMA_1).addValues("user1", 4, "il").build(),
            Row.withSchema(CG_SCHEMA_1).addValues("user2", 5, "fr").build(),
            Row.withSchema(CG_SCHEMA_1).addValues("user2", 6, "fr").build(),
            Row.withSchema(CG_SCHEMA_1).addValues("user2", 7, "ar").build(),
            Row.withSchema(CG_SCHEMA_1).addValues("user2", 8, "ar").build());
    List<Row> pc2Rows =
        Lists.newArrayList(
            Row.withSchema(CG_SCHEMA_2).addValues("user1", 9, "us").build(),
            Row.withSchema(CG_SCHEMA_2).addValues("user1", 10, "us").build(),
            Row.withSchema(CG_SCHEMA_2).addValues("user1", 11, "il").build(),
            Row.withSchema(CG_SCHEMA_2).addValues("user1", 12, "il").build(),
            Row.withSchema(CG_SCHEMA_2).addValues("user2", 13, "fr").build(),
            Row.withSchema(CG_SCHEMA_2).addValues("user2", 14, "fr").build(),
            Row.withSchema(CG_SCHEMA_2).addValues("user2", 15, "ar").build(),
            Row.withSchema(CG_SCHEMA_2).addValues("user2", 16, "ar").build());
    List<Row> pc3Rows =
        Lists.newArrayList(
            Row.withSchema(CG_SCHEMA_3).addValues("user1", 17, "us").build(),
            Row.withSchema(CG_SCHEMA_3).addValues("user1", 18, "us").build(),
            Row.withSchema(CG_SCHEMA_3).addValues("user1", 19, "il").build(),
            Row.withSchema(CG_SCHEMA_3).addValues("user1", 20, "il").build(),
            Row.withSchema(CG_SCHEMA_3).addValues("user2", 21, "fr").build(),
            Row.withSchema(CG_SCHEMA_3).addValues("user2", 22, "fr").build(),
            Row.withSchema(CG_SCHEMA_3).addValues("user2", 23, "ar").build(),
            Row.withSchema(CG_SCHEMA_3).addValues("user2", 24, "ar").build());

    PCollection<Row> pc1 = pipeline.apply("Create1", Create.of(pc1Rows)).setRowSchema(CG_SCHEMA_1);
    PCollection<Row> pc2 = pipeline.apply("Create2", Create.of(pc2Rows)).setRowSchema(CG_SCHEMA_2);
    PCollection<Row> pc3 = pipeline.apply("Create3", Create.of(pc3Rows)).setRowSchema(CG_SCHEMA_3);

    Schema expectedSchema =
        Schema.builder()
            .addRowField("pc1", CG_SCHEMA_1)
            .addRowField("pc2", CG_SCHEMA_2)
            .addRowField("pc3", CG_SCHEMA_3)
            .build();

    PCollection<Row> joined =
        PCollectionTuple.of("pc1", pc1, "pc2", pc2, "pc3", pc3)
            .apply(
                "CoGroup",
                CoGroup.join("pc1", By.fieldNames("user", "country"))
                    .join("pc2", By.fieldNames("user2", "country2"))
                    .join("pc3", By.fieldNames("user3", "country3"))
                    .crossProductJoin());
    assertEquals(expectedSchema, joined.getSchema());

    List<Row> expectedJoinedRows =
        JoinTestUtils.innerJoin(
            pc1Rows,
            pc2Rows,
            pc3Rows,
            new String[] {"user", "country"},
            new String[] {"user2", "country2"},
            new String[] {"user3", "country3"},
            expectedSchema);

    PAssert.that(joined).containsInAnyOrder(expectedJoinedRows);
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testFullOuterJoin() {
    List<Row> pc1Rows =
        Lists.newArrayList(
            Row.withSchema(CG_SCHEMA_1).addValues("user1", 1, "us").build(),
            Row.withSchema(CG_SCHEMA_1).addValues("user1", 2, "us").build(),
            Row.withSchema(CG_SCHEMA_1).addValues("user1", 3, "il").build(),
            Row.withSchema(CG_SCHEMA_1).addValues("user1", 4, "il").build(),
            Row.withSchema(CG_SCHEMA_1).addValues("user2", 5, "fr").build(),
            Row.withSchema(CG_SCHEMA_1).addValues("user2", 6, "fr").build(),
            Row.withSchema(CG_SCHEMA_1).addValues("user2", 7, "ar").build(),
            Row.withSchema(CG_SCHEMA_1).addValues("user2", 8, "ar").build(),
            Row.withSchema(CG_SCHEMA_1).addValues("user3", 7, "ar").build());

    List<Row> pc2Rows =
        Lists.newArrayList(
            Row.withSchema(CG_SCHEMA_2).addValues("user1", 9, "us").build(),
            Row.withSchema(CG_SCHEMA_2).addValues("user1", 10, "us").build(),
            Row.withSchema(CG_SCHEMA_2).addValues("user1", 11, "il").build(),
            Row.withSchema(CG_SCHEMA_2).addValues("user1", 12, "il").build(),
            Row.withSchema(CG_SCHEMA_2).addValues("user2", 13, "fr").build(),
            Row.withSchema(CG_SCHEMA_2).addValues("user2", 14, "fr").build(),
            Row.withSchema(CG_SCHEMA_2).addValues("user2", 15, "ar").build(),
            Row.withSchema(CG_SCHEMA_2).addValues("user2", 16, "ar").build(),
            Row.withSchema(CG_SCHEMA_2).addValues("user2", 16, "es").build());

    List<Row> pc3Rows =
        Lists.newArrayList(
            Row.withSchema(CG_SCHEMA_3).addValues("user1", 17, "us").build(),
            Row.withSchema(CG_SCHEMA_3).addValues("user1", 18, "us").build(),
            Row.withSchema(CG_SCHEMA_3).addValues("user1", 19, "il").build(),
            Row.withSchema(CG_SCHEMA_3).addValues("user1", 20, "il").build(),
            Row.withSchema(CG_SCHEMA_3).addValues("user2", 21, "fr").build(),
            Row.withSchema(CG_SCHEMA_3).addValues("user2", 22, "fr").build(),
            Row.withSchema(CG_SCHEMA_3).addValues("user2", 23, "ar").build(),
            Row.withSchema(CG_SCHEMA_3).addValues("user2", 24, "ar").build(),
            Row.withSchema(CG_SCHEMA_3).addValues("user27", 24, "se").build());

    PCollection<Row> pc1 = pipeline.apply("Create1", Create.of(pc1Rows)).setRowSchema(CG_SCHEMA_1);
    PCollection<Row> pc2 = pipeline.apply("Create2", Create.of(pc2Rows)).setRowSchema(CG_SCHEMA_2);
    PCollection<Row> pc3 = pipeline.apply("Create3", Create.of(pc3Rows)).setRowSchema(CG_SCHEMA_3);

    // Full outer join, so any field might be null.
    Schema expectedSchema =
        Schema.builder()
            .addNullableField("pc1", FieldType.row(CG_SCHEMA_1))
            .addNullableField("pc2", FieldType.row(CG_SCHEMA_2))
            .addNullableField("pc3", FieldType.row(CG_SCHEMA_3))
            .build();

    PCollection<Row> joined =
        PCollectionTuple.of("pc1", pc1, "pc2", pc2, "pc3", pc3)
            .apply(
                "CoGroup",
                CoGroup.join("pc1", By.fieldNames("user", "country").withOptionalParticipation())
                    .join("pc2", By.fieldNames("user2", "country2").withOptionalParticipation())
                    .join("pc3", By.fieldNames("user3", "country3").withOptionalParticipation())
                    .crossProductJoin());
    assertEquals(expectedSchema, joined.getSchema());

    List<Row> expectedJoinedRows =
        JoinTestUtils.innerJoin(
            pc1Rows,
            pc2Rows,
            pc3Rows,
            new String[] {"user", "country"},
            new String[] {"user2", "country2"},
            new String[] {"user3", "country3"},
            expectedSchema);
    // Manually add the outer-join rows to the list of expected results.
    expectedJoinedRows.add(
        Row.withSchema(expectedSchema)
            .addValues(Row.withSchema(CG_SCHEMA_1).addValues("user3", 7, "ar").build(), null, null)
            .build());
    expectedJoinedRows.add(
        Row.withSchema(expectedSchema)
            .addValues(null, Row.withSchema(CG_SCHEMA_2).addValues("user2", 16, "es").build(), null)
            .build());
    expectedJoinedRows.add(
        Row.withSchema(expectedSchema)
            .addValues(
                null, null, Row.withSchema(CG_SCHEMA_3).addValues("user27", 24, "se").build())
            .build());

    PAssert.that(joined).containsInAnyOrder(expectedJoinedRows);
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testPartialOuterJoin() {
    List<Row> pc1Rows =
        Lists.newArrayList(
            Row.withSchema(CG_SCHEMA_1).addValues("user1", 1, "us").build(),
            Row.withSchema(CG_SCHEMA_1).addValues("user1", 2, "us").build(),
            Row.withSchema(CG_SCHEMA_1).addValues("user1", 3, "il").build(),
            Row.withSchema(CG_SCHEMA_1).addValues("user1", 4, "il").build(),
            Row.withSchema(CG_SCHEMA_1).addValues("user2", 5, "fr").build(),
            Row.withSchema(CG_SCHEMA_1).addValues("user2", 6, "fr").build(),
            Row.withSchema(CG_SCHEMA_1).addValues("user2", 7, "ar").build(),
            Row.withSchema(CG_SCHEMA_1).addValues("user2", 8, "ar").build());

    List<Row> pc2Rows =
        Lists.newArrayList(
            Row.withSchema(CG_SCHEMA_2).addValues("user1", 9, "us").build(),
            Row.withSchema(CG_SCHEMA_2).addValues("user1", 10, "us").build(),
            Row.withSchema(CG_SCHEMA_2).addValues("user1", 11, "il").build(),
            Row.withSchema(CG_SCHEMA_2).addValues("user1", 12, "il").build(),
            Row.withSchema(CG_SCHEMA_2).addValues("user2", 13, "fr").build(),
            Row.withSchema(CG_SCHEMA_2).addValues("user2", 14, "fr").build(),
            Row.withSchema(CG_SCHEMA_2).addValues("user3", 7, "ar").build());

    List<Row> pc3Rows =
        Lists.newArrayList(
            Row.withSchema(CG_SCHEMA_3).addValues("user1", 17, "us").build(),
            Row.withSchema(CG_SCHEMA_3).addValues("user1", 18, "us").build(),
            Row.withSchema(CG_SCHEMA_3).addValues("user1", 19, "il").build(),
            Row.withSchema(CG_SCHEMA_3).addValues("user1", 20, "il").build(),
            Row.withSchema(CG_SCHEMA_3).addValues("user2", 21, "fr").build(),
            Row.withSchema(CG_SCHEMA_3).addValues("user2", 22, "fr").build(),
            Row.withSchema(CG_SCHEMA_3).addValues("user2", 23, "ar").build(),
            Row.withSchema(CG_SCHEMA_3).addValues("user2", 24, "ar").build(),
            Row.withSchema(CG_SCHEMA_3).addValues("user3", 25, "ar").build());

    PCollection<Row> pc1 = pipeline.apply("Create1", Create.of(pc1Rows)).setRowSchema(CG_SCHEMA_1);
    PCollection<Row> pc2 = pipeline.apply("Create2", Create.of(pc2Rows)).setRowSchema(CG_SCHEMA_2);
    PCollection<Row> pc3 = pipeline.apply("Create3", Create.of(pc3Rows)).setRowSchema(CG_SCHEMA_3);

    // Partial outer join. Missing entries in the "pc2" PCollection will be filled in with nulls,
    // but not others.
    Schema expectedSchema =
        Schema.builder()
            .addField("pc1", FieldType.row(CG_SCHEMA_1))
            .addNullableField("pc2", FieldType.row(CG_SCHEMA_2))
            .addField("pc3", FieldType.row(CG_SCHEMA_3))
            .build();

    PCollection<Row> joined =
        PCollectionTuple.of("pc1", pc1, "pc2", pc2, "pc3", pc3)
            .apply(
                "CoGroup",
                CoGroup.join("pc1", By.fieldNames("user", "country"))
                    .join("pc2", By.fieldNames("user2", "country2").withOptionalParticipation())
                    .join("pc3", By.fieldNames("user3", "country3"))
                    .crossProductJoin());
    assertEquals(expectedSchema, joined.getSchema());

    List<Row> expectedJoinedRows =
        JoinTestUtils.innerJoin(
            pc1Rows,
            pc2Rows,
            pc3Rows,
            new String[] {"user", "country"},
            new String[] {"user2", "country2"},
            new String[] {"user3", "country3"},
            expectedSchema);

    // Manually add the outer-join rows to the list of expected results. Missing results from the
    // middle (pc2) PCollection are filled in with nulls. Missing events from other PCollections
    // are not. Events with key ("user2", "ar) show up in pc1 and pc3 but not in pc2, so we expect
    // the outer join to still produce those rows, with nulls for pc2. Events with key
    // ("user3", "ar) however show up in in p2 and pc3, but not in pc1; since pc1 is marked for
    // full participation (no outer join), these events should not be included in the join.
    expectedJoinedRows.add(
        Row.withSchema(expectedSchema)
            .addValues(
                Row.withSchema(CG_SCHEMA_1).addValues("user2", 7, "ar").build(),
                null,
                Row.withSchema(CG_SCHEMA_3).addValues("user2", 23, "ar").build())
            .build());
    expectedJoinedRows.add(
        Row.withSchema(expectedSchema)
            .addValues(
                Row.withSchema(CG_SCHEMA_1).addValues("user2", 7, "ar").build(),
                null,
                Row.withSchema(CG_SCHEMA_3).addValues("user2", 24, "ar").build())
            .build());
    expectedJoinedRows.add(
        Row.withSchema(expectedSchema)
            .addValues(
                Row.withSchema(CG_SCHEMA_1).addValues("user2", 8, "ar").build(),
                null,
                Row.withSchema(CG_SCHEMA_3).addValues("user2", 23, "ar").build())
            .build());
    expectedJoinedRows.add(
        Row.withSchema(expectedSchema)
            .addValues(
                Row.withSchema(CG_SCHEMA_1).addValues("user2", 8, "ar").build(),
                null,
                Row.withSchema(CG_SCHEMA_3).addValues("user2", 24, "ar").build())
            .build());

    PAssert.that(joined).containsInAnyOrder(expectedJoinedRows);
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testUnmatchedTags() {
    PCollection<Row> pc1 = pipeline.apply("Create1", Create.empty(CG_SCHEMA_1));
    PCollection<Row> pc2 = pipeline.apply("Create2", Create.empty(CG_SCHEMA_2));

    thrown.expect(IllegalArgumentException.class);

    PCollectionTuple.of("pc1", pc1, "pc2", pc2)
        .apply(
            CoGroup.join("pc1", By.fieldNames("user"))
                .join("pc3", By.fieldNames("user3"))
                .crossProductJoin());
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
