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
import static org.apache.beam.sdk.schemas.transforms.JoinTestUtils.innerJoin;

import java.util.List;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.Join.FieldsEqual;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.UsesSchema;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link org.apache.beam.sdk.schemas.transforms.Join}. */
@RunWith(JUnit4.class)
@Category(UsesSchema.class)
public class JoinTest {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  private static final Schema CG_SCHEMA_1 =
      Schema.builder()
          .addStringField("user")
          .addInt32Field("count")
          .addStringField("country")
          .build();
  private static final Schema CG_SCHEMA_2 =
      Schema.builder()
          .addStringField("user2")
          .addInt32Field("count2")
          .addStringField("country2")
          .build();

  @Test
  @Category(NeedsRunner.class)
  public void testInnerJoinSameKeys() {
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
            Row.withSchema(CG_SCHEMA_1).addValues("user3", 8, "ar").build());
    List<Row> pc2Rows =
        Lists.newArrayList(
            Row.withSchema(CG_SCHEMA_1).addValues("user1", 9, "us").build(),
            Row.withSchema(CG_SCHEMA_1).addValues("user1", 10, "us").build(),
            Row.withSchema(CG_SCHEMA_1).addValues("user1", 11, "il").build(),
            Row.withSchema(CG_SCHEMA_1).addValues("user1", 12, "il").build(),
            Row.withSchema(CG_SCHEMA_1).addValues("user2", 13, "fr").build(),
            Row.withSchema(CG_SCHEMA_1).addValues("user2", 14, "fr").build(),
            Row.withSchema(CG_SCHEMA_1).addValues("user2", 15, "ar").build(),
            Row.withSchema(CG_SCHEMA_1).addValues("user2", 16, "ar").build(),
            Row.withSchema(CG_SCHEMA_1).addValues("user4", 8, "ar").build());

    PCollection<Row> pc1 = pipeline.apply("Create1", Create.of(pc1Rows)).setRowSchema(CG_SCHEMA_1);
    PCollection<Row> pc2 = pipeline.apply("Create2", Create.of(pc2Rows)).setRowSchema(CG_SCHEMA_1);

    Schema expectedSchema =
        Schema.builder()
            .addRowField(Join.LHS_TAG, CG_SCHEMA_1)
            .addRowField(Join.RHS_TAG, CG_SCHEMA_1)
            .build();

    PCollection<Row> joined1 =
        pc1.apply(
            "innerBroadcast", Join.<Row, Row>innerBroadcastJoin(pc2).using("user", "country"));
    PCollection<Row> joined2 =
        pc1.apply("inner", Join.<Row, Row>innerJoin(pc2).using("user", "country"));

    assertEquals(expectedSchema, joined1.getSchema());
    assertEquals(expectedSchema, joined2.getSchema());

    List<Row> expectedJoinedRows =
        innerJoin(
            pc1Rows,
            pc2Rows,
            new String[] {"user", "country"},
            new String[] {"user", "country"},
            expectedSchema);

    PAssert.that(joined1).containsInAnyOrder(expectedJoinedRows);
    PAssert.that(joined2).containsInAnyOrder(expectedJoinedRows);

    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testInnerJoinDifferentKeys() {
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
            Row.withSchema(CG_SCHEMA_1).addValues("user3", 8, "ar").build());
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
            Row.withSchema(CG_SCHEMA_2).addValues("user4", 8, "ar").build());

    PCollection<Row> pc1 = pipeline.apply("Create1", Create.of(pc1Rows)).setRowSchema(CG_SCHEMA_1);
    PCollection<Row> pc2 = pipeline.apply("Create2", Create.of(pc2Rows)).setRowSchema(CG_SCHEMA_2);

    Schema expectedSchema =
        Schema.builder()
            .addRowField(Join.LHS_TAG, CG_SCHEMA_1)
            .addRowField(Join.RHS_TAG, CG_SCHEMA_2)
            .build();

    PCollection<Row> joined1 =
        pc1.apply(
            "innerBroadcast",
            Join.<Row, Row>innerBroadcastJoin(pc2)
                .on(FieldsEqual.left("user", "country").right("user2", "country2")));
    PCollection<Row> joined2 =
        pc1.apply(
            "inner",
            Join.<Row, Row>innerJoin(pc2)
                .on(FieldsEqual.left("user", "country").right("user2", "country2")));

    assertEquals(expectedSchema, joined1.getSchema());
    assertEquals(expectedSchema, joined2.getSchema());

    List<Row> expectedJoinedRows =
        innerJoin(
            pc1Rows,
            pc2Rows,
            new String[] {"user", "country"},
            new String[] {"user2", "country2"},
            expectedSchema);

    PAssert.that(joined1).containsInAnyOrder(expectedJoinedRows);
    PAssert.that(joined2).containsInAnyOrder(expectedJoinedRows);
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testOuterJoinDifferentKeys() {
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
            Row.withSchema(CG_SCHEMA_1).addValues("user3", 8, "ar").build());
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
            Row.withSchema(CG_SCHEMA_2).addValues("user4", 8, "ar").build());

    PCollection<Row> pc1 = pipeline.apply("Create1", Create.of(pc1Rows)).setRowSchema(CG_SCHEMA_1);
    PCollection<Row> pc2 = pipeline.apply("Create2", Create.of(pc2Rows)).setRowSchema(CG_SCHEMA_2);

    Schema expectedSchema =
        Schema.builder()
            .addNullableField(Join.LHS_TAG, Schema.FieldType.row(CG_SCHEMA_1))
            .addNullableField(Join.RHS_TAG, Schema.FieldType.row(CG_SCHEMA_2))
            .build();

    PCollection<Row> joined =
        pc1.apply(
            "outer",
            Join.<Row, Row>fullOuterJoin(pc2)
                .on(FieldsEqual.left("user", "country").right("user2", "country2")));

    assertEquals(expectedSchema, joined.getSchema());

    List<Row> expectedJoinedRows =
        innerJoin(
            pc1Rows,
            pc2Rows,
            new String[] {"user", "country"},
            new String[] {"user2", "country2"},
            expectedSchema);
    expectedJoinedRows.add(
        Row.withSchema(expectedSchema)
            .addValues(Row.withSchema(CG_SCHEMA_1).addValues("user3", 8, "ar").build(), null)
            .build());
    expectedJoinedRows.add(
        Row.withSchema(expectedSchema)
            .addValues(null, Row.withSchema(CG_SCHEMA_2).addValues("user4", 8, "ar").build())
            .build());

    PAssert.that(joined).containsInAnyOrder(expectedJoinedRows);
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testOuterJoinSameKeys() {
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
            Row.withSchema(CG_SCHEMA_1).addValues("user3", 8, "ar").build());
    List<Row> pc2Rows =
        Lists.newArrayList(
            Row.withSchema(CG_SCHEMA_1).addValues("user1", 9, "us").build(),
            Row.withSchema(CG_SCHEMA_1).addValues("user1", 10, "us").build(),
            Row.withSchema(CG_SCHEMA_1).addValues("user1", 11, "il").build(),
            Row.withSchema(CG_SCHEMA_1).addValues("user1", 12, "il").build(),
            Row.withSchema(CG_SCHEMA_1).addValues("user2", 13, "fr").build(),
            Row.withSchema(CG_SCHEMA_1).addValues("user2", 14, "fr").build(),
            Row.withSchema(CG_SCHEMA_1).addValues("user2", 15, "ar").build(),
            Row.withSchema(CG_SCHEMA_1).addValues("user2", 16, "ar").build(),
            Row.withSchema(CG_SCHEMA_1).addValues("user4", 8, "ar").build());

    PCollection<Row> pc1 = pipeline.apply("Create1", Create.of(pc1Rows)).setRowSchema(CG_SCHEMA_1);
    PCollection<Row> pc2 = pipeline.apply("Create2", Create.of(pc2Rows)).setRowSchema(CG_SCHEMA_1);

    Schema expectedSchema =
        Schema.builder()
            .addNullableField(Join.LHS_TAG, Schema.FieldType.row(CG_SCHEMA_1))
            .addNullableField(Join.RHS_TAG, Schema.FieldType.row(CG_SCHEMA_1))
            .build();

    PCollection<Row> joined =
        pc1.apply("outer", Join.<Row, Row>fullOuterJoin(pc2).using("user", "country"));

    assertEquals(expectedSchema, joined.getSchema());

    List<Row> expectedJoinedRows =
        innerJoin(
            pc1Rows,
            pc2Rows,
            new String[] {"user", "country"},
            new String[] {"user", "country"},
            expectedSchema);
    expectedJoinedRows.add(
        Row.withSchema(expectedSchema)
            .addValues(Row.withSchema(CG_SCHEMA_1).addValues("user3", 8, "ar").build(), null)
            .build());
    expectedJoinedRows.add(
        Row.withSchema(expectedSchema)
            .addValues(null, Row.withSchema(CG_SCHEMA_1).addValues("user4", 8, "ar").build())
            .build());

    PAssert.that(joined).containsInAnyOrder(expectedJoinedRows);
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testLeftOuterJoinSameKeys() {
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
            Row.withSchema(CG_SCHEMA_1).addValues("user3", 8, "ar").build());
    List<Row> pc2Rows =
        Lists.newArrayList(
            Row.withSchema(CG_SCHEMA_1).addValues("user1", 9, "us").build(),
            Row.withSchema(CG_SCHEMA_1).addValues("user1", 10, "us").build(),
            Row.withSchema(CG_SCHEMA_1).addValues("user1", 11, "il").build(),
            Row.withSchema(CG_SCHEMA_1).addValues("user1", 12, "il").build(),
            Row.withSchema(CG_SCHEMA_1).addValues("user2", 13, "fr").build(),
            Row.withSchema(CG_SCHEMA_1).addValues("user2", 14, "fr").build(),
            Row.withSchema(CG_SCHEMA_1).addValues("user2", 15, "ar").build(),
            Row.withSchema(CG_SCHEMA_1).addValues("user2", 16, "ar").build(),
            Row.withSchema(CG_SCHEMA_1).addValues("user4", 8, "ar").build());

    PCollection<Row> pc1 = pipeline.apply("Create1", Create.of(pc1Rows)).setRowSchema(CG_SCHEMA_1);
    PCollection<Row> pc2 = pipeline.apply("Create2", Create.of(pc2Rows)).setRowSchema(CG_SCHEMA_1);

    Schema expectedSchema =
        Schema.builder()
            .addField(Join.LHS_TAG, Schema.FieldType.row(CG_SCHEMA_1))
            .addNullableField(Join.RHS_TAG, Schema.FieldType.row(CG_SCHEMA_1))
            .build();

    PCollection<Row> joined1 =
        pc1.apply(
            "leftBroadcast", Join.<Row, Row>leftOuterBroadcastJoin(pc2).using("user", "country"));
    PCollection<Row> joined2 =
        pc1.apply("left", Join.<Row, Row>leftOuterJoin(pc2).using("user", "country"));

    assertEquals(expectedSchema, joined1.getSchema());
    assertEquals(expectedSchema, joined2.getSchema());

    List<Row> expectedJoinedRows =
        innerJoin(
            pc1Rows,
            pc2Rows,
            new String[] {"user", "country"},
            new String[] {"user", "country"},
            expectedSchema);
    expectedJoinedRows.add(
        Row.withSchema(expectedSchema)
            .addValues(Row.withSchema(CG_SCHEMA_1).addValues("user3", 8, "ar").build(), null)
            .build());

    PAssert.that(joined1).containsInAnyOrder(expectedJoinedRows);
    PAssert.that(joined2).containsInAnyOrder(expectedJoinedRows);
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testRightOuterJoinSameKeys() {
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
            Row.withSchema(CG_SCHEMA_1).addValues("user3", 8, "ar").build());
    List<Row> pc2Rows =
        Lists.newArrayList(
            Row.withSchema(CG_SCHEMA_1).addValues("user1", 9, "us").build(),
            Row.withSchema(CG_SCHEMA_1).addValues("user1", 10, "us").build(),
            Row.withSchema(CG_SCHEMA_1).addValues("user1", 11, "il").build(),
            Row.withSchema(CG_SCHEMA_1).addValues("user1", 12, "il").build(),
            Row.withSchema(CG_SCHEMA_1).addValues("user2", 13, "fr").build(),
            Row.withSchema(CG_SCHEMA_1).addValues("user2", 14, "fr").build(),
            Row.withSchema(CG_SCHEMA_1).addValues("user2", 15, "ar").build(),
            Row.withSchema(CG_SCHEMA_1).addValues("user2", 16, "ar").build(),
            Row.withSchema(CG_SCHEMA_1).addValues("user4", 8, "ar").build());

    PCollection<Row> pc1 = pipeline.apply("Create1", Create.of(pc1Rows)).setRowSchema(CG_SCHEMA_1);
    PCollection<Row> pc2 = pipeline.apply("Create2", Create.of(pc2Rows)).setRowSchema(CG_SCHEMA_1);

    Schema expectedSchema =
        Schema.builder()
            .addNullableField(Join.LHS_TAG, Schema.FieldType.row(CG_SCHEMA_1))
            .addField(Join.RHS_TAG, Schema.FieldType.row(CG_SCHEMA_1))
            .build();

    PCollection<Row> joined =
        pc1.apply("right", Join.<Row, Row>rightOuterJoin(pc2).using("user", "country"));

    assertEquals(expectedSchema, joined.getSchema());

    List<Row> expectedJoinedRows =
        innerJoin(
            pc1Rows,
            pc2Rows,
            new String[] {"user", "country"},
            new String[] {"user", "country"},
            expectedSchema);
    expectedJoinedRows.add(
        Row.withSchema(expectedSchema)
            .addValues(null, Row.withSchema(CG_SCHEMA_1).addValues("user4", 8, "ar").build())
            .build());

    PAssert.that(joined).containsInAnyOrder(expectedJoinedRows);
    pipeline.run();
  }
}
