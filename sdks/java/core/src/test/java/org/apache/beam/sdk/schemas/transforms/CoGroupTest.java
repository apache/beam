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
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.Assert.assertThat;

import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.transforms.CoGroup.By;
import org.apache.beam.sdk.schemas.utils.SchemaTestUtils.RowFieldMatcherIterableFieldAnyOrder;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.UsesSchema;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
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

  @DefaultSchema(JavaFieldSchema.class)
  public static class CgPojo {
    public String user;
    public int count;
    public String country;

    public CgPojo() {}

    public CgPojo(String user, int count, String country) {
      this.user = user;
      this.count = count;
      this.country = country;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      CgPojo cgPojo = (CgPojo) o;
      return count == cgPojo.count
          && Objects.equals(user, cgPojo.user)
          && Objects.equals(country, cgPojo.country);
    }

    @Override
    public int hashCode() {
      return Objects.hash(user, count, country);
    }
  }

  private static final Schema SIMPLE_CG_KEY_SCHEMA =
      Schema.builder().addStringField("user").addStringField("country").build();

  @Test
  @Category(NeedsRunner.class)
  public void testCoGroupByFieldNames() {
    // Input
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

    // Output
    Schema expectedSchema =
        Schema.builder()
            .addRowField("key", SIMPLE_CG_KEY_SCHEMA)
            .addIterableField("pc1", FieldType.row(CG_SCHEMA_1))
            .addIterableField("pc2", FieldType.row(CG_SCHEMA_1))
            .addIterableField("pc3", FieldType.row(CG_SCHEMA_1))
            .build();

    Row key1Joined =
        Row.withSchema(expectedSchema)
            .addValue(Row.withSchema(SIMPLE_CG_KEY_SCHEMA).addValues("user1", "us").build())
            .addIterable(
                Lists.newArrayList(
                    Row.withSchema(CG_SCHEMA_1).addValues("user1", 1, "us").build(),
                    Row.withSchema(CG_SCHEMA_1).addValues("user1", 2, "us").build()))
            .addIterable(
                Lists.newArrayList(
                    Row.withSchema(CG_SCHEMA_1).addValues("user1", 9, "us").build(),
                    Row.withSchema(CG_SCHEMA_1).addValues("user1", 10, "us").build()))
            .addIterable(
                Lists.newArrayList(
                    Row.withSchema(CG_SCHEMA_1).addValues("user1", 17, "us").build(),
                    Row.withSchema(CG_SCHEMA_1).addValues("user1", 18, "us").build()))
            .build();

    Row key2Joined =
        Row.withSchema(expectedSchema)
            .addValue(Row.withSchema(SIMPLE_CG_KEY_SCHEMA).addValues("user1", "il").build())
            .addIterable(
                Lists.newArrayList(
                    Row.withSchema(CG_SCHEMA_1).addValues("user1", 3, "il").build(),
                    Row.withSchema(CG_SCHEMA_1).addValues("user1", 4, "il").build()))
            .addIterable(
                Lists.newArrayList(
                    Row.withSchema(CG_SCHEMA_1).addValues("user1", 11, "il").build(),
                    Row.withSchema(CG_SCHEMA_1).addValues("user1", 12, "il").build()))
            .addIterable(
                Lists.newArrayList(
                    Row.withSchema(CG_SCHEMA_1).addValues("user1", 19, "il").build(),
                    Row.withSchema(CG_SCHEMA_1).addValues("user1", 20, "il").build()))
            .build();

    Row key3Joined =
        Row.withSchema(expectedSchema)
            .addValue(Row.withSchema(SIMPLE_CG_KEY_SCHEMA).addValues("user2", "fr").build())
            .addIterable(
                Lists.newArrayList(
                    Row.withSchema(CG_SCHEMA_1).addValues("user2", 5, "fr").build(),
                    Row.withSchema(CG_SCHEMA_1).addValues("user2", 6, "fr").build()))
            .addIterable(
                Lists.newArrayList(
                    Row.withSchema(CG_SCHEMA_1).addValues("user2", 13, "fr").build(),
                    Row.withSchema(CG_SCHEMA_1).addValues("user2", 14, "fr").build()))
            .addIterable(
                Lists.newArrayList(
                    Row.withSchema(CG_SCHEMA_1).addValues("user2", 21, "fr").build(),
                    Row.withSchema(CG_SCHEMA_1).addValues("user2", 22, "fr").build()))
            .build();

    Row key4Joined =
        Row.withSchema(expectedSchema)
            .addValue(Row.withSchema(SIMPLE_CG_KEY_SCHEMA).addValues("user2", "ar").build())
            .addIterable(
                Lists.newArrayList(
                    Row.withSchema(CG_SCHEMA_1).addValues("user2", 7, "ar").build(),
                    Row.withSchema(CG_SCHEMA_1).addValues("user2", 8, "ar").build()))
            .addIterable(
                Lists.newArrayList(
                    Row.withSchema(CG_SCHEMA_1).addValues("user2", 15, "ar").build(),
                    Row.withSchema(CG_SCHEMA_1).addValues("user2", 16, "ar").build()))
            .addIterable(
                Lists.newArrayList(
                    Row.withSchema(CG_SCHEMA_1).addValues("user2", 23, "ar").build(),
                    Row.withSchema(CG_SCHEMA_1).addValues("user2", 24, "ar").build()))
            .build();

    PCollection<Row> joined =
        PCollectionTuple.of("pc1", pc1, "pc2", pc2, "pc3", pc3)
            .apply("CoGroup", CoGroup.join(By.fieldNames("user", "country")));
    List<Row> expected = ImmutableList.of(key1Joined, key2Joined, key3Joined, key4Joined);
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
    // Inputs.
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

    // Expected outputs
    Schema expectedSchema =
        Schema.builder()
            .addRowField("key", SIMPLE_CG_KEY_SCHEMA)
            .addIterableField("pc1", FieldType.row(CG_SCHEMA_1))
            .addIterableField("pc2", FieldType.row(CG_SCHEMA_2))
            .addIterableField("pc3", FieldType.row(CG_SCHEMA_3))
            .build();
    Row key1Joined =
        Row.withSchema(expectedSchema)
            .addValue(Row.withSchema(SIMPLE_CG_KEY_SCHEMA).addValues("user1", "us").build())
            .addIterable(
                Lists.newArrayList(
                    Row.withSchema(CG_SCHEMA_1).addValues("user1", 1, "us").build(),
                    Row.withSchema(CG_SCHEMA_1).addValues("user1", 2, "us").build()))
            .addIterable(
                Lists.newArrayList(
                    Row.withSchema(CG_SCHEMA_2).addValues("user1", 9, "us").build(),
                    Row.withSchema(CG_SCHEMA_2).addValues("user1", 10, "us").build()))
            .addIterable(
                Lists.newArrayList(
                    Row.withSchema(CG_SCHEMA_3).addValues("user1", 17, "us").build(),
                    Row.withSchema(CG_SCHEMA_3).addValues("user1", 18, "us").build()))
            .build();

    Row key2Joined =
        Row.withSchema(expectedSchema)
            .addValue(Row.withSchema(SIMPLE_CG_KEY_SCHEMA).addValues("user1", "il").build())
            .addIterable(
                Lists.newArrayList(
                    Row.withSchema(CG_SCHEMA_1).addValues("user1", 3, "il").build(),
                    Row.withSchema(CG_SCHEMA_1).addValues("user1", 4, "il").build()))
            .addIterable(
                Lists.newArrayList(
                    Row.withSchema(CG_SCHEMA_2).addValues("user1", 11, "il").build(),
                    Row.withSchema(CG_SCHEMA_2).addValues("user1", 12, "il").build()))
            .addIterable(
                Lists.newArrayList(
                    Row.withSchema(CG_SCHEMA_3).addValues("user1", 19, "il").build(),
                    Row.withSchema(CG_SCHEMA_3).addValues("user1", 20, "il").build()))
            .build();

    Row key3Joined =
        Row.withSchema(expectedSchema)
            .addValue(Row.withSchema(SIMPLE_CG_KEY_SCHEMA).addValues("user2", "fr").build())
            .addIterable(
                Lists.newArrayList(
                    Row.withSchema(CG_SCHEMA_1).addValues("user2", 5, "fr").build(),
                    Row.withSchema(CG_SCHEMA_1).addValues("user2", 6, "fr").build()))
            .addIterable(
                Lists.newArrayList(
                    Row.withSchema(CG_SCHEMA_2).addValues("user2", 13, "fr").build(),
                    Row.withSchema(CG_SCHEMA_2).addValues("user2", 14, "fr").build()))
            .addIterable(
                Lists.newArrayList(
                    Row.withSchema(CG_SCHEMA_3).addValues("user2", 21, "fr").build(),
                    Row.withSchema(CG_SCHEMA_3).addValues("user2", 22, "fr").build()))
            .build();

    Row key4Joined =
        Row.withSchema(expectedSchema)
            .addValue(Row.withSchema(SIMPLE_CG_KEY_SCHEMA).addValues("user2", "ar").build())
            .addIterable(
                Lists.newArrayList(
                    Row.withSchema(CG_SCHEMA_1).addValues("user2", 7, "ar").build(),
                    Row.withSchema(CG_SCHEMA_1).addValues("user2", 8, "ar").build()))
            .addIterable(
                Lists.newArrayList(
                    Row.withSchema(CG_SCHEMA_2).addValues("user2", 15, "ar").build(),
                    Row.withSchema(CG_SCHEMA_2).addValues("user2", 16, "ar").build()))
            .addIterable(
                Lists.newArrayList(
                    Row.withSchema(CG_SCHEMA_3).addValues("user2", 23, "ar").build(),
                    Row.withSchema(CG_SCHEMA_3).addValues("user2", 24, "ar").build()))
            .build();

    PCollection<Row> joined1 =
        PCollectionTuple.of("pc1", pc1, "pc2", pc2, "pc3", pc3)
            .apply(
                "CoGroup1",
                CoGroup.join("pc1", By.fieldNames("user", "country"))
                    .join("pc2", By.fieldNames("user2", "country2"))
                    .join("pc3", By.fieldNames("user3", "country3")));
    PCollection<Row> joined2 =
        PCollectionTuple.of("pc1", pc1, "pc2", pc2, "pc3", pc3)
            .apply(
                "CoGroup2",
                CoGroup.join("pc1", By.fieldNames("user", "country"))
                    .join("pc2", By.fieldNames("user2", "country2").withSideInput())
                    .join("pc3", By.fieldNames("user3", "country3")));
    PCollection<Row> joined3 =
        PCollectionTuple.of("pc1", pc1, "pc2", pc2, "pc3", pc3)
            .apply(
                "CoGroup3",
                CoGroup.join("pc1", By.fieldNames("user", "country"))
                    .join("pc2", By.fieldNames("user2", "country2").withSideInput())
                    .join("pc3", By.fieldNames("user3", "country3").withSideInput()));

    List<Row> expected = ImmutableList.of(key1Joined, key2Joined, key3Joined, key4Joined);

    PAssert.that(joined1).satisfies(actual -> containsJoinedFields(expected, actual));
    PAssert.that(joined2).satisfies(actual -> containsJoinedFields(expected, actual));
    PAssert.that(joined3).satisfies(actual -> containsJoinedFields(expected, actual));

    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testNoMainInput() {
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
        pipeline
            .apply(
                "Create3",
                Create.of(Row.withSchema(CG_SCHEMA_3).addValues("user1", 17, "us").build()))
            .setRowSchema(CG_SCHEMA_3);

    thrown.expect(IllegalArgumentException.class);
    PCollection<Row> joined =
        PCollectionTuple.of("pc1", pc1, "pc2", pc2, "pc3", pc3)
            .apply(
                "CoGroup1",
                CoGroup.join("pc1", By.fieldNames("user", "country").withSideInput())
                    .join("pc2", By.fieldNames("user2", "country2").withSideInput())
                    .join("pc3", By.fieldNames("user3", "country3").withSideInput()));
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testIllegalOuterJoinWithSideInput() {
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
        pipeline
            .apply(
                "Create3",
                Create.of(Row.withSchema(CG_SCHEMA_3).addValues("user1", 17, "us").build()))
            .setRowSchema(CG_SCHEMA_3);

    thrown.expect(IllegalArgumentException.class);
    PCollection<Row> joined =
        PCollectionTuple.of("pc1", pc1, "pc2", pc2, "pc3", pc3)
            .apply(
                "CoGroup1",
                CoGroup.join("pc1", By.fieldNames("user", "country").withOptionalParticipation())
                    .join("pc2", By.fieldNames("user2", "country2").withOptionalParticipation())
                    .join("pc3", By.fieldNames("user3", "country3").withSideInput())
                    .crossProductJoin());
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
    PCollection<Row> joined =
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

    thrown.expect(IllegalArgumentException.class);
    PCollection<Row> joined =
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

  @Test
  @Category(NeedsRunner.class)
  public void testPojo() {
    List<CgPojo> pc1Rows =
        Lists.newArrayList(
            new CgPojo("user1", 1, "us"),
            new CgPojo("user1", 2, "us"),
            new CgPojo("user1", 3, "il"),
            new CgPojo("user1", 4, "il"));

    List<CgPojo> pc2Rows =
        Lists.newArrayList(
            new CgPojo("user1", 3, "us"),
            new CgPojo("user1", 4, "us"),
            new CgPojo("user1", 5, "il"),
            new CgPojo("user1", 6, "il"));

    PCollection<CgPojo> pc1 = pipeline.apply("Create1", Create.of(pc1Rows));
    PCollection<CgPojo> pc2 = pipeline.apply("Create2", Create.of(pc2Rows));

    PCollection<Row> joined =
        PCollectionTuple.of("pc1", pc1)
            .and("pc2", pc2)
            .apply(
                CoGroup.join("pc1", By.fieldNames("user", "country"))
                    .join("pc2", By.fieldNames("user", "country")));

    Schema expectedSchema =
        Schema.builder()
            .addRowField("key", SIMPLE_CG_KEY_SCHEMA)
            .addIterableField("pc1", FieldType.row(CG_SCHEMA_1))
            .addIterableField("pc2", FieldType.row(CG_SCHEMA_1))
            .build();

    List<Row> expected =
        Lists.newArrayList(
            Row.withSchema(expectedSchema)
                .addValue(Row.withSchema(SIMPLE_CG_KEY_SCHEMA).addValues("user1", "us").build())
                .addIterable(
                    Lists.newArrayList(
                        Row.withSchema(CG_SCHEMA_1).addValues("user1", 1, "us").build(),
                        Row.withSchema(CG_SCHEMA_1).addValues("user1", 2, "us").build()))
                .addIterable(
                    Lists.newArrayList(
                        Row.withSchema(CG_SCHEMA_1).addValues("user1", 3, "us").build(),
                        Row.withSchema(CG_SCHEMA_1).addValues("user1", 4, "us").build()))
                .build(),
            Row.withSchema(expectedSchema)
                .addValue(Row.withSchema(SIMPLE_CG_KEY_SCHEMA).addValues("user1", "il").build())
                .addIterable(
                    Lists.newArrayList(
                        Row.withSchema(CG_SCHEMA_1).addValues("user1", 3, "il").build(),
                        Row.withSchema(CG_SCHEMA_1).addValues("user1", 4, "il").build()))
                .addIterable(
                    Lists.newArrayList(
                        Row.withSchema(CG_SCHEMA_1).addValues("user1", 5, "il").build(),
                        Row.withSchema(CG_SCHEMA_1).addValues("user1", 6, "il").build()))
                .build());

    assertEquals(expectedSchema, joined.getSchema());
    PAssert.that(joined).satisfies(actual -> containsJoinedFields(expected, actual));

    pipeline.run();
  }

  private static Void containsJoinedFields(List<Row> expected, Iterable<Row> actual) {
    List<Matcher<? super Row>> matchers = Lists.newArrayList();
    for (Row row : expected) {
      List<Matcher> fieldMatchers = Lists.newArrayList();
      Schema schema = row.getSchema();
      fieldMatchers.add(
          new RowFieldMatcherIterableFieldAnyOrder(row.getSchema(), 0, row.getRow(0)));
      for (int i = 1; i < schema.getFieldCount(); ++i) {
        assertEquals(TypeName.ITERABLE, schema.getField(i).getType().getTypeName());
        fieldMatchers.add(
            new RowFieldMatcherIterableFieldAnyOrder(row.getSchema(), i, row.getIterable(i)));
      }
      matchers.add(allOf(fieldMatchers.toArray(new Matcher[0])));
    }
    assertThat(actual, containsInAnyOrder(matchers.toArray(new Matcher[0])));
    return null;
  }
}
