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

import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.UsesSchema;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableMap;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test for {@link Select}. */
@RunWith(JUnit4.class)
@Category(UsesSchema.class)
public class SelectTest {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  @Rule public transient ExpectedException thrown = ExpectedException.none();

  /** flat POJO to selection from. */
  @DefaultSchema(JavaFieldSchema.class)
  static class POJO1 {
    String field1 = "field1";
    Integer field2 = 42;
    Double field3 = 3.14;

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      POJO1 pojo1 = (POJO1) o;
      return Objects.equals(field1, pojo1.field1)
          && Objects.equals(field2, pojo1.field2)
          && Objects.equals(field3, pojo1.field3);
    }

    @Override
    public int hashCode() {
      return Objects.hash(field1, field2, field3);
    }

    @Override
    public String toString() {
      return "POJO1{"
          + "field1='"
          + field1
          + '\''
          + ", field2="
          + field2
          + ", field3="
          + field3
          + '}';
    }
  };

  /** A pojo matching the schema resulting from selection field1, field3. */
  @DefaultSchema(JavaFieldSchema.class)
  static class POJO1Selected {
    String field1 = "field1";
    Double field3 = 3.14;

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      POJO1Selected that = (POJO1Selected) o;
      return Objects.equals(field1, that.field1) && Objects.equals(field3, that.field3);
    }

    @Override
    public int hashCode() {
      return Objects.hash(field1, field3);
    }
  }

  /** A nested POJO. */
  @DefaultSchema(JavaFieldSchema.class)
  static class POJO2 {
    String field1 = "field1";
    POJO1 field2 = new POJO1();

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof POJO2)) {
        return false;
      }
      POJO2 pojo2 = (POJO2) o;
      return Objects.equals(field1, pojo2.field1) && Objects.equals(field2, pojo2.field2);
    }

    @Override
    public int hashCode() {
      return Objects.hash(field1, field2);
    }
  }

  @Test
  @Category(NeedsRunner.class)
  public void testSelectMissingFieldName() {
    thrown.expect(IllegalArgumentException.class);
    pipeline.apply(Create.of(new POJO1())).apply(Select.fieldNames("missing"));
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testSelectMissingFieldIndex() {
    thrown.expect(IllegalArgumentException.class);
    pipeline.apply(Create.of(new POJO1())).apply(Select.fieldIds(42));
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testSelectAll() {
    PCollection<POJO1> pojos =
        pipeline
            .apply(Create.of(new POJO1()))
            .apply(Select.fieldNames("*"))
            .apply(Convert.to(POJO1.class));
    PAssert.that(pojos).containsInAnyOrder(new POJO1());
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testSimpleSelect() {
    PCollection<POJO1Selected> pojos =
        pipeline
            .apply(Create.of(new POJO1()))
            .apply(Select.fieldNames("field1", "field3"))
            .apply(Convert.to(POJO1Selected.class));
    PAssert.that(pojos).containsInAnyOrder(new POJO1Selected());
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testSelectNestedAll() {
    PCollection<POJO1> pojos =
        pipeline
            .apply(Create.of(new POJO2()))
            .apply(Select.fieldNames("field2"))
            .apply(Convert.to(POJO1.class));
    PAssert.that(pojos).containsInAnyOrder(new POJO1());
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testSelectNestedAllWildcard() {
    PCollection<POJO1> pojos =
        pipeline
            .apply(Create.of(new POJO2()))
            .apply(Select.fieldNames("field2.*"))
            .apply(Convert.to(POJO1.class));
    PAssert.that(pojos).containsInAnyOrder(new POJO1());
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testSelectNestedPartial() {
    PCollection<POJO1Selected> pojos =
        pipeline
            .apply(Create.of(new POJO2()))
            .apply(Select.fieldNames("field2.field1", "field2.field3"))
            .apply(Convert.to(POJO1Selected.class));
    PAssert.that(pojos).containsInAnyOrder(new POJO1Selected());
    pipeline.run();
  }

  @DefaultSchema(JavaFieldSchema.class)
  static class PrimitiveArray {
    List<Double> field1 = ImmutableList.of(1.0, 2.1, 3.2);

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof PrimitiveArray)) {
        return false;
      }
      PrimitiveArray that = (PrimitiveArray) o;
      return Objects.equals(field1, that.field1);
    }

    @Override
    public int hashCode() {
      return Objects.hash(field1);
    }
  }

  @Test
  @Category(NeedsRunner.class)
  public void testSelectPrimitiveArray() {
    PCollection<PrimitiveArray> selected =
        pipeline
            .apply(Create.of(new PrimitiveArray()))
            .apply(Select.fieldNames("field1"))
            .apply(Convert.to(PrimitiveArray.class));
    PAssert.that(selected).containsInAnyOrder(new PrimitiveArray());
    pipeline.run();
  }

  @DefaultSchema(JavaFieldSchema.class)
  static class RowSingleArray {
    List<POJO1> field1 = ImmutableList.of(new POJO1(), new POJO1(), new POJO1());

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof RowSingleArray)) {
        return false;
      }
      RowSingleArray that = (RowSingleArray) o;
      return Objects.equals(field1, that.field1);
    }

    @Override
    public int hashCode() {
      return Objects.hash(field1);
    }
  }

  @DefaultSchema(JavaFieldSchema.class)
  static class PartialRowSingleArray {
    List<String> field1 = ImmutableList.of("field1", "field1", "field1");
    List<Double> field3 = ImmutableList.of(3.14, 3.14, 3.14);

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof PartialRowSingleArray)) {
        return false;
      }
      PartialRowSingleArray that = (PartialRowSingleArray) o;
      return Objects.equals(field1, that.field1) && Objects.equals(field3, that.field3);
    }

    @Override
    public int hashCode() {
      return Objects.hash(field1, field3);
    }
  }

  @Test
  @Category(NeedsRunner.class)
  public void testSelectRowArray() {
    PCollection<PartialRowSingleArray> selected =
        pipeline
            .apply(Create.of(new RowSingleArray()))
            .apply(Select.fieldNames("field1.field1", "field1.field3"))
            .apply(Convert.to(PartialRowSingleArray.class));
    PAssert.that(selected).containsInAnyOrder(new PartialRowSingleArray());
    pipeline.run();
  }

  @DefaultSchema(JavaFieldSchema.class)
  static class RowSingleMap {
    Map<String, POJO1> field1 =
        ImmutableMap.of(
            "key1", new POJO1(),
            "key2", new POJO1(),
            "key3", new POJO1());

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof RowSingleMap)) {
        return false;
      }
      RowSingleMap that = (RowSingleMap) o;
      return Objects.equals(field1, that.field1);
    }

    @Override
    public int hashCode() {
      return Objects.hash(field1);
    }
  }

  @DefaultSchema(JavaFieldSchema.class)
  static class PartialRowSingleMap {
    Map<String, String> field1 =
        ImmutableMap.of(
            "key1", "field1",
            "key2", "field1",
            "key3", "field1");
    Map<String, Double> field3 =
        ImmutableMap.of(
            "key1", 3.14,
            "key2", 3.14,
            "key3", 3.14);

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof PartialRowSingleMap)) {
        return false;
      }
      PartialRowSingleMap that = (PartialRowSingleMap) o;
      return Objects.equals(field1, that.field1) && Objects.equals(field3, that.field3);
    }

    @Override
    public int hashCode() {
      return Objects.hash(field1, field3);
    }
  }

  @Test
  @Category(NeedsRunner.class)
  public void testSelectRowMap() {
    PCollection<PartialRowSingleMap> selected =
        pipeline
            .apply(Create.of(new RowSingleMap()))
            .apply(Select.fieldNames("field1.field1", "field1.field3"))
            .apply(Convert.to(PartialRowSingleMap.class));
    PAssert.that(selected).containsInAnyOrder(new PartialRowSingleMap());
    pipeline.run();
  }

  @DefaultSchema(JavaFieldSchema.class)
  static class RowMultipleArray {
    private static final List<POJO1> POJO_LIST =
        ImmutableList.of(new POJO1(), new POJO1(), new POJO1());
    private static final List<List<POJO1>> POJO_LIST_LIST =
        ImmutableList.of(POJO_LIST, POJO_LIST, POJO_LIST);

    List<List<List<POJO1>>> field1 =
        ImmutableList.of(POJO_LIST_LIST, POJO_LIST_LIST, POJO_LIST_LIST);

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof RowMultipleArray)) {
        return false;
      }
      RowMultipleArray that = (RowMultipleArray) o;
      return Objects.equals(field1, that.field1);
    }

    @Override
    public int hashCode() {
      return Objects.hash(field1);
    }
  }

  @DefaultSchema(JavaFieldSchema.class)
  static class PartialRowMultipleArray {
    private static final List<POJO1Selected> POJO_LIST =
        ImmutableList.of(new POJO1Selected(), new POJO1Selected(), new POJO1Selected());
    private static final List<List<POJO1Selected>> POJO_LIST_LIST =
        ImmutableList.of(POJO_LIST, POJO_LIST, POJO_LIST);

    private static final List<String> STRING_LIST = ImmutableList.of("field1", "field1", "field1");
    private static final List<List<String>> STRING_LISTLIST =
        ImmutableList.of(STRING_LIST, STRING_LIST, STRING_LIST);
    List<List<List<String>>> field1 =
        ImmutableList.of(STRING_LISTLIST, STRING_LISTLIST, STRING_LISTLIST);

    private static final List<Double> DOUBLE_LIST = ImmutableList.of(3.14, 3.14, 3.14);
    private static final List<List<Double>> DOUBLE_LISTLIST =
        ImmutableList.of(DOUBLE_LIST, DOUBLE_LIST, DOUBLE_LIST);
    List<List<List<Double>>> field3 =
        ImmutableList.of(DOUBLE_LISTLIST, DOUBLE_LISTLIST, DOUBLE_LISTLIST);

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof PartialRowMultipleArray)) {
        return false;
      }
      PartialRowMultipleArray that = (PartialRowMultipleArray) o;
      return Objects.equals(field1, that.field1);
    }

    @Override
    public int hashCode() {
      return Objects.hash(field1);
    }
  }

  @Test
  @Category(NeedsRunner.class)
  public void testSelectedNestedArrays() {
    PCollection<RowMultipleArray> input = pipeline.apply(Create.of(new RowMultipleArray()));

    PCollection<PartialRowMultipleArray> selected =
        input
            .apply("select1", Select.fieldNames("field1.field1", "field1.field3"))
            .apply("convert1", Convert.to(PartialRowMultipleArray.class));
    PAssert.that(selected).containsInAnyOrder(new PartialRowMultipleArray());

    PCollection<PartialRowMultipleArray> selected2 =
        input
            .apply("select2", Select.fieldNames("field1[][][].field1", "field1[][][].field3"))
            .apply("convert2", Convert.to(PartialRowMultipleArray.class));

    PAssert.that(selected).containsInAnyOrder(new PartialRowMultipleArray());
    PAssert.that(selected2).containsInAnyOrder(new PartialRowMultipleArray());
    pipeline.run();
  }

  @DefaultSchema(JavaFieldSchema.class)
  static class RowMultipleMaps {
    static final Map<String, POJO1> POJO_MAP =
        ImmutableMap.of(
            "key1", new POJO1(),
            "key2", new POJO1(),
            "key3", new POJO1());
    static final Map<String, Map<String, POJO1>> POJO_MAP_MAP =
        ImmutableMap.of(
            "key1", POJO_MAP,
            "key2", POJO_MAP,
            "key3", POJO_MAP);
    Map<String, Map<String, Map<String, POJO1>>> field1 =
        ImmutableMap.of(
            "key1", POJO_MAP_MAP,
            "key2", POJO_MAP_MAP,
            "key3", POJO_MAP_MAP);

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof RowMultipleMaps)) {
        return false;
      }
      RowMultipleMaps that = (RowMultipleMaps) o;
      return Objects.equals(field1, that.field1);
    }

    @Override
    public int hashCode() {
      return Objects.hash(field1);
    }
  }

  @DefaultSchema(JavaFieldSchema.class)
  static class PartialRowMultipleMaps {
    static final Map<String, String> STRING_MAP =
        ImmutableMap.of(
            "key1", "field1",
            "key2", "field1",
            "key3", "field1");
    static final Map<String, Map<String, String>> STRING_MAPMAP =
        ImmutableMap.of(
            "key1", STRING_MAP,
            "key2", STRING_MAP,
            "key3", STRING_MAP);
    Map<String, Map<String, Map<String, String>>> field1 =
        ImmutableMap.of(
            "key1", STRING_MAPMAP,
            "key2", STRING_MAPMAP,
            "key3", STRING_MAPMAP);
    static final Map<String, Double> DOUBLE_MAP =
        ImmutableMap.of(
            "key1", 3.14,
            "key2", 3.14,
            "key3", 3.14);
    static final Map<String, Map<String, Double>> DOUBLE_MAPMAP =
        ImmutableMap.of(
            "key1", DOUBLE_MAP,
            "key2", DOUBLE_MAP,
            "key3", DOUBLE_MAP);

    Map<String, Map<String, Map<String, Double>>> field3 =
        ImmutableMap.of(
            "key1", DOUBLE_MAPMAP,
            "key2", DOUBLE_MAPMAP,
            "key3", DOUBLE_MAPMAP);

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof PartialRowMultipleMaps)) {
        return false;
      }
      PartialRowMultipleMaps that = (PartialRowMultipleMaps) o;
      return Objects.equals(field1, that.field1) && Objects.equals(field3, that.field3);
    }

    @Override
    public int hashCode() {
      return Objects.hash(field1, field3);
    }
  }

  @Test
  @Category(NeedsRunner.class)
  public void testSelectRowNestedMaps() {
    PCollection<RowMultipleMaps> input = pipeline.apply(Create.of(new RowMultipleMaps()));

    PCollection<PartialRowMultipleMaps> selected =
        input
            .apply("select1", Select.fieldNames("field1.field1", "field1.field3"))
            .apply("convert1", Convert.to(PartialRowMultipleMaps.class));

    PCollection<PartialRowMultipleMaps> selected2 =
        input
            .apply("select2", Select.fieldNames("field1{}{}{}.field1", "field1{}{}{}.field3"))
            .apply("convert2", Convert.to(PartialRowMultipleMaps.class));

    PAssert.that(selected).containsInAnyOrder(new PartialRowMultipleMaps());
    PAssert.that(selected2).containsInAnyOrder(new PartialRowMultipleMaps());
    pipeline.run();
  }

  @DefaultSchema(JavaFieldSchema.class)
  static class RowNestedArraysAndMaps {
    static final List<POJO1> POJO_LIST = ImmutableList.of(new POJO1(), new POJO1(), new POJO1());
    static final Map<String, List<POJO1>> POJO_MAP_LIST =
        ImmutableMap.of(
            "key1", POJO_LIST,
            "key2", POJO_LIST,
            "key3", POJO_LIST);
    List<Map<String, List<POJO1>>> field1 =
        ImmutableList.of(POJO_MAP_LIST, POJO_MAP_LIST, POJO_MAP_LIST);

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof RowNestedArraysAndMaps)) {
        return false;
      }
      RowNestedArraysAndMaps that = (RowNestedArraysAndMaps) o;
      return Objects.equals(field1, that.field1);
    }

    @Override
    public int hashCode() {
      return Objects.hash(field1);
    }
  }

  @DefaultSchema(JavaFieldSchema.class)
  static class PartialRowNestedArraysAndMaps {
    static final Map<String, List<String>> STRING_MAP =
        ImmutableMap.of(
            "key1", ImmutableList.of("field1", "field1", "field1"),
            "key2", ImmutableList.of("field1", "field1", "field1"),
            "key3", ImmutableList.of("field1", "field1", "field1"));
    List<Map<String, List<String>>> field1 = ImmutableList.of(STRING_MAP, STRING_MAP, STRING_MAP);

    static final Map<String, List<Double>> DOUBLE_MAP =
        ImmutableMap.of(
            "key1", ImmutableList.of(3.14, 3.14, 3.14),
            "key2", ImmutableList.of(3.14, 3.14, 3.14),
            "key3", ImmutableList.of(3.14, 3.14, 3.14));

    List<Map<String, List<Double>>> field3 = ImmutableList.of(DOUBLE_MAP, DOUBLE_MAP, DOUBLE_MAP);

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof PartialRowNestedArraysAndMaps)) {
        return false;
      }
      PartialRowNestedArraysAndMaps that = (PartialRowNestedArraysAndMaps) o;
      return Objects.equals(field1, that.field1) && Objects.equals(field3, that.field3);
    }

    @Override
    public int hashCode() {
      return Objects.hash(field1, field3);
    }

    @Override
    public String toString() {
      return "PartialRowNestedArraysAndMaps{" + "field1=" + field1 + ", field3=" + field3 + '}';
    }
  }

  @Test
  @Category(NeedsRunner.class)
  public void testSelectRowNestedListsAndMaps() {
    PCollection<RowNestedArraysAndMaps> input =
        pipeline.apply(Create.of(new RowNestedArraysAndMaps()));

    PCollection<PartialRowNestedArraysAndMaps> selected =
        input
            .apply("select1", Select.fieldNames("field1.field1", "field1.field3"))
            .apply("convert1", Convert.to(PartialRowNestedArraysAndMaps.class));

    PCollection<PartialRowNestedArraysAndMaps> selected2 =
        input
            .apply("select2", Select.fieldNames("field1[]{}[].field1", "field1[]{}[].field3"))
            .apply("convert2", Convert.to(PartialRowNestedArraysAndMaps.class));

    PAssert.that(selected).containsInAnyOrder(new PartialRowNestedArraysAndMaps());
    PAssert.that(selected2).containsInAnyOrder(new PartialRowNestedArraysAndMaps());
    pipeline.run();
  }
}
