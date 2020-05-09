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
package org.apache.beam.sdk.transforms;

import static junit.framework.TestCase.assertEquals;

import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SetFnsTest {

  @Rule public final TestPipeline p = TestPipeline.create();

  Schema schema = Schema.builder().addStringField("alphabet").build();

  static PCollection<String> first;
  static PCollection<String> second;
  static PCollection<String> third;
  static PCollection<Row> firstRows;
  static PCollection<Row> secondRows;
  static PCollection<Row> thridRows;

  static final String[] FIRST_DATA = {"a", "a", "a", "b", "b", "c", "d", "d", "g", "g", "h", "h"};
  static final String[] SECOND_DATA = {"a", "a", "b", "b", "b", "c", "d", "d", "e", "e", "f", "f"};
  static final String[] THIRD_DATA = {"b", "b", "c"};

  private Iterable<Row> toRows(String... values) {
    return Iterables.transform(
        Arrays.asList(values), (elem) -> Row.withSchema(schema).addValues(elem).build());
  }

  @Before
  public void setup() {

    first = p.apply("first", Create.of(Arrays.asList(FIRST_DATA)));
    second = p.apply("second", Create.of(Arrays.asList(SECOND_DATA)));
    third = p.apply("third", Create.of(Arrays.asList(THIRD_DATA)));

    firstRows = p.apply("firstRows", Create.of(toRows(FIRST_DATA)).withRowSchema(schema));
    secondRows = p.apply("secondRows", Create.of(toRows(SECOND_DATA)).withRowSchema(schema));
    thridRows = p.apply("thridRows", Create.of(toRows(THIRD_DATA)).withRowSchema(schema));
  }

  @Test
  @Category(NeedsRunner.class)
  public void testIntersection() {
    PAssert.that(first.apply("strings", SetFns.intersectDistinct(second)))
        .containsInAnyOrder("a", "b", "c", "d");

    PCollection<Row> results = firstRows.apply("rows", SetFns.intersectDistinct(secondRows));
    PAssert.that(results).containsInAnyOrder(toRows("a", "b", "c", "d"));
    assertEquals(schema, results.getSchema());

    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testIntersectionCollectionList() {

    PAssert.that(
            PCollectionList.of(first)
                .and(second)
                .and(third)
                .apply("stringsCols", SetFns.intersectDistinct()))
        .containsInAnyOrder("b", "c");

    PAssert.that(
            PCollectionList.of(firstRows)
                .and(secondRows)
                .and(thridRows)
                .apply("rowCols", SetFns.intersectDistinct()))
        .containsInAnyOrder(toRows("b", "c"));

    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testIntersectionAll() {

    PAssert.that(first.apply("strings", SetFns.intersectAll(second)))
        .containsInAnyOrder("a", "a", "b", "b", "c", "d", "d");
    PAssert.that(firstRows.apply("rows", SetFns.intersectAll(secondRows)))
        .containsInAnyOrder(toRows("a", "a", "b", "b", "c", "d", "d"));

    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testIntersectionAllCollectionList() {

    PAssert.that(
            PCollectionList.of(first)
                .and(second)
                .and(third)
                .apply("stringsCols", SetFns.intersectAll()))
        .containsInAnyOrder("b", "b", "c");

    PAssert.that(
            PCollectionList.of(firstRows)
                .and(secondRows)
                .and(thridRows)
                .apply("rowCols", SetFns.intersectAll()))
        .containsInAnyOrder(toRows("b", "b", "c"));

    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testExcept() {

    PAssert.that(first.apply("strings", SetFns.exceptDistinct(second)))
        .containsInAnyOrder("g", "h");
    PAssert.that(firstRows.apply("rows", SetFns.exceptDistinct(secondRows)))
        .containsInAnyOrder(toRows("g", "h"));
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testExceptCollectionList() {

    PAssert.that(
            PCollectionList.of(first)
                .and(second)
                .and(third)
                .apply("stringsCols", SetFns.exceptDistinct()))
        .containsInAnyOrder("g", "h");

    PAssert.that(
            PCollectionList.of(firstRows)
                .and(secondRows)
                .and(thridRows)
                .apply("rowCols", SetFns.exceptDistinct()))
        .containsInAnyOrder(toRows("g", "h"));

    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testExceptAll() {

    PAssert.that(first.apply("strings", SetFns.exceptAll(second)))
        .containsInAnyOrder("a", "g", "g", "h", "h");
    PAssert.that(firstRows.apply("rows", SetFns.exceptAll(secondRows)))
        .containsInAnyOrder(toRows("a", "g", "g", "h", "h"));

    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testUnion() {

    PAssert.that(first.apply("strings", SetFns.unionDistinct(second)))
        .containsInAnyOrder("a", "b", "c", "d", "e", "f", "g", "h");
    PAssert.that(firstRows.apply("rows", SetFns.unionDistinct(secondRows)))
        .containsInAnyOrder(toRows("a", "b", "c", "d", "e", "f", "g", "h"));

    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testUnionAll() {

    PAssert.that(first.apply("strings", SetFns.unionAll(second)))
        .containsInAnyOrder(
            "a", "a", "a", "a", "a", "b", "b", "b", "b", "b", "c", "c", "d", "d", "d", "d", "e",
            "e", "f", "f", "g", "g", "h", "h");
    PAssert.that(firstRows.apply("rows", SetFns.unionAll(secondRows)))
        .containsInAnyOrder(
            toRows(
                "a", "a", "a", "a", "a", "b", "b", "b", "b", "b", "c", "c", "d", "d", "d", "d", "e",
                "e", "f", "f", "g", "g", "h", "h"));

    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testUnionAllCollections() {

    List<String> strings = Arrays.asList("a", "b", "b");
    PCollection<String> set3 = p.apply("set3", Create.of(strings));

    PAssert.that(
            PCollectionList.of(first).and(second).and(set3).apply("stringsCol", SetFns.unionAll()))
        .containsInAnyOrder(
            "a", "a", "a", "a", "a", "a", "b", "b", "b", "b", "b", "b", "b", "c", "c", "d", "d",
            "d", "d", "e", "e", "f", "f", "g", "g", "h", "h");
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testOnlyOneCollectionInPCollectionList() {

    PAssert.that(PCollectionList.of(first).apply("stringsCol", SetFns.intersectDistinct()))
        .containsInAnyOrder(FIRST_DATA);
    p.run();
  }
}
