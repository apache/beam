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
public class SetsTest {

  @Rule public final TestPipeline p = TestPipeline.create();

  Schema schema = Schema.builder().addStringField("alphabet").build();

  static PCollection<String> first;
  static PCollection<String> second;
  static PCollection<Row> firstRows;
  static PCollection<Row> secondRows;

  private Iterable<Row> toRows(String... values) {
    return Iterables.transform(
        Arrays.asList(values), (elem) -> Row.withSchema(schema).addValues(elem).build());
  }

  @Before
  public void setup() {
    final String[] firstData = {"a", "a", "a", "b", "b", "c", "d", "d", "g", "g", "h", "h"};
    final String[] secondData = {"a", "a", "b", "b", "b", "c", "d", "d", "e", "e", "f", "f"};

    first = p.apply("first", Create.of(Arrays.asList(firstData)));
    second = p.apply("second", Create.of(Arrays.asList(secondData)));

    firstRows = p.apply("firstRows", Create.of(toRows(firstData)).withRowSchema(schema));
    secondRows = p.apply("secondRows", Create.of(toRows(secondData)).withRowSchema(schema));
  }

  @Test
  @Category(NeedsRunner.class)
  public void testIntersection() {
    PAssert.that(first.apply("strings", Sets.intersectDistinct(second)))
        .containsInAnyOrder("a", "b", "c", "d");

    PCollection<Row> results = firstRows.apply("rows", Sets.intersectDistinct(secondRows));

    PAssert.that(results).containsInAnyOrder(toRows("a", "b", "c", "d"));

    assertEquals(schema, results.getSchema());

    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testIntersectionCollectionList() {

    PCollection<String> third = p.apply("third", Create.of(Arrays.asList("b", "b", "c", "f")));
    PCollection<Row> thirdRows = p.apply("thirdRows", Create.of(toRows("b", "b", "c", "f")));

    PAssert.that(
            PCollectionList.of(first)
                .and(second)
                .and(third)
                .apply("stringsCols", Sets.intersectDistinct()))
        .containsInAnyOrder("b", "c");

    PCollection<Row> results =
        PCollectionList.of(firstRows)
            .and(secondRows)
            .and(thirdRows)
            .apply("rowCols", Sets.intersectDistinct());

    PAssert.that(results).containsInAnyOrder(toRows("b", "c"));

    assertEquals(schema, results.getSchema());

    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testIntersectionAll() {

    PAssert.that(first.apply("strings", Sets.intersectAll(second)))
        .containsInAnyOrder("a", "a", "b", "b", "c", "d", "d");

    PCollection<Row> results = firstRows.apply("rows", Sets.intersectAll(secondRows));

    PAssert.that(results).containsInAnyOrder(toRows("a", "a", "b", "b", "c", "d", "d"));

    assertEquals(schema, results.getSchema());

    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testIntersectionAllCollectionList() {
    PCollection<String> third = p.apply("third", Create.of(Arrays.asList("a", "b", "f")));
    PCollection<Row> thirdRows = p.apply("thirdRows", Create.of(toRows("a", "b", "f")));

    PAssert.that(
            PCollectionList.of(first)
                .and(second)
                .and(third)
                .apply("stringsCols", Sets.intersectAll()))
        .containsInAnyOrder("a", "b");

    PCollection<Row> results =
        PCollectionList.of(firstRows)
            .and(secondRows)
            .and(thirdRows)
            .apply("rowCols", Sets.intersectAll());

    PAssert.that(results).containsInAnyOrder(toRows("a", "b"));

    assertEquals(schema, results.getSchema());

    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testExcept() {

    PAssert.that(first.apply("strings", Sets.exceptDistinct(second))).containsInAnyOrder("g", "h");

    PCollection<Row> results = firstRows.apply("rows", Sets.exceptDistinct(secondRows));

    PAssert.that(results).containsInAnyOrder(toRows("g", "h"));

    assertEquals(schema, results.getSchema());

    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testExceptCollectionList() {
    PCollection<String> third = p.apply("third", Create.of(Arrays.asList("a", "b", "b", "g", "g")));
    PCollection<Row> thirdRows = p.apply("thirdRows", Create.of(toRows("a", "b", "b", "g", "g")));

    PAssert.that(
            PCollectionList.of(first)
                .and(second)
                .and(third)
                .apply("stringsCols", Sets.exceptDistinct()))
        .containsInAnyOrder("h");

    PCollection<Row> results =
        PCollectionList.of(firstRows)
            .and(secondRows)
            .and(thirdRows)
            .apply("rowCols", Sets.exceptDistinct());

    PAssert.that(results).containsInAnyOrder(toRows("h"));

    assertEquals(schema, results.getSchema());

    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testExceptAll() {

    PAssert.that(first.apply("strings", Sets.exceptAll(second)))
        .containsInAnyOrder("a", "g", "g", "h", "h");

    PCollection<Row> results = firstRows.apply("rows", Sets.exceptAll(secondRows));

    PAssert.that(results).containsInAnyOrder(toRows("a", "g", "g", "h", "h"));

    assertEquals(schema, results.getSchema());

    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testExceptAllCollectionList() {
    PCollection<String> third = p.apply("third", Create.of(Arrays.asList("a", "b", "b", "g", "f")));
    PCollection<Row> thirdRows = p.apply("thirdRows", Create.of(toRows("a", "b", "b", "g")));

    PAssert.that(
            PCollectionList.of(first).and(second).and(third).apply("stringsCols", Sets.exceptAll()))
        .containsInAnyOrder("g", "h", "h");

    PCollection<Row> results =
        PCollectionList.of(firstRows)
            .and(secondRows)
            .and(thirdRows)
            .apply("rowCols", Sets.exceptAll());

    PAssert.that(results).containsInAnyOrder(toRows("g", "h", "h"));

    assertEquals(schema, results.getSchema());

    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testUnion() {

    PAssert.that(first.apply("strings", Sets.unionDistinct(second)))
        .containsInAnyOrder("a", "b", "c", "d", "e", "f", "g", "h");

    PCollection<Row> results = firstRows.apply("rows", Sets.unionDistinct(secondRows));

    PAssert.that(results).containsInAnyOrder(toRows("a", "b", "c", "d", "e", "f", "g", "h"));

    assertEquals(schema, results.getSchema());

    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testUnionCollectionList() {
    PCollection<String> third = p.apply("third", Create.of(Arrays.asList("a", "k", "k")));
    PCollection<Row> thirdRows = p.apply("thirdRows", Create.of(toRows("a", "k", "k")));

    PAssert.that(
            PCollectionList.of(first)
                .and(second)
                .and(third)
                .apply("stringsCols", Sets.unionDistinct()))
        .containsInAnyOrder("a", "b", "c", "d", "e", "f", "g", "h", "k");

    PCollection<Row> results =
        PCollectionList.of(firstRows)
            .and(secondRows)
            .and(thirdRows)
            .apply("rowCols", Sets.unionDistinct());

    PAssert.that(results).containsInAnyOrder(toRows("a", "b", "c", "d", "e", "f", "g", "h", "k"));

    assertEquals(schema, results.getSchema());

    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testUnionAll() {

    PAssert.that(first.apply("strings", Sets.unionAll(second)))
        .containsInAnyOrder(
            "a", "a", "a", "a", "a", "b", "b", "b", "b", "b", "c", "c", "d", "d", "d", "d", "e",
            "e", "f", "f", "g", "g", "h", "h");

    PCollection<Row> results = firstRows.apply("rows", Sets.unionAll(secondRows));

    PAssert.that(results)
        .containsInAnyOrder(
            toRows(
                "a", "a", "a", "a", "a", "b", "b", "b", "b", "b", "c", "c", "d", "d", "d", "d", "e",
                "e", "f", "f", "g", "g", "h", "h"));

    assertEquals(schema, results.getSchema());

    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testUnionAllCollections() {

    PCollection<String> third = p.apply("third", Create.of(Arrays.asList("a", "b", "b", "k", "k")));
    PCollection<Row> thirdRows = p.apply("thirdRows", Create.of(toRows("a", "b", "b", "k", "k")));

    PAssert.that(
            PCollectionList.of(first).and(second).and(third).apply("stringsCols", Sets.unionAll()))
        .containsInAnyOrder(
            "a", "a", "a", "a", "a", "a", "b", "b", "b", "b", "b", "b", "b", "c", "c", "d", "d",
            "d", "d", "e", "e", "f", "f", "g", "g", "h", "h", "k", "k");

    PCollection<Row> results =
        PCollectionList.of(firstRows)
            .and(secondRows)
            .and(thirdRows)
            .apply("rowCols", Sets.unionAll());

    PAssert.that(results)
        .containsInAnyOrder(
            toRows(
                "a", "a", "a", "a", "a", "a", "b", "b", "b", "b", "b", "b", "b", "c", "c", "d", "d",
                "d", "d", "e", "e", "f", "f", "g", "g", "h", "h", "k", "k"));

    assertEquals(schema, results.getSchema());

    p.run();
  }
}
