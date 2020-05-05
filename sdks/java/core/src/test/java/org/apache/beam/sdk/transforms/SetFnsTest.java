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
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
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

  static PCollection<String> left;
  static PCollection<String> right;
  static PCollection<Row> leftRow;
  static PCollection<Row> rightRow;

  private Iterable<Row> toRows(String... values) {
    return Iterables.transform(
        Arrays.asList(values), (elem) -> Row.withSchema(schema).addValues(elem).build());
  }

  @Before
  public void setup() {

    String[] leftData = {"a", "a", "a", "b", "b", "c", "d", "d", "g", "g", "h", "h"};
    String[] rightData = {"a", "a", "b", "b", "b", "c", "d", "d", "e", "e", "f", "f"};

    left = p.apply("left", Create.of(Arrays.asList(leftData)));
    right = p.apply("right", Create.of(Arrays.asList(rightData)));
    leftRow = p.apply("leftRow", Create.of(toRows(leftData)).withRowSchema(schema));
    rightRow = p.apply("rightRow", Create.of(toRows(rightData)).withRowSchema(schema));
  }

  @Test
  @Category(NeedsRunner.class)
  public void testIntersection() {
    PAssert.that(left.apply("strings", org.apache.beam.sdk.transforms.SetFns.intersect(right)))
        .containsInAnyOrder("a", "b", "c", "d");

    PCollection<Row> results =
        leftRow.apply("rows", org.apache.beam.sdk.transforms.SetFns.intersect(rightRow));
    PAssert.that(results).containsInAnyOrder(toRows("a", "b", "c", "d"));
    assertEquals(schema, results.getSchema());

    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testIntersectionAll() {

    PAssert.that(left.apply("strings", org.apache.beam.sdk.transforms.SetFns.intersectAll(right)))
        .containsInAnyOrder("a", "a", "b", "b", "c", "d", "d");
    PAssert.that(
            leftRow.apply("rows", org.apache.beam.sdk.transforms.SetFns.intersectAll(rightRow)))
        .containsInAnyOrder(toRows("a", "a", "b", "b", "c", "d", "d"));

    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testExcept() {

    PAssert.that(left.apply("strings", org.apache.beam.sdk.transforms.SetFns.except(right)))
        .containsInAnyOrder("g", "h");
    PAssert.that(leftRow.apply("rows", org.apache.beam.sdk.transforms.SetFns.except(rightRow)))
        .containsInAnyOrder(toRows("g", "h"));
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testExceptAll() {

    PAssert.that(left.apply("strings", org.apache.beam.sdk.transforms.SetFns.exceptAll(right)))
        .containsInAnyOrder("a", "g", "g", "h", "h");
    PAssert.that(leftRow.apply("rows", org.apache.beam.sdk.transforms.SetFns.exceptAll(rightRow)))
        .containsInAnyOrder(toRows("a", "g", "g", "h", "h"));

    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testUnion() {

    PAssert.that(left.apply("strings", org.apache.beam.sdk.transforms.SetFns.union(right)))
        .containsInAnyOrder("a", "b", "c", "d", "e", "f", "g", "h");
    PAssert.that(leftRow.apply("rows", org.apache.beam.sdk.transforms.SetFns.union(rightRow)))
        .containsInAnyOrder(toRows("a", "b", "c", "d", "e", "f", "g", "h"));

    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testUnionAll() {

    PAssert.that(left.apply("strings", org.apache.beam.sdk.transforms.SetFns.unionAll(right)))
        .containsInAnyOrder(
            "a", "a", "a", "a", "a", "b", "b", "b", "b", "b", "c", "c", "d", "d", "d", "d", "e",
            "e", "f", "f", "g", "g", "h", "h");
    PAssert.that(leftRow.apply("rows", org.apache.beam.sdk.transforms.SetFns.unionAll(rightRow)))
        .containsInAnyOrder(
            toRows(
                "a", "a", "a", "a", "a", "b", "b", "b", "b", "b", "c", "c", "d", "d", "d", "d", "e",
                "e", "f", "f", "g", "g", "h", "h"));

    p.run();
  }
}
