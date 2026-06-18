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

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.UsesSchema;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test for {@link Filter}. * */
@RunWith(JUnit4.class)
@Category(UsesSchema.class)
public class FilterTest {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  @Rule public transient ExpectedException thrown = ExpectedException.none();

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  abstract static class Simple {
    abstract String getField1();

    abstract int getField2();

    abstract int getField3();
  };

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  abstract static class Nested {
    abstract Simple getNested();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testMissingFieldName() {
    thrown.expect(IllegalArgumentException.class);
    pipeline
        .apply(Create.of(new AutoValue_FilterTest_Simple("pass", 52, 2)))
        .apply(Filter.<AutoValue_FilterTest_Simple>create().whereFieldName("missing", f -> true));
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testMissingFieldIndex() {
    thrown.expect(IllegalArgumentException.class);
    pipeline
        .apply(Create.of(new AutoValue_FilterTest_Simple("pass", 52, 2)))
        .apply(Filter.<AutoValue_FilterTest_Simple>create().whereFieldId(23, f -> true));
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testFilterFieldsByName() {
    // Pass only elements where field1 == "pass && field2 > 50.
    PCollection<AutoValue_FilterTest_Simple> filtered =
        pipeline
            .apply(
                Create.of(
                    new AutoValue_FilterTest_Simple("pass", 52, 2),
                    new AutoValue_FilterTest_Simple("pass", 2, 2),
                    new AutoValue_FilterTest_Simple("fail", 100, 100)))
            .apply(
                Filter.<AutoValue_FilterTest_Simple>create()
                    .whereFieldName("field1", s -> "pass".equals(s))
                    .whereFieldName("field2", (Integer i) -> i > 50));
    PAssert.that(filtered).containsInAnyOrder(new AutoValue_FilterTest_Simple("pass", 52, 2));
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testFilterOnNestedField() {
    // Pass only elements where field1 == "pass && field2 > 50.
    PCollection<AutoValue_FilterTest_Nested> filtered =
        pipeline
            .apply(
                Create.of(
                    new AutoValue_FilterTest_Nested(new AutoValue_FilterTest_Simple("pass", 52, 2)),
                    new AutoValue_FilterTest_Nested(new AutoValue_FilterTest_Simple("pass", 2, 2)),
                    new AutoValue_FilterTest_Nested(
                        new AutoValue_FilterTest_Simple("fail", 100, 100))))
            .apply(
                Filter.<AutoValue_FilterTest_Nested>create()
                    .whereFieldName("nested.field1", s -> "pass".equals(s))
                    .whereFieldName("nested.field2", (Integer i) -> i > 50));
    PAssert.that(filtered)
        .containsInAnyOrder(
            new AutoValue_FilterTest_Nested(new AutoValue_FilterTest_Simple("pass", 52, 2)));
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testFilterMultipleFields() {
    // Pass only elements where field1 + field2 >= 100.
    PCollection<AutoValue_FilterTest_Simple> filtered =
        pipeline
            .apply(
                Create.of(
                    new AutoValue_FilterTest_Simple("", 52, 48),
                    new AutoValue_FilterTest_Simple("", 52, 2),
                    new AutoValue_FilterTest_Simple("", 70, 33)))
            .apply(
                Filter.<AutoValue_FilterTest_Simple>create()
                    .whereFieldNames(
                        Lists.newArrayList("field2", "field3"),
                        r -> r.getInt32("field2") + r.getInt32("field3") >= 100));
    PAssert.that(filtered)
        .containsInAnyOrder(
            new AutoValue_FilterTest_Simple("", 52, 48),
            new AutoValue_FilterTest_Simple("", 70, 33));
    pipeline.run();
  }
}
