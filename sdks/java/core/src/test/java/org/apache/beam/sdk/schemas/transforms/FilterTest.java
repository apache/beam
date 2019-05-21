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

import java.util.Objects;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.UsesSchema;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Lists;
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

  /** POJO used to test schemas. * */
  @DefaultSchema(JavaFieldSchema.class)
  public static class POJO {
    public String field1;
    public int field2;
    public int field3;

    public POJO(String field1, int field2, int field3) {
      this.field1 = field1;
      this.field2 = field2;
      this.field3 = field3;
    }

    public POJO() {}

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      POJO pojo = (POJO) o;
      return Objects.equals(field1, pojo.field1)
          && Objects.equals(field2, pojo.field2)
          && Objects.equals(field3, pojo.field3);
    }

    @Override
    public int hashCode() {
      return Objects.hash(field1, field2, field3);
    }
  };

  @Test
  @Category(NeedsRunner.class)
  public void testMissingFieldName() {
    thrown.expect(IllegalArgumentException.class);
    pipeline
        .apply(Create.of(new POJO("pass", 52, 2)))
        .apply(Filter.<POJO>create().whereFieldName("missing", f -> true));
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testMissingFieldIndex() {
    thrown.expect(IllegalArgumentException.class);
    pipeline
        .apply(Create.of(new POJO("pass", 52, 2)))
        .apply(Filter.<POJO>create().whereFieldId(23, f -> true));
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testFilterFieldsByName() {
    // Pass only elements where field1 == "pass && field2 > 50.
    PCollection<POJO> filtered =
        pipeline
            .apply(
                Create.of(
                    new POJO("pass", 52, 2), new POJO("pass", 2, 2), new POJO("fail", 100, 100)))
            .apply(
                Filter.<POJO>create()
                    .whereFieldName("field1", s -> "pass".equals(s))
                    .whereFieldName("field2", i -> (Integer) i > 50));
    PAssert.that(filtered).containsInAnyOrder(new POJO("pass", 52, 2));
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testFilterMultipleFields() {
    // Pass only elements where field1 + field2 >= 100.
    PCollection<POJO> filtered =
        pipeline
            .apply(Create.of(new POJO("", 52, 48), new POJO("", 52, 2), new POJO("", 70, 33)))
            .apply(
                Filter.<POJO>create()
                    .whereFieldNames(
                        Lists.newArrayList("field2", "field3"),
                        r -> r.getInt32("field2") + r.getInt32("field3") >= 100));
    PAssert.that(filtered).containsInAnyOrder(new POJO("", 52, 48), new POJO("", 70, 33));
    pipeline.run();
  }
}
