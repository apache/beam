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
import org.apache.beam.sdk.schemas.DefaultSchema;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

/** Test for {@link Select}. */
public class SelectTest {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  @Rule public transient ExpectedException thrown = ExpectedException.none();

  /** flat POJO to selection from. */
  @DefaultSchema(JavaFieldSchema.class)
  public static class POJO1 {
    public String field1 = "field1";
    public Integer field2 = 42;
    public Double field3 = 3.14;

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
  public static class POJO1Selected {
    public String field1 = "field1";
    public Double field3 = 3.14;

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
  public static class POJO2 {
    public String field1 = "field1";
    public POJO1 field2 = new POJO1();
  }

  /** A pojo matching the schema results from selection field2.*. */
  @DefaultSchema(JavaFieldSchema.class)
  public static class POJO2NestedAll {
    public POJO1 field2 = new POJO1();

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      POJO2NestedAll that = (POJO2NestedAll) o;
      return Objects.equals(field2, that.field2);
    }

    @Override
    public int hashCode() {
      return Objects.hash(field2);
    }
  }

  /** A pojo matching the schema results from selection field2.field1, field2.field3. */
  @DefaultSchema(JavaFieldSchema.class)
  public static class POJO2NestedPartial {
    public POJO1Selected field2 = new POJO1Selected();

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      POJO2NestedPartial that = (POJO2NestedPartial) o;
      return Objects.equals(field2, that.field2);
    }

    @Override
    public int hashCode() {
      return Objects.hash(field2);
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
            .apply(Select.fieldAccess(FieldAccessDescriptor.withAllFields()))
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
    PCollection<POJO2NestedAll> pojos =
        pipeline
            .apply(Create.of(new POJO2()))
            .apply(
                Select.fieldAccess(
                    FieldAccessDescriptor.create()
                        .withNestedField("field2", FieldAccessDescriptor.withAllFields())))
            .apply(Convert.to(POJO2NestedAll.class));
    PAssert.that(pojos).containsInAnyOrder(new POJO2NestedAll());
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testSelectNestedPartial() {
    PCollection<POJO2NestedPartial> pojos =
        pipeline
            .apply(Create.of(new POJO2()))
            .apply(
                Select.fieldAccess(
                    FieldAccessDescriptor.create()
                        .withNestedField(
                            "field2", FieldAccessDescriptor.withFieldNames("field1", "field3"))))
            .apply(Convert.to(POJO2NestedPartial.class));
    PAssert.that(pojos).containsInAnyOrder(new POJO2NestedPartial());
    pipeline.run();
  }
}
