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

import static org.junit.Assert.assertEquals;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.DefaultSchema;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link Cast}. */
public class CastTest {

  /** Unit tests. */
  @RunWith(JUnit4.class)
  public static class UnitTests {
    @Test
    public void testCastArray() {
      Object output =
          Cast.castValue(
              Arrays.asList((short) 1, (short) 2, (short) 3),
              Schema.FieldType.array(Schema.FieldType.INT16),
              Schema.FieldType.array(Schema.FieldType.INT32));

      assertEquals(Arrays.asList(1, 2, 3), output);
    }

    @Test
    public void testCastMap() {
      Object output =
          Cast.castValue(
              ImmutableMap.of((short) 1, 1, (short) 2, 2, (short) 3, 3),
              Schema.FieldType.map(Schema.FieldType.INT16, Schema.FieldType.INT32),
              Schema.FieldType.map(Schema.FieldType.INT32, Schema.FieldType.INT64));

      assertEquals(ImmutableMap.of(1, 1L, 2, 2L, 3, 3L), output);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIgnoreNullFail() {
      Schema inputSchema = Schema.of(Schema.Field.nullable("f0", Schema.FieldType.INT32));
      Schema outputSchema = Schema.of(Schema.Field.of("f0", Schema.FieldType.INT32));

      Cast.castRow(Row.withSchema(inputSchema).addValue(null).build(), inputSchema, outputSchema);
    }
  }

  /** NeedsRunner tests. */
  @Category(NeedsRunner.class)
  @RunWith(JUnit4.class)
  public static class NeedsRunnerTests implements Serializable {
    @Rule public final transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void testProjection() throws Exception {
      Schema outputSchema = pipeline.getSchemaRegistry().getSchema(Projection2.class);
      PCollection<Projection2> pojos =
          pipeline
              .apply(Create.of(new Projection1()))
              .apply(Cast.widening(outputSchema))
              .apply(Convert.to(Projection2.class));

      PAssert.that(pojos).containsInAnyOrder(new Projection2());
      pipeline.run();
    }

    @Test
    public void testTypeWiden() throws Exception {
      Schema outputSchema = pipeline.getSchemaRegistry().getSchema(TypeWiden2.class);

      PCollection<TypeWiden2> pojos =
          pipeline
              .apply(Create.of(new TypeWiden1()))
              .apply(Cast.widening(outputSchema))
              .apply(Convert.to(TypeWiden2.class));

      PAssert.that(pojos).containsInAnyOrder(new TypeWiden2());
      pipeline.run();
    }

    @Test
    public void testTypeNarrow() throws Exception {
      // narrowing is the opposite of widening
      Schema outputSchema = pipeline.getSchemaRegistry().getSchema(TypeWiden1.class);

      PCollection<TypeWiden1> pojos =
          pipeline
              .apply(Create.of(new TypeWiden2()))
              .apply(Cast.narrowing(outputSchema))
              .apply(Convert.to(TypeWiden1.class));

      PAssert.that(pojos).containsInAnyOrder(new TypeWiden1());
      pipeline.run();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTypeNarrowFail() throws Exception {
      // narrowing is the opposite of widening
      Schema inputSchema = pipeline.getSchemaRegistry().getSchema(TypeWiden2.class);
      Schema outputSchema = pipeline.getSchemaRegistry().getSchema(TypeWiden1.class);

      Cast.narrowing(outputSchema).verifyCompatibility(inputSchema);
    }

    @Test
    public void testWeakedNullable() throws Exception {
      Schema outputSchema = pipeline.getSchemaRegistry().getSchema(Nullable2.class);

      PCollection<Nullable2> pojos =
          pipeline
              .apply(Create.of(new Nullable1()))
              .apply(Cast.narrowing(outputSchema))
              .apply(Convert.to(Nullable2.class));

      PAssert.that(pojos).containsInAnyOrder(new Nullable2());
      pipeline.run();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWeakedNullableFail() throws Exception {
      Schema inputSchema = pipeline.getSchemaRegistry().getSchema(Nullable1.class);
      Schema outputSchema = pipeline.getSchemaRegistry().getSchema(Nullable2.class);

      Cast.widening(outputSchema).verifyCompatibility(inputSchema);
    }

    @Test
    public void testIgnoreNullable() throws Exception {
      // ignoring nullable is opposite of weakening
      Schema outputSchema = pipeline.getSchemaRegistry().getSchema(Nullable1.class);

      PCollection<Nullable1> pojos =
          pipeline
              .apply(Create.of(new Nullable2()))
              .apply(Cast.narrowing(outputSchema))
              .apply(Convert.to(Nullable1.class));

      PAssert.that(pojos).containsInAnyOrder(new Nullable1());
      pipeline.run();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIgnoreNullableFail() throws Exception {
      // ignoring nullable is opposite of weakening
      Schema inputSchema = pipeline.getSchemaRegistry().getSchema(Nullable2.class);
      Schema outputSchema = pipeline.getSchemaRegistry().getSchema(Nullable1.class);

      Cast.widening(outputSchema).verifyCompatibility(inputSchema);
    }

    @Test
    public void testComplexCast() throws Exception {
      Schema outputSchema = pipeline.getSchemaRegistry().getSchema(All2.class);

      PCollection<All2> pojos =
          pipeline
              .apply(Create.of(new All1()))
              .apply(Cast.narrowing(outputSchema))
              .apply(Convert.to(All2.class));

      PAssert.that(pojos).containsInAnyOrder(new All2());
      pipeline.run();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testComplexCastFail() throws Exception {
      Schema inputSchema = pipeline.getSchemaRegistry().getSchema(All1.class);
      Schema outputSchema = pipeline.getSchemaRegistry().getSchema(All2.class);

      Cast.widening(outputSchema).verifyCompatibility(inputSchema);
    }
  }

  /** POJO for {@link CastTest.NeedsRunnerTests#testProjection()}. */
  @DefaultSchema(JavaFieldSchema.class)
  @VisibleForTesting
  public static class Projection1 {

    public Short field1 = 42;
    public Integer field2 = 1337;
    public String field3 = "field";

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final Projection1 pojo1 = (Projection1) o;
      return Objects.equals(field1, pojo1.field1) && Objects.equals(field2, pojo1.field2);
    }

    @Override
    public int hashCode() {
      return Objects.hash(field1, field2);
    }

    @Override
    public String toString() {
      return "Projection1{"
          + "field1="
          + field1
          + ", field2="
          + field2
          + ", field3='"
          + field3
          + '\''
          + '}';
    }
  }

  /** POJO for {@link CastTest.NeedsRunnerTests#testProjection()}. */
  @DefaultSchema(JavaFieldSchema.class)
  @VisibleForTesting
  public static class Projection2 {
    public Integer field2 = 1337;

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final Projection2 that = (Projection2) o;
      return Objects.equals(field2, that.field2);
    }

    @Override
    public int hashCode() {
      return Objects.hash(field2);
    }

    @Override
    public String toString() {
      return "Projection2{" + "field2=" + field2 + '}';
    }
  }

  /** POJO for {@link CastTest.NeedsRunnerTests#testTypeWiden()}. */
  @DefaultSchema(JavaFieldSchema.class)
  public static class TypeWiden1 {

    public Short field1 = 42;
    public Integer field2 = 1337;

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final TypeWiden1 typeWiden1 = (TypeWiden1) o;
      return Objects.equals(field1, typeWiden1.field1) && Objects.equals(field2, typeWiden1.field2);
    }

    @Override
    public int hashCode() {
      return Objects.hash(field1, field2);
    }

    @Override
    public String toString() {
      return "TypeWiden1{" + "field1=" + field1 + ", field2=" + field2 + '}';
    }
  }

  /** POJO for {@link CastTest.NeedsRunnerTests#testTypeWiden()}. */
  @DefaultSchema(JavaFieldSchema.class)
  @VisibleForTesting
  public static class TypeWiden2 {

    public Integer field1 = 42;
    public Long field2 = 1337L;

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final TypeWiden2 typeWiden2 = (TypeWiden2) o;
      return Objects.equals(field1, typeWiden2.field1) && Objects.equals(field2, typeWiden2.field2);
    }

    @Override
    public int hashCode() {
      return Objects.hash(field1, field2);
    }

    @Override
    public String toString() {
      return "TypeWiden2{" + "field1=" + field1 + ", field2=" + field2 + '}';
    }
  }

  /** POJO for {@link CastTest.NeedsRunnerTests#testWeakedNullable()}. */
  @DefaultSchema(JavaFieldSchema.class)
  @VisibleForTesting
  public static class Nullable1 {
    public Integer field1 = 42;
    public @Nullable Long field2 = null;

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final Nullable1 nullable1 = (Nullable1) o;
      return Objects.equals(field1, nullable1.field1) && Objects.equals(field2, nullable1.field2);
    }

    @Override
    public int hashCode() {
      return Objects.hash(field1, field2);
    }

    @Override
    public String toString() {
      return "Nullable1{" + "field1=" + field1 + ", field2=" + field2 + '}';
    }
  }

  /** POJO for {@link CastTest.NeedsRunnerTests#testWeakedNullable()}. */
  @DefaultSchema(JavaFieldSchema.class)
  @VisibleForTesting
  public static class Nullable2 {
    public @Nullable Integer field1 = 42;
    public @Nullable Long field2 = null;

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final Nullable2 nullable2 = (Nullable2) o;
      return Objects.equals(field1, nullable2.field1) && Objects.equals(field2, nullable2.field2);
    }

    @Override
    public int hashCode() {
      return Objects.hash(field1, field2);
    }

    @Override
    public String toString() {
      return "Nullable2{" + "field1=" + field1 + ", field2=" + field2 + '}';
    }
  }

  /** POJO for {@link CastTest.NeedsRunnerTests#testComplexCast()}. */
  @DefaultSchema(JavaFieldSchema.class)
  @VisibleForTesting
  public static class All1 {
    public Projection1 field1 = new Projection1();
    public TypeWiden1 field2 = new TypeWiden1();
    public TypeWiden2 field3 = new TypeWiden2();
    public Nullable1 field4 = new Nullable1();
    public @Nullable Nullable2 field5 = new Nullable2();

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final All1 all1 = (All1) o;
      return Objects.equals(field1, all1.field1)
          && Objects.equals(field2, all1.field2)
          && Objects.equals(field3, all1.field3)
          && Objects.equals(field4, all1.field4)
          && Objects.equals(field5, all1.field5);
    }

    @Override
    public int hashCode() {
      return Objects.hash(field1, field2, field3, field4, field5);
    }

    @Override
    public String toString() {
      return "All1{"
          + "field1="
          + field1
          + ", field2="
          + field2
          + ", field3="
          + field3
          + ", field4="
          + field4
          + ", field5="
          + field5
          + '}';
    }
  }

  /** POJO for {@link CastTest.NeedsRunnerTests#testComplexCast()}. */
  @DefaultSchema(JavaFieldSchema.class)
  @VisibleForTesting
  public static class All2 {
    public Projection2 field1 = new Projection2();
    public TypeWiden2 field2 = new TypeWiden2();
    public TypeWiden1 field3 = new TypeWiden1();
    public Nullable2 field4 = new Nullable2();
    public @Nullable Nullable1 field5 = new Nullable1();

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final All2 all2 = (All2) o;
      return Objects.equals(field1, all2.field1)
          && Objects.equals(field2, all2.field2)
          && Objects.equals(field3, all2.field3)
          && Objects.equals(field4, all2.field4)
          && Objects.equals(field5, all2.field5);
    }

    @Override
    public int hashCode() {
      return Objects.hash(field1, field2, field3, field4, field5);
    }

    @Override
    public String toString() {
      return "All2{"
          + "field1="
          + field1
          + ", field2="
          + field2
          + ", field3="
          + field3
          + ", field4="
          + field4
          + ", field5="
          + field5
          + '}';
    }
  }
}
