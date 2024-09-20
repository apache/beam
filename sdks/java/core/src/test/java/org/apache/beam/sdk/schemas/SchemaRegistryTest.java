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
package org.apache.beam.sdk.schemas;

import static org.apache.beam.sdk.schemas.annotations.DefaultSchema.DefaultSchemaProvider;
import static org.apache.beam.sdk.schemas.utils.TestJavaBeans.SIMPLE_BEAN_SCHEMA;
import static org.apache.beam.sdk.schemas.utils.TestPOJOs.SIMPLE_POJO_SCHEMA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import java.util.List;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.utils.TestJavaBeans.SimpleBean;
import org.apache.beam.sdk.schemas.utils.TestPOJOs.SimplePOJO;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Unit tests for {@link SchemaRegistry}. */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
})
public class SchemaRegistryTest {
  static final Schema EMPTY_SCHEMA = Schema.builder().build();
  static final Schema STRING_SCHEMA = Schema.builder().addStringField("string").build();
  static final Schema INTEGER_SCHEMA = Schema.builder().addInt32Field("integer").build();
  @Rule public ExpectedException thrown = ExpectedException.none();

  private void tryGetters(SchemaRegistry registry) throws NoSuchSchemaException {
    assertEquals(STRING_SCHEMA, registry.getSchema(String.class));
    assertEquals(STRING_SCHEMA, registry.getSchema(TypeDescriptors.strings()));
    assertEquals(
        Row.withSchema(STRING_SCHEMA).addValue("foobar").build(),
        registry.getToRowFunction(String.class).apply("foobar"));
    assertEquals(
        Row.withSchema(STRING_SCHEMA).addValue("foobar").build(),
        registry.getToRowFunction(TypeDescriptors.strings()).apply("foobar"));

    assertEquals(INTEGER_SCHEMA, registry.getSchema(Integer.class));
    assertEquals(INTEGER_SCHEMA, registry.getSchema(TypeDescriptors.integers()));
    assertEquals(
        Row.withSchema(INTEGER_SCHEMA).addValue(42).build(),
        registry.getToRowFunction(Integer.class).apply(42));
    assertEquals(
        Row.withSchema(INTEGER_SCHEMA).addValue(42).build(),
        registry.getToRowFunction(TypeDescriptors.integers()).apply(42));

    thrown.expect(NoSuchSchemaException.class);
    registry.getSchema(Double.class);
  }

  @Test
  public void testRegisterForClass() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    registry.registerSchemaForClass(
        String.class,
        STRING_SCHEMA,
        s -> Row.withSchema(STRING_SCHEMA).addValue(s).build(),
        r -> r.getString("string"));
    registry.registerSchemaForClass(
        Integer.class,
        INTEGER_SCHEMA,
        s -> Row.withSchema(INTEGER_SCHEMA).addValue(s).build(),
        r -> r.getInt32("integer"));
    tryGetters(registry);
  }

  @Test
  public void testRegisterForType() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    registry.registerSchemaForType(
        TypeDescriptors.strings(),
        STRING_SCHEMA,
        s -> Row.withSchema(STRING_SCHEMA).addValue(s).build(),
        r -> r.getString("string"));
    registry.registerSchemaForType(
        TypeDescriptors.integers(),
        INTEGER_SCHEMA,
        s -> Row.withSchema(INTEGER_SCHEMA).addValue(s).build(),
        r -> r.getInt32("integer"));
    tryGetters(registry);
  }

  /** A test SchemaProvider. */
  public static final class Provider implements SchemaProvider {
    @Override
    public <T> Schema schemaFor(TypeDescriptor<T> typeDescriptor) {
      if (typeDescriptor.equals(TypeDescriptors.strings())) {
        return STRING_SCHEMA;
      } else if (typeDescriptor.equals(TypeDescriptors.integers())) {
        return INTEGER_SCHEMA;
      } else {
        return null;
      }
    }

    @Override
    public <T> SerializableFunction<T, Row> toRowFunction(TypeDescriptor<T> typeDescriptor) {
      return v -> Row.withSchema(schemaFor(typeDescriptor)).addValue(v).build();
    }

    @Override
    public <T> SerializableFunction<Row, T> fromRowFunction(TypeDescriptor<T> typeDescriptor) {
      return r -> r.getValue(0);
    }
  }

  @Test
  public void testRegisterProvider() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    registry.registerSchemaProvider(new Provider());
    tryGetters(registry);
  }

  static class TestSchemaClass {}

  static final class TestAutoProvider implements SchemaProvider {
    @Override
    public <T> Schema schemaFor(TypeDescriptor<T> typeDescriptor) {
      if (typeDescriptor.equals(TypeDescriptor.of(TestSchemaClass.class))) {
        return EMPTY_SCHEMA;
      }
      return null;
    }

    @Override
    public <T> SerializableFunction<T, Row> toRowFunction(TypeDescriptor<T> typeDescriptor) {
      if (typeDescriptor.equals(TypeDescriptor.of(TestSchemaClass.class))) {
        return v -> Row.withSchema(EMPTY_SCHEMA).build();
      }
      return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> SerializableFunction<Row, T> fromRowFunction(TypeDescriptor<T> typeDescriptor) {
      if (typeDescriptor.equals(TypeDescriptor.of(TestSchemaClass.class))) {
        return r -> (T) new TestSchemaClass();
      }
      return null;
    }
  }

  /** A @link SchemaProviderRegistrar} for testing. */
  @AutoService(SchemaProviderRegistrar.class)
  public static class TestSchemaProviderRegistrar implements SchemaProviderRegistrar {
    @Override
    public List<SchemaProvider> getSchemaProviders() {
      return ImmutableList.of(new TestAutoProvider());
    }
  }

  @Test
  public void testAutoSchemaProvider() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    assertEquals(EMPTY_SCHEMA, registry.getSchema(TestSchemaClass.class));
  }

  @DefaultSchema(TestDefaultSchemaProvider.class)
  static class TestDefaultSchemaClass {}

  /** A test schema provider. */
  public static final class TestDefaultSchemaProvider implements SchemaProvider {
    @Override
    public <T> Schema schemaFor(TypeDescriptor<T> typeDescriptor) {
      if (typeDescriptor.equals(TypeDescriptor.of(TestDefaultSchemaClass.class))) {
        return EMPTY_SCHEMA;
      }
      return null;
    }

    @Override
    public <T> SerializableFunction<T, Row> toRowFunction(TypeDescriptor<T> typeDescriptor) {
      if (typeDescriptor.equals(TypeDescriptor.of(TestDefaultSchemaClass.class))) {
        return v -> Row.withSchema(EMPTY_SCHEMA).build();
      }
      return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> SerializableFunction<Row, T> fromRowFunction(TypeDescriptor<T> typeDescriptor) {
      if (typeDescriptor.equals(TypeDescriptor.of(TestDefaultSchemaClass.class))) {
        return r -> (T) new TestSchemaClass();
      }
      return null;
    }
  }

  @Test
  public void testDefaultSchemaProvider() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    assertEquals(EMPTY_SCHEMA, registry.getSchema(TestDefaultSchemaClass.class));
  }

  @Test
  public void testRegisterPojo() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    registry.registerPOJO(SimplePOJO.class);
    Schema schema = registry.getSchema(SimplePOJO.class);
    assertTrue(SIMPLE_POJO_SCHEMA.equivalent(schema));
  }

  @Test
  public void testRegisterJavaBean() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    registry.registerJavaBean(SimpleBean.class);
    Schema schema = registry.getSchema(SimpleBean.class);
    assertTrue(SIMPLE_BEAN_SCHEMA.equivalent(schema));
  }

  @Test
  public void testGetSchemaCoder() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    registry.registerJavaBean(SimpleBean.class);

    Schema schema = registry.getSchema(SimpleBean.class);
    SerializableFunction<SimpleBean, Row> toRowFunction =
        registry.getToRowFunction(SimpleBean.class);
    SerializableFunction<Row, SimpleBean> fromRowFunction =
        registry.getFromRowFunction(SimpleBean.class);
    SchemaCoder schemaCoder = registry.getSchemaCoder(SimpleBean.class);

    assertTrue(schema.equivalent(schemaCoder.getSchema()));
    assertTrue(toRowFunction.equals(schemaCoder.getToRowFunction()));
    assertTrue(fromRowFunction.equals(schemaCoder.getFromRowFunction()));

    thrown.expect(NoSuchSchemaException.class);
    registry.getSchemaCoder(Double.class);
  }

  @Test
  public void testGetSchemaProvider() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();

    SchemaProvider testDefaultSchemaProvider =
        registry.getSchemaProvider(TestDefaultSchemaClass.class);
    assertEquals(DefaultSchemaProvider.class, testDefaultSchemaProvider.getClass());
    assertEquals(
        TestDefaultSchemaProvider.class,
        ((DefaultSchemaProvider) testDefaultSchemaProvider)
            .getUnderlyingSchemaProvider(TestDefaultSchemaClass.class)
            .getClass());

    SchemaProvider autoValueSchemaProvider = registry.getSchemaProvider(TestAutoValue.class);
    assertEquals(DefaultSchemaProvider.class, autoValueSchemaProvider.getClass());
    assertEquals(
        AutoValueSchema.class,
        ((DefaultSchemaProvider) autoValueSchemaProvider)
            .getUnderlyingSchemaProvider(TestAutoValue.class)
            .getClass());

    SchemaProvider simpleBeanSchemaProvider = registry.getSchemaProvider(SimpleBean.class);
    assertEquals(DefaultSchemaProvider.class, simpleBeanSchemaProvider.getClass());
    assertEquals(
        JavaBeanSchema.class,
        ((DefaultSchemaProvider) simpleBeanSchemaProvider)
            .getUnderlyingSchemaProvider(SimpleBean.class)
            .getClass());
  }

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class TestAutoValue {
    public static Builder builder() {
      return new AutoValue_SchemaRegistryTest_TestAutoValue.Builder();
    }

    public abstract String getStr();

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setStr(String str);

      public abstract TestAutoValue build();
    }
  }
}
