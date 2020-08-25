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

import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A {@link SchemaProvider} base class that vends schemas and rows based on {@link
 * FieldValueGetter}s.
 */
@Experimental(Kind.SCHEMAS)
public abstract class GetterBasedSchemaProvider implements SchemaProvider {
  /** Implementing class should override to return FieldValueGetters. */
  public abstract List<FieldValueGetter> fieldValueGetters(Class<?> targetClass, Schema schema);

  /** Implementing class should override to return a list of type-informations. */
  public abstract List<FieldValueTypeInformation> fieldValueTypeInformations(
      Class<?> targetClass, Schema schema);

  /** Implementing class should override to return a constructor. */
  public abstract SchemaUserTypeCreator schemaTypeCreator(Class<?> targetClass, Schema schema);

  private class ToRowWithValueGetters<T> implements SerializableFunction<T, Row> {
    private final Schema schema;
    private final Factory<List<FieldValueGetter>> getterFactory;

    public ToRowWithValueGetters(Schema schema) {
      this.schema = schema;
      // Since we know that this factory is always called from inside the lambda with the same
      // schema,
      // return a caching factory that caches the first value seen for each class. This prevents
      // having to lookup the getter list each time createGetters is called.
      this.getterFactory = new CachingFactory<>(GetterBasedSchemaProvider.this::fieldValueGetters);
    }

    @Override
    public Row apply(T input) {
      return Row.withSchema(schema).withFieldValueGetters(getterFactory, input);
    }

    private GetterBasedSchemaProvider getOuter() {
      return GetterBasedSchemaProvider.this;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ToRowWithValueGetters<?> that = (ToRowWithValueGetters<?>) o;
      return getOuter().equals(that.getOuter()) && schema.equals(that.schema);
    }

    @Override
    public int hashCode() {
      return Objects.hash(GetterBasedSchemaProvider.this, schema);
    }
  }

  @Override
  public <T> SerializableFunction<T, Row> toRowFunction(TypeDescriptor<T> typeDescriptor) {
    // schemaFor is non deterministic - it might return fields in an arbitrary order. The reason
    // why is that Java reflection does not guarantee the order in which it returns fields and
    // methods, and these schemas are often based on reflective analysis of classes. Therefore it's
    // important to capture the schema once here, so all invocations of the toRowFunction see the
    // same version of the schema. If schemaFor were to be called inside the lambda below, different
    // workers would see different versions of the schema.
    Schema schema = schemaFor(typeDescriptor);

    return new ToRowWithValueGetters<>(schema);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> SerializableFunction<Row, T> fromRowFunction(TypeDescriptor<T> typeDescriptor) {
    Class<T> clazz = (Class<T>) typeDescriptor.getType();
    return new FromRowUsingCreator<>(clazz, this);
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  @Override
  public boolean equals(@Nullable Object obj) {
    return obj != null && this.getClass() == obj.getClass();
  }
}
