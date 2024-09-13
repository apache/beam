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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.LogicalType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.apache.beam.sdk.schemas.logicaltypes.OneOfType;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Verify;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Collections2;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.checkerframework.checker.initialization.qual.NotOnlyInitialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A {@link SchemaProvider} base class that vends schemas and rows based on {@link
 * FieldValueGetter}s.
 *
 * @deprecated new implementations should extend the {@link GetterBasedSchemaProviderV2} class'
 *     methods which receive {@link TypeDescriptor}s instead of ordinary {@link Class}es as
 *     arguments, which permits to support generic type signatures during schema inference
 */
@SuppressWarnings({"rawtypes"})
@Deprecated
public abstract class GetterBasedSchemaProvider implements SchemaProvider {

  /**
   * Implementing class should override to return FieldValueGetters.
   *
   * @deprecated new implementations should override {@link #fieldValueGetters(TypeDescriptor,
   *     Schema)} and make this method throw an {@link UnsupportedOperationException}
   */
  @Deprecated
  public abstract List<FieldValueGetter> fieldValueGetters(Class<?> targetClass, Schema schema);

  /**
   * Delegates to the {@link #fieldValueGetters(Class, Schema)} for backwards compatibility,
   * override it if you want to use the richer type signature contained in the {@link
   * TypeDescriptor} not subject to the type erasure.
   */
  public <T> List<FieldValueGetter<@NonNull T, Object>> fieldValueGetters(
      TypeDescriptor<T> targetTypeDescriptor, Schema schema) {
    return (List) fieldValueGetters(targetTypeDescriptor.getRawType(), schema);
  }

  /**
   * Implementing class should override to return a list of type-informations.
   *
   * @deprecated new implementations should override {@link
   *     #fieldValueTypeInformations(TypeDescriptor, Schema)} and make this method throw an {@link
   *     UnsupportedOperationException}
   */
  @Deprecated
  public abstract List<FieldValueTypeInformation> fieldValueTypeInformations(
      Class<?> targetClass, Schema schema);

  /**
   * Delegates to the {@link #fieldValueTypeInformations(Class, Schema)} for backwards
   * compatibility, override it if you want to use the richer type signature contained in the {@link
   * TypeDescriptor} not subject to the type erasure.
   */
  public List<FieldValueTypeInformation> fieldValueTypeInformations(
      TypeDescriptor<?> targetTypeDescriptor, Schema schema) {
    return fieldValueTypeInformations(targetTypeDescriptor.getRawType(), schema);
  }

  /**
   * Implementing class should override to return a constructor.
   *
   * @deprecated new implementations should override {@link #schemaTypeCreator(TypeDescriptor,
   *     Schema)} and make this method throw an {@link UnsupportedOperationException}
   */
  @Deprecated
  public abstract SchemaUserTypeCreator schemaTypeCreator(Class<?> targetClass, Schema schema);

  /**
   * Delegates to the {@link #schemaTypeCreator(Class, Schema)} for backwards compatibility,
   * override it if you want to use the richer type signature contained in the {@link
   * TypeDescriptor} not subject to the type erasure.
   */
  public SchemaUserTypeCreator schemaTypeCreator(
      TypeDescriptor<?> targetTypeDescriptor, Schema schema) {
    return schemaTypeCreator(targetTypeDescriptor.getRawType(), schema);
  }

  private class ToRowWithValueGetters<T extends @NonNull Object>
      implements SerializableFunction<T, Row> {
    private final Schema schema;
    private final Factory<List<FieldValueGetter<?, ?>>> getterFactory;

    public ToRowWithValueGetters(Schema schema) {
      this.schema = schema;
      // Since we know that this factory is always called from inside the lambda with the same
      // schema, return a caching factory that caches the first value seen for each class. This
      // prevents having to lookup the getter list each time createGetters is called.
      this.getterFactory =
          RowValueGettersFactory.of(
              (Factory<List<FieldValueGetter<?, ?>>>)
                  (typeDescriptor, schema1) ->
                      (List)
                          GetterBasedSchemaProvider.this.fieldValueGetters(
                              typeDescriptor, schema1));
    }

    @Override
    public Row apply(T input) {
      return Row.withSchema(schema).withFieldValueGetters((Factory) getterFactory, input);
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
    @NonNull
    Schema schema =
        Verify.verifyNotNull(
            schemaFor(typeDescriptor), "can't create a ToRowFunction with null schema");

    return new ToRowWithValueGetters<>(schema);
  }

  @Override
  public <T> SerializableFunction<Row, T> fromRowFunction(TypeDescriptor<T> typeDescriptor) {
    return new FromRowUsingCreator<>(typeDescriptor, this);
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  @Override
  public boolean equals(@Nullable Object obj) {
    return obj != null && this.getClass() == obj.getClass();
  }

  private static class RowValueGettersFactory implements Factory<List<FieldValueGetter<?, ?>>> {
    private final Factory<List<FieldValueGetter<?, ?>>> gettersFactory;
    private final @NotOnlyInitialized Factory<List<FieldValueGetter<?, ?>>> cachingGettersFactory;

    static Factory<List<FieldValueGetter<?, ?>>> of(
        Factory<List<FieldValueGetter<?, ?>>> gettersFactory) {
      return new RowValueGettersFactory(gettersFactory).cachingGettersFactory;
    }

    RowValueGettersFactory(Factory<List<FieldValueGetter<?, ?>>> gettersFactory) {
      this.gettersFactory = gettersFactory;
      this.cachingGettersFactory = new CachingFactory<>(this);
    }

    @Override
    public List<FieldValueGetter<?, ?>> create(TypeDescriptor<?> typeDescriptor, Schema schema) {
      List<FieldValueGetter<?, ?>> getters = gettersFactory.create(typeDescriptor, schema);
      List<FieldValueGetter<?, ?>> rowGetters = new ArrayList<>(getters.size());
      for (int i = 0; i < getters.size(); i++) {
        rowGetters.add(rowValueGetter(getters.get(i), schema.getField(i).getType()));
      }
      return rowGetters;
    }

    static boolean needsConversion(FieldType type) {
      TypeName typeName = type.getTypeName();
      return typeName.equals(TypeName.ROW)
          || typeName.isLogicalType()
          || ((typeName.equals(TypeName.ARRAY) || typeName.equals(TypeName.ITERABLE))
              && needsConversion(Verify.verifyNotNull(type.getCollectionElementType())))
          || (typeName.equals(TypeName.MAP)
              && (needsConversion(Verify.verifyNotNull(type.getMapKeyType()))
                  || needsConversion(Verify.verifyNotNull(type.getMapValueType()))));
    }

    FieldValueGetter<?, ?> rowValueGetter(FieldValueGetter base, FieldType type) {
      TypeName typeName = type.getTypeName();
      if (!needsConversion(type)) {
        return base;
      }
      if (typeName.equals(TypeName.ROW)) {
        return new GetRow(base, Verify.verifyNotNull(type.getRowSchema()), cachingGettersFactory);
      } else if (typeName.equals(TypeName.ARRAY)) {
        FieldType elementType = Verify.verifyNotNull(type.getCollectionElementType());
        return elementType.getTypeName().equals(TypeName.ROW)
            ? new GetEagerCollection(base, converter(elementType))
            : new GetCollection(base, converter(elementType));
      } else if (typeName.equals(TypeName.ITERABLE)) {
        return new GetIterable(
            base, converter(Verify.verifyNotNull(type.getCollectionElementType())));
      } else if (typeName.equals(TypeName.MAP)) {
        return new GetMap(
            base,
            converter(Verify.verifyNotNull(type.getMapKeyType())),
            converter(Verify.verifyNotNull(type.getMapValueType())));
      } else if (type.isLogicalType(OneOfType.IDENTIFIER)) {
        OneOfType oneOfType = type.getLogicalType(OneOfType.class);
        Schema oneOfSchema = oneOfType.getOneOfSchema();
        Map<String, Integer> values = oneOfType.getCaseEnumType().getValuesMap();

        Map<Integer, FieldValueGetter<?, ?>> converters =
            Maps.newHashMapWithExpectedSize(values.size());
        for (Map.Entry<String, Integer> kv : values.entrySet()) {
          FieldType fieldType = oneOfSchema.getField(kv.getKey()).getType();
          FieldValueGetter<?, ?> converter = converter(fieldType);
          converters.put(kv.getValue(), converter);
        }

        return new GetOneOf(base, converters, oneOfType);
      } else if (typeName.isLogicalType()) {
        return new GetLogicalInputType(base, Verify.verifyNotNull(type.getLogicalType()));
      }
      return base;
    }

    FieldValueGetter<?, ?> converter(FieldType type) {
      return rowValueGetter(IDENTITY, type);
    }

    static class GetRow<T extends @NonNull Object, V extends @NonNull Object>
        extends Converter<T, V> {
      final Schema schema;
      final Factory<List<FieldValueGetter<V, ?>>> factory;

      GetRow(
          FieldValueGetter<T, V> getter,
          Schema schema,
          Factory<List<FieldValueGetter<V, ?>>> factory) {
        super(getter);
        this.schema = schema;
        this.factory = factory;
      }

      @Override
      Object convert(V value) {
        return Row.withSchema(schema).withFieldValueGetters(factory, value);
      }
    }

    static class GetEagerCollection<T extends @NonNull Object> extends Converter<T, Collection> {
      final FieldValueGetter converter;

      GetEagerCollection(FieldValueGetter<T, Collection> getter, FieldValueGetter converter) {
        super(getter);
        this.converter = converter;
      }

      @Override
      Object convert(Collection collection) {
        List newList = new ArrayList(collection.size());
        for (Object obj : collection) {
          newList.add(converter.get(obj));
        }
        return newList;
      }
    }

    static class GetCollection<T extends @NonNull Object> extends Converter<T, Collection> {
      final FieldValueGetter converter;

      GetCollection(FieldValueGetter<T, Collection> getter, FieldValueGetter converter) {
        super(getter);
        this.converter = converter;
      }

      @Override
      @SuppressWarnings({"nullness"})
      Object convert(Collection collection) {
        if (collection instanceof List) {
          // For performance reasons if the input is a list, make sure that we produce a list.
          // Otherwise, Row forwarding is forced to physically copy the collection into a new List
          // object.
          return Lists.transform((List) collection, converter::get);
        } else {
          return Collections2.transform(collection, converter::get);
        }
      }
    }

    static class GetIterable<T extends @NonNull Object> extends Converter<T, Iterable> {
      final FieldValueGetter converter;

      GetIterable(FieldValueGetter<T, Iterable> getter, FieldValueGetter converter) {
        super(getter);
        this.converter = converter;
      }

      @Override
      @SuppressWarnings({"nullness"})
      Object convert(Iterable value) {
        return Iterables.transform(value, converter::get);
      }
    }

    static class GetMap<T extends @NonNull Object, K1, K2, V1, V2>
        extends Converter<T, Map<@Nullable K1, @Nullable V1>> {
      final FieldValueGetter<@NonNull K1, K2> keyConverter;
      final FieldValueGetter<@NonNull V1, V2> valueConverter;

      GetMap(
          FieldValueGetter<T, Map<@Nullable K1, @Nullable V1>> getter,
          FieldValueGetter<@NonNull K1, K2> keyConverter,
          FieldValueGetter<@NonNull V1, V2> valueConverter) {
        super(getter);
        this.keyConverter = keyConverter;
        this.valueConverter = valueConverter;
      }

      @Override
      Map<@Nullable K2, @Nullable V2> convert(Map<@Nullable K1, @Nullable V1> value) {
        Map<@Nullable K2, @Nullable V2> returnMap = Maps.newHashMapWithExpectedSize(value.size());
        for (Map.Entry<@Nullable K1, @Nullable V1> entry : value.entrySet()) {
          returnMap.put(
              Optional.ofNullable(entry.getKey()).map(keyConverter::get).orElse(null),
              Optional.ofNullable(entry.getValue()).map(valueConverter::get).orElse(null));
        }
        return returnMap;
      }
    }

    static class GetLogicalInputType<T extends @NonNull Object> extends Converter<T, Object> {
      final LogicalType logicalType;

      GetLogicalInputType(FieldValueGetter<T, Object> getter, LogicalType logicalType) {
        super(getter);
        this.logicalType = logicalType;
      }

      @Override
      Object convert(Object value) {
        // Getters are assumed to return the base type.
        return logicalType.toInputType(value);
      }
    }

    static class GetOneOf<T extends @NonNull Object> extends Converter<T, OneOfType.Value> {
      final OneOfType oneOfType;
      final Map<Integer, FieldValueGetter<@NonNull Object, Object>> converters;

      GetOneOf(
          FieldValueGetter<T, OneOfType.Value> getter,
          Map<Integer, FieldValueGetter<@NonNull Object, Object>> converters,
          OneOfType oneOfType) {
        super(getter);
        this.converters = converters;
        this.oneOfType = oneOfType;
      }

      @Override
      Object convert(OneOfType.Value value) {
        EnumerationType.Value caseType = value.getCaseType();

        @NonNull
        FieldValueGetter<@NonNull Object, Object> converter =
            Verify.verifyNotNull(
                converters.get(caseType.getValue()),
                "Missing OneOf converter for case %s.",
                caseType);

        return oneOfType.createValue(caseType, converter.get(value.getValue()));
      }
    }

    abstract static class Converter<ObjectT extends @NonNull Object, ValueT>
        implements FieldValueGetter<ObjectT, Object> {
      final FieldValueGetter<ObjectT, ValueT> getter;

      public Converter(FieldValueGetter<ObjectT, ValueT> getter) {
        this.getter = getter;
      }

      abstract Object convert(ValueT value);

      @Override
      public @Nullable Object get(ObjectT object) {
        ValueT value = getter.get(object);
        if (value == null) {
          return null;
        }
        return convert(value);
      }

      @Override
      public @Nullable Object getRaw(ObjectT object) {
        return getter.getRaw(object);
      }

      @Override
      public String name() {
        return getter.name();
      }
    }

    private static final FieldValueGetter<@NonNull Object, Object> IDENTITY =
        new FieldValueGetter<@NonNull Object, Object>() {
          @Override
          public Object get(@NonNull Object object) {
            return object;
          }

          @Override
          public String name() {
            return "IDENTITY";
          }
        };
  }
}
