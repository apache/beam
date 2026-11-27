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

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.LogicalType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.apache.beam.sdk.schemas.logicaltypes.OneOfType;
import org.apache.beam.sdk.schemas.utils.ByteBuddyUtils;
import org.apache.beam.sdk.schemas.utils.ReflectUtils;
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

  class ToRowWithValueGetters<T extends @NonNull Object> implements SerializableFunction<T, Row> {
    private final Schema schema;
    private final Factory<List<FieldValueGetter<T, Object>>> getterFactory;
    private final TypeDescriptor getterTargetType;

    @SuppressFBWarnings("SE_TRANSIENT_FIELD_NOT_RESTORED")
    private transient volatile @Nullable List<FieldValueGetter<T, Object>> cachedGetters;

    public ToRowWithValueGetters(Schema schema, TypeDescriptor getterTargetType) {
      this.schema = schema;
      this.getterTargetType = getterTargetType;
      // Since we know that this factory is always called from inside the lambda with the same
      // schema, return a caching factory that caches the first value seen for each class. This
      // prevents having to lookup the getter list each time createGetters is called.
      this.getterFactory =
          RowValueGettersFactory.of(
              (Factory<List<FieldValueGetter<T, Object>>>)
                  (typeDescriptor, schema1) ->
                      (List)
                          GetterBasedSchemaProvider.this.fieldValueGetters(typeDescriptor, schema1),
              GetterBasedSchemaProvider.this::fieldValueTypeInformations);
    }

    @Override
    public Row apply(T input) {
      List<FieldValueGetter<T, Object>> getters = cachedGetters;
      if (getters == null) {
        getters = getterFactory.create(getterTargetType, schema);
        cachedGetters = getters;
      }
      return Row.withFieldValueGetters(schema, getters, input, getterTargetType);
    }

    GetterBasedSchemaProvider getOuter() {
      return GetterBasedSchemaProvider.this;
    }

    Schema getSchema() {
      return schema;
    }

    TypeDescriptor getTargetType() {
      return getterTargetType;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof ToRowWithValueGetters)) {
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
    @NonNull Schema schema =
        Verify.verifyNotNull(
            schemaFor(typeDescriptor), "can't create a ToRowFunction with null schema");

    return new ToRowWithValueGetters<>(schema, typeDescriptor);
  }

  @Override
  public <T> SerializableFunction<Row, T> fromRowFunction(TypeDescriptor<T> typeDescriptor) {
    return new FromRowUsingCreator<>(typeDescriptor, this);
  }

  @Override
  public int hashCode() {
    return getClass().hashCode();
  }

  @Override
  @SuppressWarnings("EqualsGetClass")
  public boolean equals(@Nullable Object obj) {
    return obj != null && this.getClass() == obj.getClass();
  }

  private static class RowValueGettersFactory<T extends @NonNull Object>
      implements Factory<List<FieldValueGetter<T, Object>>> {
    private final Factory<List<FieldValueGetter<T, Object>>> gettersFactory;
    private final Factory<List<FieldValueTypeInformation>> typeInfoFactory;
    private final @NotOnlyInitialized Factory<List<FieldValueGetter<T, Object>>>
        cachingGettersFactory;

    static <T extends @NonNull Object> Factory<List<FieldValueGetter<T, Object>>> of(
        Factory<List<FieldValueGetter<T, Object>>> gettersFactory,
        Factory<List<FieldValueTypeInformation>> typeInfoFactory) {
      return new RowValueGettersFactory(gettersFactory, typeInfoFactory).cachingGettersFactory;
    }

    RowValueGettersFactory(
        Factory<List<FieldValueGetter<T, Object>>> gettersFactory,
        Factory<List<FieldValueTypeInformation>> typeInfoFactory) {
      this.gettersFactory = gettersFactory;
      this.typeInfoFactory = typeInfoFactory;
      this.cachingGettersFactory = new CachingFactory<>(this);
    }

    @Override
    public List<FieldValueGetter<T, Object>> create(
        TypeDescriptor<?> typeDescriptor, Schema schema) {
      List<FieldValueGetter<T, Object>> getters = gettersFactory.create(typeDescriptor, schema);
      Map<String, FieldValueTypeInformation> typeInfoByName =
          typeInfoFactory.create(typeDescriptor, schema).stream()
              .collect(Collectors.toMap(FieldValueTypeInformation::getName, Function.identity()));
      List<FieldValueGetter<T, Object>> rowGetters = new ArrayList<>(getters.size());
      for (int i = 0; i < getters.size(); i++) {
        FieldValueGetter getter = Verify.verifyNotNull(getters.get(i));
        rowGetters.add(
            rowValueGetter(
                getter,
                schema.getField(i).getType(),
                Verify.verifyNotNull(typeInfoByName.get(getter.name())).getType()));
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

    FieldValueGetter<T, Object> rowValueGetter(
        FieldValueGetter base, FieldType type, @Nullable TypeDescriptor<?> getterReturnType) {
      TypeName typeName = type.getTypeName();
      if (!needsConversion(type)) {
        return base;
      }
      if (typeName.equals(TypeName.ROW)) {
        return new GetRow(
            base,
            getterReturnType,
            Verify.verifyNotNull(type.getRowSchema()),
            cachingGettersFactory);
      } else if (typeName.equals(TypeName.ARRAY) || typeName.equals(TypeName.ITERABLE)) {
        FieldType elementType = Verify.verifyNotNull(type.getCollectionElementType());
        TypeDescriptor<?> elementTypeDescriptor =
            Optional.ofNullable(getterReturnType)
                .map(ReflectUtils::getIterableComponentType)
                .orElse(null);
        if (TypeName.ARRAY == typeName) {
          return TypeName.ROW == elementType.getTypeName()
              ? new GetEagerCollection(base, converter(elementType, elementTypeDescriptor))
              : new GetCollection(base, converter(elementType, elementTypeDescriptor));
        } else { // TypeName.ITERABLE
          return new GetIterable(base, converter(elementType, elementTypeDescriptor));
        }
      } else if (typeName.equals(TypeName.MAP)) {
        @Nullable TypeDescriptor[] resolvedKeyValueTypes =
            Optional.ofNullable(getterReturnType)
                .<@Nullable TypeDescriptor[]>map(
                    getterType ->
                        Arrays.stream(Map.class.getTypeParameters())
                            .<@Nullable TypeDescriptor>map(
                                typeVar -> {
                                  TypeDescriptor resolved = getterType.resolveType(typeVar);
                                  return resolved.hasUnresolvedParameters() ? null : resolved;
                                })
                            .<@Nullable TypeDescriptor>toArray(TypeDescriptor[]::new))
                .orElse(new TypeDescriptor[] {null, null});
        return new GetMap(
            base,
            converter(Verify.verifyNotNull(type.getMapKeyType()), resolvedKeyValueTypes[0]),
            converter(Verify.verifyNotNull(type.getMapValueType()), resolvedKeyValueTypes[1]));
      } else if (type.isLogicalType(OneOfType.IDENTIFIER)) {
        OneOfType oneOfType = type.getLogicalType(OneOfType.class);
        Schema oneOfSchema = oneOfType.getOneOfSchema();
        Map<String, Integer> values = oneOfType.getCaseEnumType().getValuesMap();

        Map<Integer, FieldValueGetter<?, ?>> converters =
            Maps.newHashMapWithExpectedSize(values.size());
        for (Map.Entry<String, Integer> kv : values.entrySet()) {
          FieldType fieldType = oneOfSchema.getField(kv.getKey()).getType();
          FieldValueGetter<?, ?> converter = converter(fieldType, null);
          converters.put(kv.getValue(), converter);
        }

        return new GetOneOf(base, converters, oneOfType);
      } else if (typeName.isLogicalType()) {
        LogicalType<?, ?> logicalType = Verify.verifyNotNull(type.getLogicalType());
        if (base instanceof ByteBuddyUtils.DirectLogicalTypeGetter
            && getterReturnType != null
            && ByteBuddyUtils.matchesInferredLogicalType(
                logicalType, getterReturnType.getRawType())) {
          return new GetRawPassThrough<>(base);
        }
        return new GetLogicalInputType(base, logicalType);
      }
      return base;
    }

    FieldValueGetter<?, ?> converter(FieldType type, @Nullable TypeDescriptor<?> getterReturnType) {
      return rowValueGetter(IDENTITY, type, getterReturnType);
    }

    static class GetRow<T extends @NonNull Object, V extends @NonNull Object>
        extends Converter<T, V> {
      final Schema schema;
      final Factory<List<FieldValueGetter<V, Object>>> factory;
      final @Nullable TypeDescriptor<?> valueType;
      transient volatile @Nullable List<FieldValueGetter<V, Object>> cachedGetters;

      GetRow(
          FieldValueGetter<T, V> getter,
          @Nullable TypeDescriptor<?> getterReturnType,
          Schema schema,
          Factory<List<FieldValueGetter<V, Object>>> factory) {
        super(getter);
        this.schema = schema;
        this.factory = factory;
        this.valueType = getterReturnType;
      }

      @Override
      Object convert(V value) {
        TypeDescriptor<?> typeDescriptor = valueType;
        if (typeDescriptor != null) {
          List<FieldValueGetter<V, Object>> getters = cachedGetters;
          if (getters == null) {
            getters = factory.create(typeDescriptor, schema);
            cachedGetters = getters;
          }
          return Row.withFieldValueGetters(schema, getters, value, typeDescriptor);
        }
        return Row.withFieldValueGetters(
            schema, factory, value, TypeDescriptor.of(value.getClass()));
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
      final org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Function convertFn;

      @SuppressWarnings("nullness")
      GetCollection(FieldValueGetter<T, Collection> getter, FieldValueGetter converter) {
        super(getter);
        this.convertFn = converter::get;
      }

      @Override
      @SuppressWarnings({"nullness"})
      Object convert(Collection collection) {
        if (collection instanceof List) {
          // For performance reasons if the input is a list, make sure that we produce a list.
          // Otherwise, Row forwarding is forced to physically copy the collection into a new List
          // object.
          return Lists.transform((List) collection, convertFn);
        } else {
          return Collections2.transform(collection, convertFn);
        }
      }
    }

    static class GetIterable<T extends @NonNull Object> extends Converter<T, Iterable> {
      final org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Function convertFn;

      @SuppressWarnings("nullness")
      GetIterable(FieldValueGetter<T, Iterable> getter, FieldValueGetter converter) {
        super(getter);
        this.convertFn = converter::get;
      }

      @Override
      @SuppressWarnings({"nullness"})
      Object convert(Iterable value) {
        return Iterables.transform(value, convertFn);
      }
    }

    static class GetMap<T extends @NonNull Object, K1, K2, V1, V2>
        extends Converter<T, Map<@Nullable K1, @Nullable V1>> {
      final FieldValueGetter<@NonNull K1, K2> keyConverter;
      final FieldValueGetter<@NonNull V1, V2> valueConverter;
      final boolean convertKey;
      final boolean convertValue;

      GetMap(
          FieldValueGetter<T, Map<@Nullable K1, @Nullable V1>> getter,
          FieldValueGetter<@NonNull K1, K2> keyConverter,
          FieldValueGetter<@NonNull V1, V2> valueConverter) {
        super(getter);
        this.keyConverter = keyConverter;
        this.valueConverter = valueConverter;
        this.convertKey = keyConverter != IDENTITY;
        this.convertValue = valueConverter != IDENTITY;
      }

      @Override
      @SuppressWarnings("unchecked")
      Map<@Nullable K2, @Nullable V2> convert(Map<@Nullable K1, @Nullable V1> value) {
        if (!convertKey && !convertValue) {
          return (Map<@Nullable K2, @Nullable V2>) value;
        }
        Map<@Nullable K2, @Nullable V2> returnMap = Maps.newHashMapWithExpectedSize(value.size());
        for (Map.Entry<@Nullable K1, @Nullable V1> entry : value.entrySet()) {
          @Nullable K1 k = entry.getKey();
          @Nullable V1 v = entry.getValue();
          @Nullable K2 outKey = (k == null || !convertKey) ? (K2) k : keyConverter.get(k);
          @Nullable V2 outValue = (v == null || !convertValue) ? (V2) v : valueConverter.get(v);
          returnMap.put(outKey, outValue);
        }
        return returnMap;
      }
    }

    static class GetRawPassThrough<T extends @NonNull Object>
        implements FieldValueGetter<T, Object> {
      final FieldValueGetter<T, ?> getter;

      GetRawPassThrough(FieldValueGetter<T, ?> getter) {
        this.getter = getter;
      }

      @Override
      public @Nullable Object get(T object) {
        return getter.getRaw(object);
      }

      @Override
      public @Nullable Object getRaw(T object) {
        return getter.getRaw(object);
      }

      @Override
      public String name() {
        return getter.name();
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
      private static final int MAX_DENSE_ENUM_ID = 256;
      final OneOfType oneOfType;
      final Map<Integer, FieldValueGetter<@NonNull Object, Object>> converters;
      final @Nullable FieldValueGetter<@NonNull Object, Object> @Nullable [] denseConverters;

      @SuppressWarnings({"unchecked", "rawtypes"})
      GetOneOf(
          FieldValueGetter<T, OneOfType.Value> getter,
          Map<Integer, FieldValueGetter<@NonNull Object, Object>> converters,
          OneOfType oneOfType) {
        super(getter);
        this.converters = converters;
        this.oneOfType = oneOfType;
        int min = Integer.MAX_VALUE;
        int max = Integer.MIN_VALUE;
        for (Integer id : converters.keySet()) {
          int v = id;
          if (v < min) {
            min = v;
          }
          if (v > max) {
            max = v;
          }
        }
        if (min >= 0 && max >= 0 && max < MAX_DENSE_ENUM_ID && !converters.isEmpty()) {
          @Nullable FieldValueGetter<@NonNull Object, Object>[] dense =
              new FieldValueGetter[max + 1];
          for (Map.Entry<Integer, FieldValueGetter<@NonNull Object, Object>> entry :
              converters.entrySet()) {
            dense[entry.getKey()] = entry.getValue();
          }
          this.denseConverters = dense;
        } else {
          this.denseConverters = null;
        }
      }

      @Override
      Object convert(OneOfType.Value value) {
        EnumerationType.Value caseType = value.getCaseType();
        int id = caseType.getValue();
        @Nullable FieldValueGetter<@NonNull Object, Object> converter;
        @Nullable FieldValueGetter<@NonNull Object, Object> @Nullable [] dense = denseConverters;
        if (dense != null) {
          converter = (id >= 0 && id < dense.length) ? dense[id] : null;
        } else {
          converter = converters.get(id);
        }
        @NonNull FieldValueGetter<@NonNull Object, Object> nonNullConverter =
            checkStateNotNull(converter, "Missing OneOf converter for case %s.", caseType);
        Object rawValue = value.getValue();
        if (nonNullConverter == IDENTITY) {
          return value;
        }
        Object convertedValue =
            checkStateNotNull(
                nonNullConverter.get(rawValue),
                "Bug! converting a non-null value in a OneOf resulted in null result value");
        if (convertedValue == rawValue) {
          return value;
        }
        return oneOfType.createValue(caseType, convertedValue);
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
