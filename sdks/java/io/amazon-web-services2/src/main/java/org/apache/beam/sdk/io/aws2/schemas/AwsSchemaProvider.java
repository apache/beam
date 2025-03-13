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
package org.apache.beam.sdk.io.aws2.schemas;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets.difference;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets.newHashSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import org.apache.beam.sdk.io.aws2.schemas.AwsSchemaUtils.SdkBuilderSetter;
import org.apache.beam.sdk.io.aws2.schemas.AwsTypes.ConverterFactory;
import org.apache.beam.sdk.schemas.CachingFactory;
import org.apache.beam.sdk.schemas.Factory;
import org.apache.beam.sdk.schemas.FieldValueGetter;
import org.apache.beam.sdk.schemas.FieldValueTypeInformation;
import org.apache.beam.sdk.schemas.GetterBasedSchemaProviderV2;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaUserTypeCreator;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.RowWithGetters;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.awssdk.core.SdkField;
import software.amazon.awssdk.core.SdkPojo;
import software.amazon.awssdk.utils.builder.SdkBuilder;

/**
 * Schema provider for AWS {@link SdkPojo} models using the provided field metadata (@see {@link
 * SdkPojo#sdkFields()}) rather than reflection.
 *
 * <p>Note: Beam doesn't support self-referential schemas. Some AWS models are not compatible with
 * schemas for that reason and require a dedicated coder, such as {@link
 * software.amazon.awssdk.services.dynamodb.model.AttributeValue DynamoDB AttributeValue} ({@link
 * org.apache.beam.sdk.io.aws2.dynamodb.AttributeValueCoder coder}).
 */
public class AwsSchemaProvider extends GetterBasedSchemaProviderV2 {
  /** Byte-code generated {@link SdkBuilder} factories. */
  @SuppressWarnings("rawtypes") // Crashes checker otherwise
  private static final Map<Class, AwsBuilderFactory> FACTORIES = Maps.newConcurrentMap();

  @Override
  public @Nullable <T> Schema schemaFor(TypeDescriptor<T> type) {
    if (!SdkPojo.class.isAssignableFrom(type.getRawType())) {
      return null;
    }
    return AwsTypes.schemaFor(sdkFields((Class<? extends SdkPojo>) type.getRawType()));
  }

  @Override
  public <T> List<FieldValueGetter<@NonNull T, Object>> fieldValueGetters(
      TypeDescriptor<T> targetTypeDescriptor, Schema schema) {
    ConverterFactory fromAws = ConverterFactory.fromAws();
    Map<String, SdkField<?>> sdkFields =
        sdkFieldsByName((Class<? extends SdkPojo>) targetTypeDescriptor.getRawType());
    List<FieldValueGetter<@NonNull T, Object>> getters = new ArrayList<>(schema.getFieldCount());
    for (@NonNull String field : schema.getFieldNames()) {
      SdkField<?> sdkField = checkStateNotNull(sdkFields.get(field), "Unknown field");
      getters.add(
          AwsSchemaUtils.getter(
              field,
              (SerializableFunction<@NonNull T, Object>)
                  fromAws.create(sdkField::getValueOrDefault, sdkField)));
    }
    return getters;
  }

  // Overriding `fromRowFunction` to instead use the generated builder factories with SDK provided
  // setters from `SdkField`s.
  @Override
  public <T> SerializableFunction<Row, T> fromRowFunction(TypeDescriptor<T> type) {
    checkState(SdkPojo.class.isAssignableFrom(type.getRawType()), "Unsupported type %s", type);
    return FromRowFactory.create(type);
  }

  private static class FromRowWithBuilder<T extends SdkPojo>
      implements SerializableFunction<Row, T> {
    private final Class<T> cls;
    private final Factory<List<SdkBuilderSetter>> factory;

    FromRowWithBuilder(Class<T> cls, Factory<List<SdkBuilderSetter>> factory) {
      this.cls = cls;
      this.factory = factory;
    }

    @Override
    @SuppressWarnings("nullness") // checker doesn't recognize the builder type
    public T apply(Row row) {
      if (row instanceof RowWithGetters) {
        Object target = ((RowWithGetters) row).getGetterTarget();
        if (target.getClass().equals(cls)) {
          return (T) target; // simply extract the underlying object instead of creating a new one.
        }
      }
      SdkBuilder<?, T> builder = sdkBuilder(cls);
      List<SdkBuilderSetter> setters = factory.create(TypeDescriptor.of(cls), row.getSchema());
      for (SdkBuilderSetter set : setters) {
        if (!row.getSchema().hasField(set.name())) {
          continue;
        }
        set.set(builder, row.getValue(set.name()));
      }
      return builder.build();
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      FromRowWithBuilder<?> that = (FromRowWithBuilder<?>) o;
      return cls.equals(that.cls);
    }

    @Override
    public int hashCode() {
      return Objects.hash(cls);
    }
  }

  private static class FromRowFactory implements Factory<SerializableFunction<Row, ?>> {
    @SuppressWarnings("nullness") // circular initialization
    private final Factory<SerializableFunction<Row, ?>> cachingFactory = new CachingFactory<>(this);

    private final Factory<List<SdkBuilderSetter>> settersFactory =
        new CachingFactory<>(new SettersFactory());

    @SuppressWarnings("nullness") // schema nullable for this factory
    static <T> SerializableFunction<Row, T> create(TypeDescriptor<? super T> typeDescriptor) {
      checkState(
          SdkPojo.class.isAssignableFrom(typeDescriptor.getRawType()),
          "Unsupported clazz %s",
          typeDescriptor);
      return (SerializableFunction<Row, T>)
          new FromRowFactory().cachingFactory.create(typeDescriptor, null);
    }

    @Override
    public SerializableFunction<Row, ?> create(TypeDescriptor<?> typeDescriptor, Schema ignored) {
      return new FromRowWithBuilder<>(
          (Class<? extends SdkPojo>) typeDescriptor.getRawType(), settersFactory);
    }

    private class SettersFactory implements Factory<List<SdkBuilderSetter>> {
      private final ConverterFactory toAws;

      private SettersFactory() {
        this.toAws = ConverterFactory.toAws(cachingFactory);
      }

      @Override
      public List<SdkBuilderSetter> create(TypeDescriptor<?> typeDescriptor, Schema schema) {
        Map<String, SdkField<?>> fields =
            sdkFieldsByName((Class<? extends SdkPojo>) typeDescriptor.getRawType());
        checkForUnknownFields(schema, fields);

        List<SdkBuilderSetter> setters = new ArrayList<>(schema.getFieldCount());
        for (Entry<String, SdkField<?>> entry : fields.entrySet()) {
          SdkField<?> sdkField = entry.getValue();
          BiConsumer<SdkBuilder<?, ?>, Object> setter =
              toAws.needsConversion(sdkField)
                  ? ConverterFactory.createSetter(sdkField::set, toAws.create(sdkField))
                  : sdkField::set;
          setters.add(AwsSchemaUtils.setter(entry.getKey(), setter));
        }
        return setters;
      }
    }

    private void checkForUnknownFields(Schema schema, Map<String, SdkField<?>> fields) {
      Set<String> unknowns = difference(newHashSet(schema.getFieldNames()), fields.keySet());
      checkState(unknowns.isEmpty(), "Row schema contains unknown fields: %s", unknowns);
    }
  }

  @Override
  public List<FieldValueTypeInformation> fieldValueTypeInformations(
      TypeDescriptor<?> targetTypeDescriptor, Schema schema) {
    throw new UnsupportedOperationException("FieldValueTypeInformation not available");
  }

  @Override
  public SchemaUserTypeCreator schemaTypeCreator(
      TypeDescriptor<?> targetTypeDescriptor, Schema schema) {
    throw new UnsupportedOperationException("SchemaUserTypeCreator not available");
  }

  private static <T extends SdkPojo> AwsBuilderFactory<T, ?> builderFactory(Class<T> cls) {
    return FACTORIES.computeIfAbsent(cls, c -> AwsSchemaUtils.builderFactory(cls));
  }

  private static <T extends SdkPojo> List<SdkField<?>> sdkFields(Class<T> cls) {
    return builderFactory(cls).sdkFields();
  }

  private static <T extends SdkPojo> SdkBuilder<?, T> sdkBuilder(Class<T> cls) {
    return builderFactory(cls).get();
  }

  private static <T extends SdkPojo> Map<String, SdkField<?>> sdkFieldsByName(Class<T> cls) {
    return sdkFields(cls).stream().collect(toMap(AwsTypes::normalizedNameOf, identity()));
  }
}
