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
package org.apache.beam.sdk.io.thrift;

import static java.util.Collections.unmodifiableMap;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.schemas.FieldValueGetter;
import org.apache.beam.sdk.schemas.FieldValueTypeInformation;
import org.apache.beam.sdk.schemas.GetterBasedSchemaProviderV2;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.SchemaProvider;
import org.apache.beam.sdk.schemas.SchemaUserTypeCreator;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.apache.beam.sdk.schemas.logicaltypes.OneOfType;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.thrift.TBase;
import org.apache.thrift.TEnum;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.TFieldRequirementType;
import org.apache.thrift.TUnion;
import org.apache.thrift.meta_data.EnumMetaData;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.meta_data.FieldValueMetaData;
import org.apache.thrift.meta_data.ListMetaData;
import org.apache.thrift.meta_data.MapMetaData;
import org.apache.thrift.meta_data.SetMetaData;
import org.apache.thrift.meta_data.StructMetaData;
import org.apache.thrift.protocol.TType;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Schema provider for generated thrift types.
 *
 * <ul>
 *   <li>Primitive type mapping is straight-forward (e.g. {@link TType#I32} -> {@link
 *       FieldType#INT32}).
 *   <li>{@link TType#STRING} gets mapped as either {@link FieldType#STRING} or {@link
 *       FieldType#BYTES}, depending on whether the {@link FieldValueMetaData#isBinary()} flag is
 *       set.
 *   <li>{@link TType#MAP} becomes {@link FieldType#map(FieldType, FieldType) a beam map} passing
 *       the key and value types recursively.
 *   <li>{@link TType#SET} gets translated into a beam {@link FieldType#iterable(FieldType)
 *       iterable}, passing the corresponding element type.
 *   <li>{@link TType#LIST} becomes an {@link FieldType#array(FieldType) array} of the corresponding
 *       element type.
 *   <li>{@link TType#ENUM thrift enums} are converted into {@link EnumerationType beam enumeration
 *       types}.
 *   <li>{@link TUnion thrift union} types get mapped to {@link OneOfType beam one-of} types.
 * </ul>
 *
 * <p>The mapping logic relies on the available {@link FieldMetaData thrift metadata} introspection
 * and tries to make as few assumptions about the generated code as possible (i.e. does not rely on
 * accessor naming convention, as the thrift compiler supports options such as "beans" or
 * "fullcamel"/"nocamel".<br>
 * However, the following strong assumptions are made by this class:
 *
 * <ul>
 *   <li>All thrift generated classes implement {@link TBase}, except for enums which become {@link
 *       Enum java enums} implementing {@link TEnum}.
 *   <li>All {@link TUnion} types provide static factory methods for each of the supported field
 *       types, with the same name as the field itself and only one such method taking a single
 *       parameter exists.
 *   <li>All non-union types have a corresponding java field with the same name for every field in
 *       the original thrift source file.
 * </ul>
 *
 * <p>Thrift typedefs for container types (and possibly others) do not preserve the full type
 * information. For this reason, this class allows for {@link #custom() manual registration} of such
 * "lossy" typedefs with their corresponding beam types.
 *
 * <p>Note: Thrift encoding and decoding are not fully symmetrical, i.e. the {@link
 * TBase#isSet(TFieldIdEnum) isSet} flag may not be preserved upon converting a thrift object to a
 * beam row and back. On encoding, we extract all thrift values, no matter if the fields are set or
 * not. On decoding, we set all non-{@code null} beam row values to the corresponding thrift fields,
 * leaving the rest unset.
 */
public final class ThriftSchema extends GetterBasedSchemaProviderV2 {
  private static final ThriftSchema defaultProvider = new ThriftSchema(Collections.emptyMap());

  private final Map<String, FieldType> typedefs;

  private ThriftSchema(Map<String, FieldType> typedefs) {
    this.typedefs = typedefs;
  }

  /**
   * Schema provider that maps any thrift type to a Beam schema, assuming that any typedefs that
   * might have been used in the thrift definitions will preserve all required metadata to infer the
   * beam type (which is the case for any primitive typedefs and alike).
   *
   * @see #custom() for how to manually pass the beam type for container typedefs
   */
  public static @NonNull SchemaProvider provider() {
    return defaultProvider;
  }

  /**
   * Builds a schema provider that maps any thrift type to a Beam schema, allowing for custom thrift
   * typedef entries (which cannot be resolved using the available metadata) to be manually
   * registered with their corresponding beam types.
   *
   * <p>E.g. {@code typedef set<string> StringSet} will not carry the element type information and
   * needs to be manually mapped here as {@code .custom().withTypedef("StringSet",
   * FieldType.iterable(FieldType.STRING)).provider()}.
   */
  public static @NonNull Customizer custom() {
    return new Customizer();
  }

  public static final class Customizer {
    private final Map<String, FieldType> typedefs = new HashMap<>();

    private Customizer() {}

    public @NonNull Customizer typedef(
        @NonNull String thriftTypedefName, @NonNull FieldType beamType) {
      typedefs.put(thriftTypedefName, beamType);
      return this;
    }

    public @NonNull SchemaProvider provider() {
      if (typedefs.isEmpty()) {
        return defaultProvider;
      } else {
        return new ThriftSchema(unmodifiableMap(new HashMap<>(typedefs)));
      }
    }
  }

  @Override
  public <T> @NonNull Schema schemaFor(TypeDescriptor<T> typeDescriptor) {
    return schemaFor(typeDescriptor.getRawType());
  }

  private Schema schemaFor(Class<?> targetClass) {
    if (!TBase.class.isAssignableFrom(targetClass)) {
      throw new IllegalArgumentException("Expected thrift class but got: " + targetClass);
    }
    final Stream<Schema.Field> fields =
        thriftFieldDescriptors(targetClass).values().stream().map(this::beamField);
    if (TUnion.class.isAssignableFrom(targetClass)) {
      return OneOfType.create(fields.collect(Collectors.toList())).getOneOfSchema();
    } else {
      return fields
          .reduce(Schema.builder(), Schema.Builder::addField, ThriftSchema::throwingCombiner)
          .build();
    }
  }

  private static <X> X throwingCombiner(X lhs, X rhs) {
    throw new IllegalStateException();
  }

  private Schema.Field beamField(FieldMetaData fieldDescriptor) {
    try {
      final FieldType type = beamType(fieldDescriptor.valueMetaData);
      switch (fieldDescriptor.requirementType) {
        case TFieldRequirementType.REQUIRED:
          return Schema.Field.of(fieldDescriptor.fieldName, type);
        case TFieldRequirementType.DEFAULT:
          // aka "opt-in, req-out", so it's safest to fall through to nullable
        case TFieldRequirementType.OPTIONAL:
        default:
          return Schema.Field.nullable(fieldDescriptor.fieldName, type);
      }
    } catch (Exception e) {
      throw new IllegalStateException(
          "Could not infer beam type for thrift field: " + fieldDescriptor.fieldName, e);
    }
  }

  @SuppressWarnings("rawtypes")
  @Override
  public @NonNull List<FieldValueGetter> fieldValueGetters(
      @NonNull TypeDescriptor<?> targetTypeDescriptor, @NonNull Schema schema) {
    return schemaFieldDescriptors(targetTypeDescriptor.getRawType(), schema).keySet().stream()
        .map(FieldExtractor::new)
        .collect(Collectors.toList());
  }

  @Override
  public @NonNull List<FieldValueTypeInformation> fieldValueTypeInformations(
      @NonNull TypeDescriptor<?> targetTypeDescriptor, @NonNull Schema schema) {
    return schemaFieldDescriptors(targetTypeDescriptor.getRawType(), schema).values().stream()
        .map(
            descriptor ->
                fieldValueTypeInfo(targetTypeDescriptor.getRawType(), descriptor.fieldName))
        .collect(Collectors.toList());
  }

  @SuppressWarnings("unchecked")
  private static <FieldT extends TFieldIdEnum, T extends TBase<T, FieldT>>
      Map<FieldT, FieldMetaData> thriftFieldDescriptors(Class<?> targetClass) {
    return (Map<FieldT, FieldMetaData>) FieldMetaData.getStructMetaDataMap((Class<T>) targetClass);
  }

  private FieldValueTypeInformation fieldValueTypeInfo(Class<?> type, String fieldName) {
    if (TUnion.class.isAssignableFrom(type)) {
      final List<Method> factoryMethods =
          Stream.of(type.getDeclaredMethods())
              .filter(m -> m.getName().equals(fieldName))
              .filter(m -> m.getModifiers() == (Modifier.PUBLIC | Modifier.STATIC))
              .filter(m -> m.getParameterCount() == 1)
              .filter(m -> m.getReturnType() == type)
              .collect(Collectors.toList());
      if (factoryMethods.isEmpty()) {
        throw new IllegalArgumentException(
            String.format(
                "No suitable static factory method: %s.%s(...)", type.getName(), fieldName));
      }
      if (factoryMethods.size() > 1) {
        throw new IllegalStateException("Overloaded factory methods: " + factoryMethods);
      }
      return FieldValueTypeInformation.forSetter(factoryMethods.get(0), "", Collections.emptyMap());
    } else {
      try {
        return FieldValueTypeInformation.forField(
            type.getDeclaredField(fieldName), 0, Collections.emptyMap());
      } catch (NoSuchFieldException e) {
        throw new IllegalArgumentException(e);
      }
    }
  }

  @Override
  public @NonNull SchemaUserTypeCreator schemaTypeCreator(
      @NonNull TypeDescriptor<?> targetTypeDescriptor, @NonNull Schema schema) {
    final Map<TFieldIdEnum, FieldMetaData> fieldDescriptors =
        schemaFieldDescriptors(targetTypeDescriptor.getRawType(), schema);
    return params ->
        restoreThriftObject(targetTypeDescriptor.getRawType(), fieldDescriptors, params);
  }

  @SuppressWarnings("nullness")
  private Map<TFieldIdEnum, FieldMetaData> schemaFieldDescriptors(
      Class<?> targetClass, Schema schema) {
    final Map<TFieldIdEnum, FieldMetaData> fieldDescriptors = thriftFieldDescriptors(targetClass);
    final Map<String, TFieldIdEnum> fields =
        fieldDescriptors.keySet().stream()
            .collect(Collectors.toMap(TFieldIdEnum::getFieldName, Function.identity()));

    return schema.getFields().stream()
        .map(Schema.Field::getName)
        .map(fields::get)
        .collect(
            Collectors.toMap(
                Function.identity(),
                fieldDescriptors::get,
                ThriftSchema::throwingCombiner,
                LinkedHashMap::new));
  }

  private <FieldT extends TFieldIdEnum, T extends TBase<T, FieldT>> T restoreThriftObject(
      Class<?> targetClass, Map<FieldT, FieldMetaData> fields, Object[] params) {
    if (params.length != fields.size()) {
      throw new IllegalArgumentException(
          String.format(
              "The parameter list: %s does not match the expected fields: %s",
              Arrays.toString(params), fields.keySet()));
    }
    try {
      @SuppressWarnings("unchecked")
      final T thrift = (T) targetClass.getDeclaredConstructor().newInstance();
      final Iterator<Entry<FieldT, FieldMetaData>> iter = fields.entrySet().iterator();
      Stream.of(params).forEach(param -> setThriftField(thrift, iter.next(), param));
      return thrift;
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private <FieldT extends TFieldIdEnum, T extends TBase<T, FieldT>> void setThriftField(
      T thrift, Entry<FieldT, FieldMetaData> fieldDescriptor, Object value) {
    final FieldT field = fieldDescriptor.getKey();
    final FieldMetaData descriptor = fieldDescriptor.getValue();
    if (value != null) {
      final Object actualValue;
      switch (descriptor.valueMetaData.type) {
        case TType.SET:
          actualValue =
              StreamSupport.stream(((Iterable<?>) value).spliterator(), false)
                  .collect(Collectors.toSet());
          break;
        case TType.ENUM:
          final Class<? extends TEnum> enumClass =
              ((EnumMetaData) descriptor.valueMetaData).enumClass;
          @SuppressWarnings("nullness") // it's either "nullness" or "unsafe", apparently
          final TEnum @NonNull [] enumConstants = enumClass.getEnumConstants();
          actualValue = enumConstants[(Integer) value];
          break;
        default:
          actualValue = value;
      }
      thrift.setFieldValue(field, actualValue);
    }
  }

  private <EnumT extends Enum<EnumT> & TEnum> FieldType beamType(FieldValueMetaData metadata) {
    if (metadata.isTypedef()) {
      final FieldType beamType = typedefs.get(metadata.getTypedefName());
      if (beamType != null) {
        return beamType;
      }
    }
    switch (metadata.type) {
      case TType.BOOL:
        return FieldType.BOOLEAN;
      case TType.BYTE:
        return FieldType.BYTE;
      case TType.I16:
        return FieldType.INT16;
      case TType.I32:
        return FieldType.INT32;
      case TType.I64:
        return FieldType.INT64;
      case TType.DOUBLE:
        return FieldType.DOUBLE;
      case TType.STRING:
        return metadata.isBinary() ? FieldType.BYTES : FieldType.STRING;
      case TType.SET:
        final FieldValueMetaData setElemMetadata = ((SetMetaData) metadata).elemMetaData;
        return FieldType.iterable(beamType(setElemMetadata));
      case TType.LIST:
        final FieldValueMetaData listElemMetadata = ((ListMetaData) metadata).elemMetaData;
        return FieldType.array(beamType(listElemMetadata));
      case TType.MAP:
        final MapMetaData mapMetadata = ((MapMetaData) metadata);
        return FieldType.map(
            beamType(mapMetadata.keyMetaData), beamType(mapMetadata.valueMetaData));
      case TType.STRUCT:
        final StructMetaData structMetadata = ((StructMetaData) metadata);
        return FieldType.row(schemaFor(structMetadata.structClass));
      case TType.ENUM:
        @SuppressWarnings("unchecked")
        final Class<EnumT> enumClass = (Class<EnumT>) ((EnumMetaData) metadata).enumClass;
        @SuppressWarnings("nullness") // it's either "nullness" or "unsafe", apparently
        final EnumT @NonNull [] enumConstants = enumClass.getEnumConstants();
        final String[] enumValues =
            Stream.of(enumConstants).map(EnumT::name).toArray(String[]::new);
        return FieldType.logicalType(EnumerationType.create(enumValues));
      default:
        throw new IllegalArgumentException("Unsupported thrift type code: " + metadata.type);
    }
  }

  private static class FieldExtractor<FieldT extends TFieldIdEnum, T extends TBase<T, FieldT>>
      implements FieldValueGetter<T, Object> {
    private final FieldT field;

    private FieldExtractor(FieldT field) {
      this.field = field;
    }

    @Override
    public @Nullable Object get(T thrift) {
      if (!(thrift instanceof TUnion) || thrift.isSet(field)) {
        final Object value = thrift.getFieldValue(field);
        if (value instanceof Enum<?>) {
          return ((Enum<?>) value).ordinal();
        } else {
          return value;
        }
      } else {
        return null;
      }
    }

    @Override
    public @NonNull String name() {
      return field.getFieldName();
    }

    @Override
    public String toString() {
      return name();
    }
  }
}
