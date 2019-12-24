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
package org.apache.beam.sdk.extensions.protobuf;

import static org.apache.beam.sdk.extensions.protobuf.ProtoSchemaTranslator.getFieldNumber;

import com.google.protobuf.BoolValue;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.DoubleValue;
import com.google.protobuf.Duration;
import com.google.protobuf.FloatValue;
import com.google.protobuf.Int32Value;
import com.google.protobuf.Int64Value;
import com.google.protobuf.Internal.EnumLite;
import com.google.protobuf.MessageLite;
import com.google.protobuf.ProtocolMessageEnum;
import com.google.protobuf.StringValue;
import com.google.protobuf.Timestamp;
import com.google.protobuf.UInt32Value;
import com.google.protobuf.UInt64Value;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.sdk.extensions.protobuf.ProtoSchemaLogicalTypes.DurationNanos;
import org.apache.beam.sdk.extensions.protobuf.ProtoSchemaLogicalTypes.TimestampNanos;
import org.apache.beam.sdk.schemas.FieldValueGetter;
import org.apache.beam.sdk.schemas.FieldValueSetter;
import org.apache.beam.sdk.schemas.FieldValueTypeInformation;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.schemas.SchemaUserTypeCreator;
import org.apache.beam.sdk.schemas.logicaltypes.OneOfType;
import org.apache.beam.sdk.schemas.logicaltypes.OneOfType.Value;
import org.apache.beam.sdk.schemas.utils.ByteBuddyUtils.ConvertType;
import org.apache.beam.sdk.schemas.utils.ByteBuddyUtils.ConvertValueForGetter;
import org.apache.beam.sdk.schemas.utils.ByteBuddyUtils.ConvertValueForSetter;
import org.apache.beam.sdk.schemas.utils.ByteBuddyUtils.InjectPackageStrategy;
import org.apache.beam.sdk.schemas.utils.ByteBuddyUtils.TypeConversion;
import org.apache.beam.sdk.schemas.utils.ByteBuddyUtils.TypeConversionsFactory;
import org.apache.beam.sdk.schemas.utils.FieldValueTypeSupplier;
import org.apache.beam.sdk.schemas.utils.JavaBeanUtils;
import org.apache.beam.sdk.schemas.utils.ReflectUtils;
import org.apache.beam.sdk.schemas.utils.ReflectUtils.ClassWithSchema;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.ByteBuddy;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.description.method.MethodDescription;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.description.type.TypeDescription;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.description.type.TypeDescription.ForLoadedType;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.dynamic.DynamicType;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.dynamic.scaffold.InstrumentedType;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.implementation.Implementation;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.implementation.bytecode.ByteCodeAppender;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.implementation.bytecode.StackManipulation;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.implementation.bytecode.StackManipulation.Compound;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.implementation.bytecode.assign.Assigner;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.implementation.bytecode.assign.Assigner.Typing;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.implementation.bytecode.assign.TypeCasting;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.implementation.bytecode.member.MethodInvocation;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.implementation.bytecode.member.MethodReturn;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.matcher.ElementMatchers;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.CaseFormat;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Multimap;

public class ProtoByteBuddyUtils {
  private static final ByteBuddy BYTE_BUDDY = new ByteBuddy();
  private static final TypeDescriptor<ByteString> BYTE_STRING_TYPE_DESCRIPTOR =
      TypeDescriptor.of(ByteString.class);
  private static final TypeDescriptor<Timestamp> PROTO_TIMESTAMP_TYPE_DESCRIPTOR =
      TypeDescriptor.of(Timestamp.class);
  private static final TypeDescriptor<Duration> PROTO_DURATION_TYPE_DESCRIPTOR =
      TypeDescriptor.of(Duration.class);
  private static final TypeDescriptor<Int32Value> PROTO_INT32_VALUE_TYPE_DESCRIPTOR =
      TypeDescriptor.of(Int32Value.class);
  private static final TypeDescriptor<Int64Value> PROTO_INT64_VALUE_TYPE_DESCRIPTOR =
      TypeDescriptor.of(Int64Value.class);
  private static final TypeDescriptor<UInt32Value> PROTO_UINT32_VALUE_TYPE_DESCRIPTOR =
      TypeDescriptor.of(UInt32Value.class);
  private static final TypeDescriptor<UInt64Value> PROTO_UINT64_VALUE_TYPE_DESCRIPTOR =
      TypeDescriptor.of(UInt64Value.class);
  private static final TypeDescriptor<FloatValue> PROTO_FLOAT_VALUE_TYPE_DESCRIPTOR =
      TypeDescriptor.of(FloatValue.class);
  private static final TypeDescriptor<DoubleValue> PROTO_DOUBLE_VALUE_TYPE_DESCRIPTOR =
      TypeDescriptor.of(DoubleValue.class);
  private static final TypeDescriptor<BoolValue> PROTO_BOOL_VALUE_TYPE_DESCRIPTOR =
      TypeDescriptor.of(BoolValue.class);
  private static final TypeDescriptor<StringValue> PROTO_STRING_VALUE_TYPE_DESCRIPTOR =
      TypeDescriptor.of(StringValue.class);
  private static final TypeDescriptor<BytesValue> PROTO_BYTES_VALUE_TYPE_DESCRIPTOR =
      TypeDescriptor.of(BytesValue.class);

  private static final ForLoadedType BYTE_STRING_TYPE = new ForLoadedType(ByteString.class);
  private static final ForLoadedType BYTE_ARRAY_TYPE = new ForLoadedType(byte[].class);
  private static final ForLoadedType PROTO_ENUM_TYPE = new ForLoadedType(ProtocolMessageEnum.class);
  private static final ForLoadedType INTEGER_TYPE = new ForLoadedType(Integer.class);

  private static final Map<TypeDescriptor<?>, ForLoadedType> WRAPPER_LOADED_TYPES =
      ImmutableMap.<TypeDescriptor<?>, ForLoadedType>builder()
          .put(PROTO_INT32_VALUE_TYPE_DESCRIPTOR, new ForLoadedType(Int32Value.class))
          .put(PROTO_INT64_VALUE_TYPE_DESCRIPTOR, new ForLoadedType(Int64Value.class))
          .put(PROTO_UINT32_VALUE_TYPE_DESCRIPTOR, new ForLoadedType(UInt32Value.class))
          .put(PROTO_UINT64_VALUE_TYPE_DESCRIPTOR, new ForLoadedType(UInt64Value.class))
          .put(PROTO_FLOAT_VALUE_TYPE_DESCRIPTOR, new ForLoadedType(FloatValue.class))
          .put(PROTO_DOUBLE_VALUE_TYPE_DESCRIPTOR, new ForLoadedType(DoubleValue.class))
          .put(PROTO_BOOL_VALUE_TYPE_DESCRIPTOR, new ForLoadedType(BoolValue.class))
          .put(PROTO_STRING_VALUE_TYPE_DESCRIPTOR, new ForLoadedType(StringValue.class))
          .put(PROTO_BYTES_VALUE_TYPE_DESCRIPTOR, new ForLoadedType(BytesValue.class))
          .build();

  private static final Map<TypeName, String> PROTO_GETTER_SUFFIX =
      ImmutableMap.of(
          TypeName.ARRAY, "List",
          TypeName.ITERABLE, "List",
          TypeName.MAP, "Map");

  private static final Map<TypeName, String> PROTO_SETTER_PREFIX =
      ImmutableMap.of(
          TypeName.ARRAY, "addAll",
          TypeName.ITERABLE, "addAll",
          TypeName.MAP, "putAll");
  private static final String DEFAULT_PROTO_GETTER_PREFIX = "get";
  private static final String DEFAULT_PROTO_SETTER_PREFIX = "set";

  static String protoGetterName(String name, FieldType fieldType) {
    final String camel = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, name);
    return DEFAULT_PROTO_GETTER_PREFIX
        + camel
        + PROTO_GETTER_SUFFIX.getOrDefault(fieldType.getTypeName(), "");
  }

  static String protoSetterName(String name, FieldType fieldType) {
    final String camel = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, name);
    return protoSetterPrefix(fieldType) + camel;
  }

  static String protoSetterPrefix(FieldType fieldType) {
    return PROTO_SETTER_PREFIX.getOrDefault(fieldType.getTypeName(), DEFAULT_PROTO_SETTER_PREFIX);
  }

  static class ProtoConvertType extends ConvertType {
    ProtoConvertType(boolean returnRawValues) {
      super(returnRawValues);
    }

    private static final Map<TypeDescriptor<?>, Class<?>> TYPE_OVERRIDES =
        ImmutableMap.<TypeDescriptor<?>, Class<?>>builder()
            .put(PROTO_TIMESTAMP_TYPE_DESCRIPTOR, Row.class)
            .put(PROTO_DURATION_TYPE_DESCRIPTOR, Row.class)
            .put(PROTO_INT32_VALUE_TYPE_DESCRIPTOR, Integer.class)
            .put(PROTO_INT64_VALUE_TYPE_DESCRIPTOR, Long.class)
            .put(PROTO_UINT32_VALUE_TYPE_DESCRIPTOR, Integer.class)
            .put(PROTO_UINT64_VALUE_TYPE_DESCRIPTOR, Long.class)
            .put(PROTO_FLOAT_VALUE_TYPE_DESCRIPTOR, Float.class)
            .put(PROTO_DOUBLE_VALUE_TYPE_DESCRIPTOR, Double.class)
            .put(PROTO_BOOL_VALUE_TYPE_DESCRIPTOR, Boolean.class)
            .put(PROTO_STRING_VALUE_TYPE_DESCRIPTOR, String.class)
            .put(PROTO_BYTES_VALUE_TYPE_DESCRIPTOR, byte[].class)
            .build();

    @Override
    public Type convert(TypeDescriptor typeDescriptor) {
      if (typeDescriptor.equals(BYTE_STRING_TYPE_DESCRIPTOR)
          || typeDescriptor.isSubtypeOf(BYTE_STRING_TYPE_DESCRIPTOR)) {
        return byte[].class;
      } else if (typeDescriptor.isSubtypeOf(TypeDescriptor.of(ProtocolMessageEnum.class))) {
        return Integer.class;
      } else if (typeDescriptor.equals(PROTO_TIMESTAMP_TYPE_DESCRIPTOR)
          || typeDescriptor.equals(PROTO_DURATION_TYPE_DESCRIPTOR)) {
        return Row.class;
      } else {
        Type type = TYPE_OVERRIDES.get(typeDescriptor);
        return (type != null) ? type : super.convert(typeDescriptor);
      }
    }
  }

  static class ProtoConvertValueForGetter extends ConvertValueForGetter {
    ProtoConvertValueForGetter(StackManipulation readValue) {
      super(readValue);
    }

    @Override
    protected ProtoTypeConversionsFactory getFactory() {
      return new ProtoTypeConversionsFactory();
    }

    @Override
    public StackManipulation convert(TypeDescriptor type) {
      if (type.equals(BYTE_STRING_TYPE_DESCRIPTOR)
          || type.isSubtypeOf(BYTE_STRING_TYPE_DESCRIPTOR)) {
        return new Compound(
            readValue,
            MethodInvocation.invoke(
                BYTE_STRING_TYPE
                    .getDeclaredMethods()
                    .filter(ElementMatchers.named("toByteArray"))
                    .getOnly()));
      } else if (type.isSubtypeOf(TypeDescriptor.of(ProtocolMessageEnum.class))) {
        return new Compound(
            readValue,
            MethodInvocation.invoke(
                PROTO_ENUM_TYPE
                    .getDeclaredMethods()
                    .filter(
                        ElementMatchers.named("getNumber").and(ElementMatchers.takesArguments(0)))
                    .getOnly()),
            Assigner.DEFAULT.assign(
                INTEGER_TYPE.asUnboxed().asGenericType(),
                INTEGER_TYPE.asGenericType(),
                Typing.STATIC));
      } else if (type.equals(PROTO_TIMESTAMP_TYPE_DESCRIPTOR)) {
        return new Compound(
            readValue,
            MethodInvocation.invoke(
                new ForLoadedType(TimestampNanos.class)
                    .getDeclaredMethods()
                    .filter(ElementMatchers.named("toRow"))
                    .getOnly()));
      } else if (type.equals(PROTO_DURATION_TYPE_DESCRIPTOR)) {
        return new Compound(
            readValue,
            MethodInvocation.invoke(
                new ForLoadedType(DurationNanos.class)
                    .getDeclaredMethods()
                    .filter(ElementMatchers.named("toRow"))
                    .getOnly()));
      } else {
        ForLoadedType wrapperType = WRAPPER_LOADED_TYPES.get(type);
        if (wrapperType != null) {
          MethodDescription.InDefinedShape getValueMethod =
              wrapperType.getDeclaredMethods().filter(ElementMatchers.named("getValue")).getOnly();
          TypeDescription.Generic returnType = getValueMethod.getReturnType();
          StackManipulation stackManipulation =
              new Compound(
                  readValue,
                  MethodInvocation.invoke(getValueMethod),
                  Assigner.DEFAULT.assign(
                      returnType, returnType.asErasure().asBoxed().asGenericType(), Typing.STATIC));
          if (type.equals(PROTO_BYTES_VALUE_TYPE_DESCRIPTOR)) {
            stackManipulation =
                getFactory()
                    .createGetterConversions(stackManipulation)
                    .convert(BYTE_STRING_TYPE_DESCRIPTOR);
          }
          return stackManipulation;
        }
        return super.convert(type);
      }
    }
  }

  static class ProtoConvertValueForSetter extends ConvertValueForSetter {
    ProtoConvertValueForSetter(StackManipulation readValue) {
      super(readValue);
    }

    @Override
    protected ProtoTypeConversionsFactory getFactory() {
      return new ProtoTypeConversionsFactory();
    }

    @Override
    public StackManipulation convert(TypeDescriptor type) {
      if (type.isSubtypeOf(TypeDescriptor.of(ByteString.class))) {
        return new Compound(
            readValue,
            TypeCasting.to(BYTE_ARRAY_TYPE),
            MethodInvocation.invoke(
                BYTE_STRING_TYPE
                    .getDeclaredMethods()
                    .filter(
                        ElementMatchers.named("copyFrom")
                            .and(ElementMatchers.takesArguments(BYTE_ARRAY_TYPE)))
                    .getOnly()));
      } else if (type.isSubtypeOf(TypeDescriptor.of(ProtocolMessageEnum.class))) {
        ForLoadedType loadedType = new ForLoadedType(type.getRawType());
        // Convert the stored number back to the enum constant.
        return new Compound(
            readValue,
            Assigner.DEFAULT.assign(
                INTEGER_TYPE.asBoxed().asGenericType(),
                INTEGER_TYPE.asUnboxed().asGenericType(),
                Typing.STATIC),
            MethodInvocation.invoke(
                loadedType
                    .getDeclaredMethods()
                    .filter(
                        ElementMatchers.named("forNumber")
                            .and(ElementMatchers.isStatic().and(ElementMatchers.takesArguments(1))))
                    .getOnly()));
      } else if (type.equals(PROTO_TIMESTAMP_TYPE_DESCRIPTOR)) {
        return new Compound(
            readValue,
            MethodInvocation.invoke(
                new ForLoadedType(TimestampNanos.class)
                    .getDeclaredMethods()
                    .filter(ElementMatchers.named("toTimestamp"))
                    .getOnly()));
      } else if (type.equals(PROTO_DURATION_TYPE_DESCRIPTOR)) {
        return new Compound(
            readValue,
            MethodInvocation.invoke(
                new ForLoadedType(DurationNanos.class)
                    .getDeclaredMethods()
                    .filter(ElementMatchers.named("toDuration"))
                    .getOnly()));
      } else {
        ForLoadedType wrapperType = WRAPPER_LOADED_TYPES.get(type);
        if (wrapperType != null) {
          if (type.equals(PROTO_BYTES_VALUE_TYPE_DESCRIPTOR)) {
            readValue =
                getFactory()
                    .createSetterConversions(readValue)
                    .convert(TypeDescriptor.of(ByteString.class));
          }
          MethodDescription.InDefinedShape ofMethod =
              wrapperType.getDeclaredMethods().filter(ElementMatchers.named("of")).getOnly();
          TypeDescription.Generic argumentType = ofMethod.getParameters().get(0).getType();
          return new Compound(
              readValue,
              Assigner.DEFAULT.assign(
                  argumentType.asErasure().asBoxed().asGenericType(), argumentType, Typing.STATIC),
              MethodInvocation.invoke(ofMethod));
        } else {
          return super.convert(type);
        }
      }
    }
  }

  static class ProtoTypeConversionsFactory implements TypeConversionsFactory {
    @Override
    public TypeConversion<Type> createTypeConversion(boolean returnRawTypes) {
      return new ProtoConvertType(returnRawTypes);
    }

    @Override
    public TypeConversion<StackManipulation> createGetterConversions(StackManipulation readValue) {
      return new ProtoConvertValueForGetter(readValue);
    }

    @Override
    public TypeConversion<StackManipulation> createSetterConversions(StackManipulation readValue) {
      return new ProtoConvertValueForSetter(readValue);
    }
  }

  // The list of getters for a class is cached, so we only create the classes the first time
  // getSetters is called.
  private static final Map<ClassWithSchema, List<FieldValueGetter>> CACHED_GETTERS =
      Maps.newConcurrentMap();

  /**
   * Return the list of {@link FieldValueGetter}s for a Java Bean class
   *
   * <p>The returned list is ordered by the order of fields in the schema.
   */
  public static List<FieldValueGetter> getGetters(
      Class<?> clazz,
      Schema schema,
      FieldValueTypeSupplier fieldValueTypeSupplier,
      TypeConversionsFactory typeConversionsFactory) {
    Multimap<String, Method> methods = ReflectUtils.getMethodsMap(clazz);
    return CACHED_GETTERS.computeIfAbsent(
        ClassWithSchema.create(clazz, schema),
        c -> {
          List<FieldValueTypeInformation> types = fieldValueTypeSupplier.get(clazz, schema);
          return types.stream()
              .map(
                  t ->
                      createGetter(
                          t,
                          typeConversionsFactory,
                          clazz,
                          methods,
                          schema.getField(t.getName()),
                          fieldValueTypeSupplier))
              .collect(Collectors.toList());
        });
  }

  static class OneOfFieldValueGetter<ProtoT extends MessageLite>
      implements FieldValueGetter<ProtoT, OneOfType.Value> {
    private final String name;
    private final Supplier<Method> getCaseMethod;
    private final Map<Integer, FieldValueGetter<ProtoT, ?>> getterMethodMap;
    private final OneOfType oneOfType;

    public OneOfFieldValueGetter(
        String name,
        Supplier<Method> getCaseMethod,
        Map<Integer, FieldValueGetter<ProtoT, ?>> getterMethodMap,
        OneOfType oneOfType) {
      this.name = name;
      this.getCaseMethod = getCaseMethod;
      this.getterMethodMap = getterMethodMap;
      this.oneOfType = oneOfType;
    }

    @Nullable
    @Override
    public Value get(ProtoT object) {
      try {
        EnumLite caseValue = (EnumLite) getCaseMethod.get().invoke(object);
        if (caseValue.getNumber() == 0) {
          return null;
        } else {
          Object value = getterMethodMap.get(caseValue.getNumber()).get(object);
          return oneOfType.createValue(
              oneOfType.getCaseEnumType().valueOf(caseValue.getNumber()), value);
        }
      } catch (IllegalAccessException | InvocationTargetException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public String name() {
      return name;
    }
  }

  private static FieldValueGetter createGetter(
      FieldValueTypeInformation fieldValueTypeInformation,
      TypeConversionsFactory typeConversionsFactory,
      Class clazz,
      Multimap<String, Method> methods,
      Field field,
      FieldValueTypeSupplier fieldValueTypeSupplier) {
    if (field.getType().isLogicalType(OneOfType.IDENTIFIER)) {
      OneOfType oneOfType = field.getType().getLogicalType(OneOfType.class);

      // The case accessor method in the proto is named getOneOfNameCase.
      Method caseMethod =
          getProtoGetter(
              methods,
              field.getName() + "_case",
              FieldType.logicalType(oneOfType.getCaseEnumType()));
      Map<Integer, FieldValueGetter> oneOfGetters = Maps.newHashMap();
      Map<String, FieldValueTypeInformation> oneOfFieldTypes =
          fieldValueTypeSupplier.get(clazz, oneOfType.getOneOfSchema()).stream()
              .collect(Collectors.toMap(FieldValueTypeInformation::getName, f -> f));
      for (Field oneOfField : oneOfType.getOneOfSchema().getFields()) {
        int protoFieldIndex = getFieldNumber(oneOfField.getType());
        FieldValueGetter oneOfFieldGetter =
            createGetter(
                oneOfFieldTypes.get(oneOfField.getName()),
                typeConversionsFactory,
                clazz,
                methods,
                oneOfField,
                fieldValueTypeSupplier);
        oneOfGetters.put(protoFieldIndex, oneOfFieldGetter);
      }
      return new OneOfFieldValueGetter(
          field.getName(),
          (Supplier<Method> & Serializable) () -> caseMethod,
          oneOfGetters,
          oneOfType);
    } else {
      return JavaBeanUtils.createGetter(fieldValueTypeInformation, typeConversionsFactory);
    }
  }

  private static Class getProtoGeneratedBuilder(Class<?> clazz) {
    String builderClassName = clazz.getName() + "$Builder";
    try {
      return Class.forName(builderClassName);
    } catch (ClassNotFoundException e) {
      return null;
    }
  }

  static Method getProtoSetter(Multimap<String, Method> methods, String name, FieldType fieldType) {
    final TypeDescriptor<MessageLite.Builder> builderDescriptor =
        TypeDescriptor.of(MessageLite.Builder.class);
    return methods.get(protoSetterName(name, fieldType)).stream()
        // Setter methods take only a single parameter.
        .filter(m -> m.getParameterCount() == 1)
        // For nested types, we don't use the version that takes a builder.
        .filter(
            m -> !TypeDescriptor.of(m.getGenericParameterTypes()[0]).isSubtypeOf(builderDescriptor))
        .findAny()
        .orElseThrow(IllegalArgumentException::new);
  }

  static Method getProtoGetter(Multimap<String, Method> methods, String name, FieldType fieldType) {
    return methods.get(protoGetterName(name, fieldType)).stream()
        .filter(m -> m.getParameterCount() == 0)
        .findAny()
        .orElseThrow(IllegalArgumentException::new);
  }

  @Nullable
  public static SchemaUserTypeCreator getBuilderCreator(
      Class<?> protoClass, Schema schema, FieldValueTypeSupplier fieldValueTypeSupplier) {
    Class<?> builderClass = getProtoGeneratedBuilder(protoClass);
    if (builderClass == null) {
      return null;
    }

    List<FieldValueSetter> setters = Lists.newArrayListWithCapacity(schema.getFieldCount());
    Multimap<String, Method> methods = ReflectUtils.getMethodsMap(builderClass);
    for (Field field : schema.getFields()) {
      if (field.getType().isLogicalType(OneOfType.IDENTIFIER)) {
        OneOfType oneOfType = field.getType().getLogicalType(OneOfType.class);
        Map<Integer, Method> oneOfMethods = Maps.newHashMap();
        for (Field oneOfField : oneOfType.getOneOfSchema().getFields()) {
          Method method = getProtoSetter(methods, oneOfField.getName(), oneOfField.getType());
          oneOfMethods.put(getFieldNumber(oneOfField.getType()), method);
        }
        setters.add(
            new ProtoOneOfSetter(
                (Function<Integer, Method> & Serializable) oneOfMethods::get, field.getName()));
      } else {
        Method method = getProtoSetter(methods, field.getName(), field.getType());
        setters.add(
            JavaBeanUtils.createSetter(
                FieldValueTypeInformation.forSetter(method, protoSetterPrefix(field.getType())),
                new ProtoTypeConversionsFactory()));
      }
    }
    return createBuilderCreator(protoClass, builderClass, setters, schema);
  }

  static class ProtoOneOfSetter<BuilderT extends MessageLite.Builder>
      implements FieldValueSetter<BuilderT, OneOfType.Value> {
    private final Function<Integer, Method> methods;
    private final String name;

    ProtoOneOfSetter(Function<Integer, Method> methods, String name) {
      this.methods = methods;
      this.name = name;
    }

    @Override
    public void set(BuilderT builder, OneOfType.Value oneOfValue) {
      Method method = methods.apply(oneOfValue.getCaseType().getValue());
      try {
        method.invoke(builder, oneOfValue.getValue());
      } catch (IllegalAccessException | InvocationTargetException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public String name() {
      return name;
    }
  }

  static SchemaUserTypeCreator createBuilderCreator(
      Class<?> protoClass, Class<?> builderClass, List<FieldValueSetter> setters, Schema schema) {
    try {
      DynamicType.Builder<Supplier> builder =
          BYTE_BUDDY
              .with(new InjectPackageStrategy(builderClass))
              .subclass(Supplier.class)
              .method(ElementMatchers.named("get"))
              .intercept(new BuilderSupplier(protoClass));
      Supplier supplier =
          builder
              .make()
              .load(ReflectHelpers.findClassLoader(), ClassLoadingStrategy.Default.INJECTION)
              .getLoaded()
              .getDeclaredConstructor()
              .newInstance();
      return new ProtoCreatorFactory(supplier, setters);
    } catch (InstantiationException
        | IllegalAccessException
        | NoSuchMethodException
        | InvocationTargetException e) {
      throw new RuntimeException(
          "Unable to generate a creator for class " + builderClass + " with schema " + schema);
    }
  }

  static class ProtoCreatorFactory implements SchemaUserTypeCreator {
    private final Supplier<? extends MessageLite.Builder> builderCreator;
    private final List<FieldValueSetter> setters;

    public ProtoCreatorFactory(
        Supplier<? extends MessageLite.Builder> builderCreator, List<FieldValueSetter> setters) {
      this.builderCreator = builderCreator;
      this.setters = setters;
    }

    @Override
    public Object create(Object... params) {
      MessageLite.Builder builder = builderCreator.get();
      for (int i = 0; i < params.length; ++i) {
        setters.get(i).set(builder, params[i]);
      }
      return builder.build();
    }
  }

  static class BuilderSupplier implements Implementation {
    private final Class<?> protoClass;

    public BuilderSupplier(Class<?> protoClass) {
      this.protoClass = protoClass;
    }

    @Override
    public InstrumentedType prepare(InstrumentedType instrumentedType) {
      return instrumentedType;
    }

    @Override
    public ByteCodeAppender appender(final Target implementationTarget) {
      ForLoadedType loadedProto = new ForLoadedType(protoClass);
      return (methodVisitor, implementationContext, instrumentedMethod) -> {
        // this + method parameters.
        int numLocals = 1 + instrumentedMethod.getParameters().size();

        // Create the builder object by calling ProtoClass.newBuilder().
        StackManipulation stackManipulation =
            new StackManipulation.Compound(
                MethodInvocation.invoke(
                    loadedProto
                        .getDeclaredMethods()
                        .filter(
                            ElementMatchers.named("newBuilder")
                                .and(ElementMatchers.takesArguments(0)))
                        .getOnly()),
                MethodReturn.REFERENCE);
        StackManipulation.Size size = stackManipulation.apply(methodVisitor, implementationContext);
        return new ByteCodeAppender.Size(size.getMaximalSize(), numLocals);
      };
    }
  }
}
