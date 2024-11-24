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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.asm.AsmVisitorWrapper;
import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.description.method.MethodDescription.ForLoadedMethod;
import net.bytebuddy.description.modifier.FieldManifestation;
import net.bytebuddy.description.modifier.Visibility;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.description.type.TypeDescription.ForLoadedType;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.dynamic.scaffold.InstrumentedType;
import net.bytebuddy.implementation.FixedValue;
import net.bytebuddy.implementation.Implementation;
import net.bytebuddy.implementation.bytecode.ByteCodeAppender;
import net.bytebuddy.implementation.bytecode.ByteCodeAppender.Size;
import net.bytebuddy.implementation.bytecode.Duplication;
import net.bytebuddy.implementation.bytecode.StackManipulation;
import net.bytebuddy.implementation.bytecode.StackManipulation.Compound;
import net.bytebuddy.implementation.bytecode.assign.Assigner;
import net.bytebuddy.implementation.bytecode.assign.Assigner.Typing;
import net.bytebuddy.implementation.bytecode.assign.TypeCasting;
import net.bytebuddy.implementation.bytecode.collection.ArrayAccess;
import net.bytebuddy.implementation.bytecode.constant.IntegerConstant;
import net.bytebuddy.implementation.bytecode.constant.NullConstant;
import net.bytebuddy.implementation.bytecode.member.FieldAccess;
import net.bytebuddy.implementation.bytecode.member.MethodInvocation;
import net.bytebuddy.implementation.bytecode.member.MethodReturn;
import net.bytebuddy.implementation.bytecode.member.MethodVariableAccess;
import net.bytebuddy.jar.asm.ClassWriter;
import net.bytebuddy.jar.asm.Label;
import net.bytebuddy.matcher.ElementMatchers;
import org.apache.beam.sdk.schemas.FieldValueGetter;
import org.apache.beam.sdk.schemas.FieldValueSetter;
import org.apache.beam.sdk.schemas.FieldValueTypeInformation;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.schemas.SchemaUserTypeCreator;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.apache.beam.sdk.schemas.logicaltypes.OneOfType;
import org.apache.beam.sdk.schemas.utils.ByteBuddyUtils;
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
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Verify;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Multimap;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

class ProtoByteBuddyUtils {
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
  private static final ForLoadedType ENUM_LITE_TYPE = new ForLoadedType(EnumLite.class);
  private static final ForLoadedType FIELD_VALUE_GETTER_LOADED_TYPE =
      new ForLoadedType(FieldValueGetter.class);
  private static final ForLoadedType FIELD_VALUE_SETTER_LOADED_TYPE =
      new ForLoadedType(FieldValueSetter.class);
  private static final ForLoadedType ONEOF_TYPE_LOADED_TYPE = new ForLoadedType(OneOfType.class);
  private static final ForLoadedType ONEOF_VALUE_TYPE_LOADED_TYPE =
      new ForLoadedType(OneOfType.Value.class);
  private static final ForLoadedType ENUM_TYPE_LOADED_TYPE =
      new ForLoadedType(EnumerationType.class);
  private static final ForLoadedType ENUM_VALUE_TYPE_LOADED_TYPE =
      new ForLoadedType(EnumerationType.Value.class);

  private static final String CASE_GETTERS_FIELD_NAME = "CASE_GETTERS";
  private static final String CASE_SETTERS_FIELD_NAME = "CASE_SETTERS";
  private static final String ONEOF_TYPE_FIELD_NAME = "ONEOF_TYPE";

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

  // https://github.com/apache/beam/issues/21626: there is a slight difference between 'protoc' and
  // Guava CaseFormat regarding the camel case conversion
  // - guava keeps the first character after a number lower case
  // - protoc makes it upper case
  // based on
  // https://github.com/protocolbuffers/protobuf/blob/ec79d0d328c7e6cea15cc27fbeb9b018ca289590/src/google/protobuf/compiler/java/helpers.cc#L173-L208
  @VisibleForTesting
  static String convertProtoPropertyNameToJavaPropertyName(String input) {
    boolean capitalizeNextLetter = true;
    Preconditions.checkArgument(!Strings.isNullOrEmpty(input));
    StringBuilder result = new StringBuilder(input.length());
    for (int i = 0; i < input.length(); i++) {
      final char c = input.charAt(i);
      if (Character.isLowerCase(c)) {
        if (capitalizeNextLetter) {
          result.append(Character.toUpperCase(c));
        } else {
          result.append(c);
        }
        capitalizeNextLetter = false;
      } else if (Character.isUpperCase(c)) {
        if (i == 0 && !capitalizeNextLetter) {
          // Force first letter to lower-case unless explicitly told to
          // capitalize it.
          result.append(Character.toLowerCase(c));
        } else {
          // Capital letters after the first are left as-is.
          result.append(c);
        }
        capitalizeNextLetter = false;
      } else if ('0' <= c && c <= '9') {
        result.append(c);
        capitalizeNextLetter = true;
      } else {
        capitalizeNextLetter = true;
      }
    }
    // Add a trailing "_" if the name should be altered.
    if (input.charAt(input.length() - 1) == '#') {
      result.append('_');
    }
    return result.toString();
  }

  static String protoGetterName(String name, FieldType fieldType) {
    final String camel = convertProtoPropertyNameToJavaPropertyName(name);
    return DEFAULT_PROTO_GETTER_PREFIX
        + camel
        + PROTO_GETTER_SUFFIX.getOrDefault(fieldType.getTypeName(), "");
  }

  static String protoSetterName(String name, FieldType fieldType) {
    final String camel = convertProtoPropertyNameToJavaPropertyName(name);
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
    public Type convert(TypeDescriptor<?> typeDescriptor) {
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
    public StackManipulation convert(TypeDescriptor<?> type) {
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
                new ForLoadedType(ProtoSchemaLogicalTypes.TimestampConvert.class)
                    .getDeclaredMethods()
                    .filter(ElementMatchers.named("toRow"))
                    .getOnly()));
      } else if (type.equals(PROTO_DURATION_TYPE_DESCRIPTOR)) {
        return new Compound(
            readValue,
            MethodInvocation.invoke(
                new ForLoadedType(ProtoSchemaLogicalTypes.DurationConvert.class)
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
    public StackManipulation convert(TypeDescriptor<?> type) {
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
                new ForLoadedType(ProtoSchemaLogicalTypes.TimestampConvert.class)
                    .getDeclaredMethods()
                    .filter(ElementMatchers.named("toTimestamp"))
                    .getOnly()));
      } else if (type.equals(PROTO_DURATION_TYPE_DESCRIPTOR)) {
        return new Compound(
            readValue,
            MethodInvocation.invoke(
                new ForLoadedType(ProtoSchemaLogicalTypes.DurationConvert.class)
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
  private static final Map<ClassWithSchema, List<FieldValueGetter<?, ?>>> CACHED_GETTERS =
      Maps.newConcurrentMap();

  /**
   * Return the list of {@link FieldValueGetter}s for a Java Bean class
   *
   * <p>The returned list is ordered by the order of fields in the schema.
   */
  public static <T> List<FieldValueGetter<@NonNull T, Object>> getGetters(
      Class<? super T> clazz,
      Schema schema,
      FieldValueTypeSupplier fieldValueTypeSupplier,
      TypeConversionsFactory typeConversionsFactory) {
    Multimap<String, Method> methods = ReflectUtils.getMethodsMap(clazz);
    return (List)
        CACHED_GETTERS.computeIfAbsent(
            ClassWithSchema.create(clazz, schema),
            c -> {
              List<FieldValueTypeInformation> types =
                  fieldValueTypeSupplier.get(TypeDescriptor.of(clazz), schema);
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

  static <ProtoT> FieldValueGetter<@NonNull ProtoT, OneOfType.Value> createOneOfGetter(
      FieldValueTypeInformation typeInformation,
      TreeMap<Integer, FieldValueGetter<@NonNull ProtoT, OneOfType.Value>> getterMethodMap,
      Class<ProtoT> protoClass,
      OneOfType oneOfType,
      Method getCaseMethod) {
    Set<Integer> indices = getterMethodMap.keySet();
    boolean contiguous = isContiguous(indices);
    Preconditions.checkArgument(
        typeInformation.getType().equals(TypeDescriptor.of(OneOfType.Value.class)));

    int[] keys = getterMethodMap.keySet().stream().mapToInt(Integer::intValue).toArray();

    DynamicType.Builder<FieldValueGetter<@NonNull ProtoT, OneOfType.Value>> builder =
        ByteBuddyUtils.subclassGetterInterface(BYTE_BUDDY, protoClass, OneOfType.Value.class);
    builder =
        builder
            .method(ElementMatchers.named("name"))
            .intercept(FixedValue.reference(typeInformation.getName()))
            .method(ElementMatchers.named("get"))
            .intercept(new OneOfGetterInstruction(contiguous, keys, getCaseMethod));

    List<FieldValueGetter<@NonNull ProtoT, OneOfType.Value>> getters =
        Lists.newArrayList(getterMethodMap.values());
    builder =
        builder
            // Store a field with the list of individual getters. The get() instruction will pick
            // the appropriate
            // getter from the list based on the case value of the OneOf.
            .defineField(
                CASE_GETTERS_FIELD_NAME,
                FieldValueGetter[].class,
                Visibility.PRIVATE,
                FieldManifestation.FINAL)
            // Store a field for the specific OneOf type.
            .defineField(
                ONEOF_TYPE_FIELD_NAME,
                OneOfType.class,
                Visibility.PRIVATE,
                FieldManifestation.FINAL)
            .defineConstructor(Modifier.PUBLIC)
            .withParameters(List.class, OneOfType.class)
            .intercept(new OneOfGetterConstructor());

    try {
      return builder
          .visit(new AsmVisitorWrapper.ForDeclaredMethods().writerFlags(ClassWriter.COMPUTE_FRAMES))
          .make()
          .load(ReflectHelpers.findClassLoader(), ClassLoadingStrategy.Default.INJECTION)
          .getLoaded()
          .getDeclaredConstructor(List.class, OneOfType.class)
          .newInstance(getters, oneOfType);
    } catch (InstantiationException
        | IllegalAccessException
        | NoSuchMethodException
        | InvocationTargetException e) {
      throw new RuntimeException(
          "Unable to generate a getter for getter '" + typeInformation.getMethod() + "'");
    }
  }

  static <ProtoBuilderT extends MessageLite.Builder>
      FieldValueSetter<ProtoBuilderT, Object> createOneOfSetter(
          String name,
          TreeMap<Integer, FieldValueSetter<ProtoBuilderT, Object>> setterMethodMap,
          Class<ProtoBuilderT> protoBuilderClass) {
    Set<Integer> indices = setterMethodMap.keySet();
    boolean contiguous = isContiguous(indices);
    int[] keys = setterMethodMap.keySet().stream().mapToInt(Integer::intValue).toArray();

    DynamicType.Builder<FieldValueSetter<ProtoBuilderT, Object>> builder =
        ByteBuddyUtils.subclassSetterInterface(
            BYTE_BUDDY, protoBuilderClass, OneOfType.Value.class);
    builder =
        builder
            .method(ElementMatchers.named("name"))
            .intercept(FixedValue.reference(name))
            .method(ElementMatchers.named("set"))
            .intercept(new OneOfSetterInstruction(contiguous, keys));

    builder =
        builder
            // Store a field with the list of individual setters. The get() instruction will pick
            // the appropriate
            // getter from the list based on the case value of the OneOf.
            .defineField(
                CASE_SETTERS_FIELD_NAME,
                FieldValueSetter[].class,
                Visibility.PRIVATE,
                FieldManifestation.FINAL)
            .defineConstructor(Modifier.PUBLIC)
            .withParameters(List.class)
            .intercept(new OneOfSetterConstructor());

    List<FieldValueSetter<ProtoBuilderT, Object>> setters =
        Lists.newArrayList(setterMethodMap.values());
    try {
      return builder
          .visit(new AsmVisitorWrapper.ForDeclaredMethods().writerFlags(ClassWriter.COMPUTE_FRAMES))
          .make()
          .load(ReflectHelpers.findClassLoader(), ClassLoadingStrategy.Default.INJECTION)
          .getLoaded()
          .getDeclaredConstructor(List.class)
          .newInstance(setters);
    } catch (InstantiationException
        | IllegalAccessException
        | NoSuchMethodException
        | InvocationTargetException e) {
      throw new RuntimeException("Unable to generate a setter for setter '" + name + "'", e);
    }
  }

  private static boolean isContiguous(Set<Integer> indices) {
    Preconditions.checkArgument(!indices.isEmpty());
    Iterator<Integer> iter = indices.iterator();
    Preconditions.checkArgument(iter.hasNext());
    int current = iter.next();
    while (iter.hasNext()) {
      if (iter.next() > current + 1) {
        return false;
      }
    }
    return true;
  }

  private static class OneOfGetterConstructor implements Implementation {
    @Override
    public InstrumentedType prepare(InstrumentedType instrumentedType) {
      return instrumentedType;
    }

    @Override
    public ByteCodeAppender appender(final Target implementationTarget) {
      return (methodVisitor, implementationContext, instrumentedMethod) -> {
        StackManipulation stackManipulation =
            new StackManipulation.Compound(
                // Call the base constructor for Object.
                MethodVariableAccess.REFERENCE.loadFrom(0),
                Duplication.SINGLE,
                MethodInvocation.invoke(
                    new ForLoadedType(Object.class)
                        .getDeclaredMethods()
                        .filter(
                            ElementMatchers.isConstructor().and(ElementMatchers.takesArguments(0)))
                        .getOnly()),
                Duplication.SINGLE,
                // Store the list of FieldValueGetters as a member variable.
                MethodVariableAccess.REFERENCE.loadFrom(1),
                MethodInvocation.invoke(
                    new ForLoadedType(List.class)
                        .getDeclaredMethods()
                        .filter(
                            ElementMatchers.named("toArray").and(ElementMatchers.takesArguments(0)))
                        .getOnly()),
                FieldAccess.forField(
                        implementationTarget
                            .getInstrumentedType()
                            .getDeclaredFields()
                            .filter(ElementMatchers.named(CASE_GETTERS_FIELD_NAME))
                            .getOnly())
                    .write(),
                // Store the OneOf type as a member variable.
                MethodVariableAccess.REFERENCE.loadFrom(2),
                FieldAccess.forField(
                        implementationTarget
                            .getInstrumentedType()
                            .getDeclaredFields()
                            .filter(ElementMatchers.named(ONEOF_TYPE_FIELD_NAME))
                            .getOnly())
                    .write(),
                MethodReturn.VOID);
        StackManipulation.Size size = stackManipulation.apply(methodVisitor, implementationContext);
        return new Size(size.getMaximalSize(), 2);
      };
    }
  }

  private static class OneOfSetterConstructor implements Implementation {
    @Override
    public InstrumentedType prepare(InstrumentedType instrumentedType) {
      return instrumentedType;
    }

    @Override
    public ByteCodeAppender appender(final Target implementationTarget) {
      return (methodVisitor, implementationContext, instrumentedMethod) -> {
        StackManipulation stackManipulation =
            new StackManipulation.Compound(
                // Call the base constructor for Object.
                MethodVariableAccess.REFERENCE.loadFrom(0),
                Duplication.SINGLE,
                MethodInvocation.invoke(
                    new ForLoadedType(Object.class)
                        .getDeclaredMethods()
                        .filter(
                            ElementMatchers.isConstructor().and(ElementMatchers.takesArguments(0)))
                        .getOnly()),
                Duplication.SINGLE,
                // Store the list of FieldValueSetters as a member variable.
                MethodVariableAccess.REFERENCE.loadFrom(1),
                MethodInvocation.invoke(
                    new ForLoadedType(List.class)
                        .getDeclaredMethods()
                        .filter(
                            ElementMatchers.named("toArray").and(ElementMatchers.takesArguments(0)))
                        .getOnly()),
                FieldAccess.forField(
                        implementationTarget
                            .getInstrumentedType()
                            .getDeclaredFields()
                            .filter(ElementMatchers.named(CASE_SETTERS_FIELD_NAME))
                            .getOnly())
                    .write(),
                MethodReturn.VOID);
        StackManipulation.Size size = stackManipulation.apply(methodVisitor, implementationContext);
        return new Size(size.getMaximalSize(), 2);
      };
    }
  }

  // Implements the get method for OneOf fields.
  static class OneOfGetterInstruction implements Implementation {
    private final boolean isContiguous;
    private final int[] keys;
    private final Method getCaseMethod;

    public OneOfGetterInstruction(boolean isContiguous, int[] keys, Method getCaseMethod) {
      this.isContiguous = isContiguous;
      this.keys = keys;
      this.getCaseMethod = getCaseMethod;
    }

    @Override
    public InstrumentedType prepare(InstrumentedType instrumentedType) {
      return instrumentedType;
    }

    @Override
    public ByteCodeAppender appender(final Target implementationTarget) {
      return (methodVisitor, implementationContext, instrumentedMethod) -> {
        // this + method parameters.
        int numLocals = 1 + instrumentedMethod.getParameters().size();
        StackManipulation.Size size = new StackManipulation.Size(0, numLocals);

        // Initialize the set of keys and switch labels. The set of keys must be sorted, which we
        // get since
        // getterMethodMap is a TreeSet.
        Label defaultLabel = new Label();
        Label[] labels = new Label[keys.length];
        Arrays.setAll(labels, i -> new Label());

        // Read case value to switch on.
        StackManipulation readCaseValue =
            new StackManipulation.Compound(
                // Call the proto getter that returns the case enum.
                MethodVariableAccess.REFERENCE.loadFrom(1),
                MethodInvocation.invoke(new ForLoadedMethod(getCaseMethod)),
                TypeCasting.to(ENUM_LITE_TYPE),
                // Call EnumLite.getNumber to extract the integer for the current case enum.
                MethodInvocation.invoke(
                    ENUM_LITE_TYPE
                        .getDeclaredMethods()
                        .filter(ElementMatchers.named("getNumber"))
                        .getOnly()));
        size = size.aggregate(readCaseValue.apply(methodVisitor, implementationContext));

        // Start the switch block.
        if (isContiguous) {
          // If the case enum values are not all contiguous (i.e. there are holes), then generate
          // the slower
          // TABLESWITCH opcode.
          methodVisitor.visitTableSwitchInsn(keys[0], keys[keys.length - 1], defaultLabel, labels);
        } else {
          // If all the case enum value are contiguous, then generate the faster LOOKUPSWITCH
          // opcode.
          methodVisitor.visitLookupSwitchInsn(defaultLabel, keys, labels);
        }

        // Now generate all the case labels.
        for (int i = 0; i < labels.length; ++i) {
          // Place the current case label.
          methodVisitor.visitLabel(labels[i]);
          // Generate the following code:
          // return this.ONE_OF_TYPE.createValue(
          //    oneOfType.getCaseEnumType().valueOf(caseValue.getNumber()),
          // CASE_GETTERS[i].get(object));
          StackManipulation returnGetterGet =
              new StackManipulation.Compound(
                  // this parameter.
                  MethodVariableAccess.REFERENCE.loadFrom(0),
                  // Read the OneOf type value.
                  FieldAccess.forField(
                          implementationTarget
                              .getInstrumentedType()
                              .getDeclaredFields()
                              .filter(ElementMatchers.named(ONEOF_TYPE_FIELD_NAME))
                              .getOnly())
                      .read(),
                  Duplication.SINGLE,
                  MethodInvocation.invoke(
                      ONEOF_TYPE_LOADED_TYPE
                          .getDeclaredMethods()
                          .filter(ElementMatchers.named("getCaseEnumType"))
                          .getOnly()),
                  IntegerConstant.forValue(keys[i]),
                  MethodInvocation.invoke(
                      ENUM_TYPE_LOADED_TYPE
                          .getDeclaredMethods()
                          .filter(
                              ElementMatchers.named("valueOf")
                                  .and(ElementMatchers.takesArguments(int.class)))
                          .getOnly()),
                  MethodVariableAccess.REFERENCE.loadFrom(0),
                  // load array of component getters
                  FieldAccess.forField(
                          implementationTarget
                              .getInstrumentedType()
                              .getDeclaredFields()
                              .filter(ElementMatchers.named(CASE_GETTERS_FIELD_NAME))
                              .getOnly())
                      .read(),
                  // Access the ith getter.
                  IntegerConstant.forValue(i),
                  ArrayAccess.REFERENCE.load(),
                  // Access the object parameter.
                  MethodVariableAccess.REFERENCE.loadFrom(1),
                  // Now call the getter's get method.
                  MethodInvocation.invoke(
                      FIELD_VALUE_GETTER_LOADED_TYPE
                          .getDeclaredMethods()
                          .filter(ElementMatchers.named("get"))
                          .getOnly()),
                  MethodInvocation.invoke(
                      ONEOF_TYPE_LOADED_TYPE
                          .getDeclaredMethods()
                          .filter(
                              ElementMatchers.named("createValue")
                                  .and(
                                      ElementMatchers.takesArgument(
                                          0, EnumerationType.Value.class)))
                          .getOnly()),
                  MethodReturn.REFERENCE);
          size = size.aggregate(returnGetterGet.apply(methodVisitor, implementationContext));
        }
        methodVisitor.visitLabel(defaultLabel);
        StackManipulation defaultHandler =
            new StackManipulation.Compound(NullConstant.INSTANCE, MethodReturn.REFERENCE);
        size = size.aggregate(defaultHandler.apply(methodVisitor, implementationContext));

        return new Size(size.getMaximalSize(), size.getSizeImpact());
      };
    }
  }

  static class OneOfSetterInstruction implements Implementation {
    private final boolean isContiguous;
    private final int[] keys;

    public OneOfSetterInstruction(boolean isContiguous, int[] keys) {
      this.isContiguous = isContiguous;
      this.keys = keys;
    }

    @Override
    public InstrumentedType prepare(InstrumentedType instrumentedType) {
      return instrumentedType;
    }

    @Override
    public ByteCodeAppender appender(final Target implementationTarget) {
      return (methodVisitor, implementationContext, instrumentedMethod) -> {
        // this + method parameters.
        int numLocals = 1 + instrumentedMethod.getParameters().size();
        StackManipulation.Size size = new StackManipulation.Size(0, numLocals);

        // Initialize the set of keys and switch labels. The set of keys must be sorted, which we
        // get since
        // getterMethodMap is a TreeSet.
        Label defaultLabel = new Label();
        Label[] labels = new Label[keys.length];
        Arrays.setAll(labels, i -> new Label());

        // Read case value to switch on.
        StackManipulation readCaseValue =
            new StackManipulation.Compound(
                // Call the proto getter that returns the case enum.
                // value.getCaseType().getValue()
                MethodVariableAccess.REFERENCE.loadFrom(2),
                MethodInvocation.invoke(
                    ONEOF_VALUE_TYPE_LOADED_TYPE
                        .getDeclaredMethods()
                        .filter(ElementMatchers.named("getCaseType"))
                        .getOnly()),
                MethodInvocation.invoke(
                    ENUM_VALUE_TYPE_LOADED_TYPE
                        .getDeclaredMethods()
                        .filter(ElementMatchers.named("getValue"))
                        .getOnly()));
        size = size.aggregate(readCaseValue.apply(methodVisitor, implementationContext));

        // Start the switch block.
        if (isContiguous) {
          // If the case enum values are not all contiguous (i.e. there are holes), then generate
          // the slower TABLESWITCH opcode.
          methodVisitor.visitTableSwitchInsn(keys[0], keys[keys.length - 1], defaultLabel, labels);
        } else {
          // If all the case enum value are contiguous, then generate the faster LOOKUPSWITCH
          // opcode.
          methodVisitor.visitLookupSwitchInsn(defaultLabel, keys, labels);
        }

        // Now generate all the case labels.
        for (int i = 0; i < labels.length; ++i) {
          // Place the current case label.
          methodVisitor.visitLabel(labels[i]);
          // Generate the following code:
          // return this.CASE_SETTERS[caseValueIndex].set(object, value.getValue());
          StackManipulation returnGetterGet =
              new StackManipulation.Compound(
                  // load array of component getters
                  MethodVariableAccess.REFERENCE.loadFrom(0),
                  FieldAccess.forField(
                          implementationTarget
                              .getInstrumentedType()
                              .getDeclaredFields()
                              .filter(ElementMatchers.named(CASE_SETTERS_FIELD_NAME))
                              .getOnly())
                      .read(),
                  // Access the ith getter.
                  IntegerConstant.forValue(i),
                  ArrayAccess.REFERENCE.load(),
                  MethodVariableAccess.REFERENCE.loadFrom(1),
                  MethodVariableAccess.REFERENCE.loadFrom(2),
                  MethodInvocation.invoke(
                      ONEOF_VALUE_TYPE_LOADED_TYPE
                          .getDeclaredMethods()
                          .filter(
                              ElementMatchers.named("getValue")
                                  .and(ElementMatchers.takesArguments(0)))
                          .getOnly()),
                  MethodInvocation.invoke(
                      FIELD_VALUE_SETTER_LOADED_TYPE
                          .getDeclaredMethods()
                          .filter(ElementMatchers.named("set"))
                          .getOnly()),
                  MethodReturn.VOID);
          size = size.aggregate(returnGetterGet.apply(methodVisitor, implementationContext));
        }
        methodVisitor.visitLabel(defaultLabel);
        StackManipulation defaultHandler = MethodReturn.VOID;
        size = size.aggregate(defaultHandler.apply(methodVisitor, implementationContext));

        return new Size(size.getMaximalSize(), size.getSizeImpact());
      };
    }
  }

  private static <ProtoT> FieldValueGetter<@NonNull ProtoT, ?> createGetter(
      FieldValueTypeInformation fieldValueTypeInformation,
      TypeConversionsFactory typeConversionsFactory,
      Class<ProtoT> clazz,
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
      // Create a map of case enum value to getter. This must be sorted, so store in a TreeMap.
      TreeMap<Integer, FieldValueGetter<@NonNull ProtoT, OneOfType.Value>> oneOfGetters =
          Maps.newTreeMap();
      Map<String, FieldValueTypeInformation> oneOfFieldTypes =
          fieldValueTypeSupplier.get(TypeDescriptor.of(clazz), oneOfType.getOneOfSchema()).stream()
              .collect(Collectors.toMap(FieldValueTypeInformation::getName, f -> f));
      for (Field oneOfField : oneOfType.getOneOfSchema().getFields()) {
        int protoFieldIndex = getFieldNumber(oneOfField);
        FieldValueGetter<@NonNull ProtoT, ?> oneOfFieldGetter =
            createGetter(
                Verify.verifyNotNull(oneOfFieldTypes.get(oneOfField.getName())),
                typeConversionsFactory,
                clazz,
                methods,
                oneOfField,
                fieldValueTypeSupplier);
        oneOfGetters.put(
            protoFieldIndex, (FieldValueGetter<@NonNull ProtoT, OneOfType.Value>) oneOfFieldGetter);
      }
      return createOneOfGetter(
          fieldValueTypeInformation, oneOfGetters, clazz, oneOfType, caseMethod);
    } else {
      return JavaBeanUtils.createGetter(fieldValueTypeInformation, typeConversionsFactory);
    }
  }

  private static <ProtoBuilderT> @Nullable Class<ProtoBuilderT> getProtoGeneratedBuilder(
      Class<?> clazz) {
    String builderClassName = clazz.getName() + "$Builder";
    try {
      return (Class<ProtoBuilderT>) Class.forName(builderClassName);
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

  public static @Nullable <ProtoBuilderT extends MessageLite.Builder>
      SchemaUserTypeCreator getBuilderCreator(
          TypeDescriptor<?> protoTypeDescriptor,
          Schema schema,
          FieldValueTypeSupplier fieldValueTypeSupplier) {
    Class<ProtoBuilderT> builderClass = getProtoGeneratedBuilder(protoTypeDescriptor.getRawType());
    if (builderClass == null) {
      return null;
    }
    Multimap<String, Method> methods = ReflectUtils.getMethodsMap(builderClass);
    List<FieldValueSetter<ProtoBuilderT, Object>> setters =
        schema.getFields().stream()
            .map(f -> getProtoFieldValueSetter(protoTypeDescriptor, f, methods, builderClass))
            .collect(Collectors.toList());
    return createBuilderCreator(protoTypeDescriptor.getRawType(), builderClass, setters, schema);
  }

  private static <ProtoBuilderT extends MessageLite.Builder>
      FieldValueSetter<ProtoBuilderT, Object> getProtoFieldValueSetter(
          TypeDescriptor<?> typeDescriptor,
          Field field,
          Multimap<String, Method> methods,
          Class<ProtoBuilderT> builderClass) {
    if (field.getType().isLogicalType(OneOfType.IDENTIFIER)) {
      OneOfType oneOfType = field.getType().getLogicalType(OneOfType.class);
      TreeMap<Integer, FieldValueSetter<ProtoBuilderT, Object>> oneOfSetters = Maps.newTreeMap();
      for (Field oneOfField : oneOfType.getOneOfSchema().getFields()) {
        FieldValueSetter<ProtoBuilderT, Object> setter =
            getProtoFieldValueSetter(typeDescriptor, oneOfField, methods, builderClass);
        oneOfSetters.put(getFieldNumber(oneOfField), setter);
      }
      return createOneOfSetter(field.getName(), oneOfSetters, builderClass);
    } else {
      Method method = getProtoSetter(methods, field.getName(), field.getType());
      return JavaBeanUtils.createSetter(
          FieldValueTypeInformation.forSetter(
              typeDescriptor, method, protoSetterPrefix(field.getType())),
          new ProtoTypeConversionsFactory());
    }
  }

  static <ProtoBuilderT extends MessageLite.Builder> SchemaUserTypeCreator createBuilderCreator(
      Class<?> protoClass,
      Class<ProtoBuilderT> builderClass,
      List<FieldValueSetter<ProtoBuilderT, Object>> setters,
      Schema schema) {
    try {
      DynamicType.Builder<Supplier<ProtoBuilderT>> builder =
          (DynamicType.Builder)
              BYTE_BUDDY
                  .with(new InjectPackageStrategy(builderClass))
                  .subclass(Supplier.class)
                  .method(ElementMatchers.named("get"))
                  .intercept(new BuilderSupplier(protoClass));
      Supplier<ProtoBuilderT> supplier =
          builder
              .visit(
                  new AsmVisitorWrapper.ForDeclaredMethods()
                      .writerFlags(ClassWriter.COMPUTE_FRAMES))
              .make()
              .load(ReflectHelpers.findClassLoader(), ClassLoadingStrategy.Default.INJECTION)
              .getLoaded()
              .getDeclaredConstructor()
              .newInstance();
      return new ProtoCreatorFactory<>(supplier, setters);
    } catch (InstantiationException
        | IllegalAccessException
        | NoSuchMethodException
        | InvocationTargetException e) {
      throw new RuntimeException(
          "Unable to generate a creator for class " + builderClass + " with schema " + schema);
    }
  }

  static class ProtoCreatorFactory<ProtoBuilderT extends MessageLite.Builder>
      implements SchemaUserTypeCreator {
    private final Supplier<ProtoBuilderT> builderCreator;
    private final List<FieldValueSetter<ProtoBuilderT, Object>> setters;

    public ProtoCreatorFactory(
        Supplier<ProtoBuilderT> builderCreator,
        List<FieldValueSetter<ProtoBuilderT, Object>> setters) {
      this.builderCreator = builderCreator;
      this.setters = setters;
    }

    @Override
    public Object create(Object... params) {
      ProtoBuilderT builder = builderCreator.get();
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
