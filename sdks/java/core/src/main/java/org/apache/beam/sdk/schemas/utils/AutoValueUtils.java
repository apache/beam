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
package org.apache.beam.sdk.schemas.utils;

import static org.apache.beam.sdk.util.ByteBuddyUtils.getClassLoadingStrategy;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.asm.AsmVisitorWrapper.ForDeclaredMethods;
import net.bytebuddy.description.method.MethodDescription.ForLoadedMethod;
import net.bytebuddy.description.type.TypeDescription.ForLoadedType;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.dynamic.scaffold.InstrumentedType;
import net.bytebuddy.implementation.Implementation;
import net.bytebuddy.implementation.bytecode.ByteCodeAppender;
import net.bytebuddy.implementation.bytecode.ByteCodeAppender.Size;
import net.bytebuddy.implementation.bytecode.Duplication;
import net.bytebuddy.implementation.bytecode.Removal;
import net.bytebuddy.implementation.bytecode.StackManipulation;
import net.bytebuddy.implementation.bytecode.TypeCreation;
import net.bytebuddy.implementation.bytecode.assign.TypeCasting;
import net.bytebuddy.implementation.bytecode.collection.ArrayAccess;
import net.bytebuddy.implementation.bytecode.constant.IntegerConstant;
import net.bytebuddy.implementation.bytecode.member.MethodInvocation;
import net.bytebuddy.implementation.bytecode.member.MethodReturn;
import net.bytebuddy.implementation.bytecode.member.MethodVariableAccess;
import net.bytebuddy.jar.asm.ClassWriter;
import net.bytebuddy.matcher.ElementMatchers;
import org.apache.beam.sdk.schemas.FieldValueTypeInformation;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaUserTypeCreator;
import org.apache.beam.sdk.schemas.utils.ByteBuddyUtils.DefaultTypeConversionsFactory;
import org.apache.beam.sdk.schemas.utils.ByteBuddyUtils.InjectPackageStrategy;
import org.apache.beam.sdk.schemas.utils.ByteBuddyUtils.TypeConversion;
import org.apache.beam.sdk.schemas.utils.ByteBuddyUtils.TypeConversionsFactory;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Utilities for managing AutoValue schemas. */
@SuppressWarnings({"rawtypes"})
public class AutoValueUtils {
  public static @Nullable TypeDescriptor<?> getBaseAutoValueClass(
      TypeDescriptor<?> typeDescriptor) {
    // AutoValue extensions may be nested
    @Nullable TypeDescriptor<?> baseTypeDescriptor = typeDescriptor;
    while (baseTypeDescriptor != null
        && baseTypeDescriptor.getRawType().getName().contains("AutoValue_")) {
      baseTypeDescriptor =
          Optional.ofNullable(baseTypeDescriptor.getRawType().getSuperclass())
              .map(TypeDescriptor::of)
              .orElse(null);
    }
    return baseTypeDescriptor;
  }

  private static TypeDescriptor<?> getAutoValueGenerated(TypeDescriptor<?> typeDescriptor) {
    String generatedClassName = getAutoValueGeneratedName(typeDescriptor.getRawType().getName());
    try {
      return TypeDescriptor.of(Class.forName(generatedClassName));
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException("AutoValue generated class not found: " + generatedClassName);
    }
  }

  private static @Nullable Class getAutoValueGeneratedBuilder(Class<?> clazz) {
    Class generated;
    try {
      generated = Class.forName(getAutoValueGeneratedName(clazz.getName()));
    } catch (ClassNotFoundException e) {
      return null;
    }
    // Find the first generated class
    Class base = generated;
    while (base != null && base.getName().contains("AutoValue_")) {
      generated = base;
      base = base.getSuperclass();
    }
    String builderName = generated.getName() + "$Builder";
    try {
      return Class.forName(builderName);
    } catch (ClassNotFoundException e) {
      return null;
    }
  }

  private static String getAutoValueGeneratedName(String baseClass) {
    int lastDot = baseClass.lastIndexOf('.');
    String packageName = baseClass.substring(0, lastDot);
    String baseName = baseClass.substring(lastDot + 1, baseClass.length());
    baseName = baseName.replace('$', '_');
    return packageName + ".AutoValue_" + baseName;
  }

  /**
   * Try to find an accessible constructor for creating an AutoValue class. Otherwise return null.
   */
  public static @Nullable SchemaUserTypeCreator getConstructorCreator(
      TypeDescriptor<?> typeDescriptor,
      Schema schema,
      FieldValueTypeSupplier fieldValueTypeSupplier) {
    TypeDescriptor<?> generatedTypeDescriptor = getAutoValueGenerated(typeDescriptor);
    List<FieldValueTypeInformation> schemaTypes =
        fieldValueTypeSupplier.get(typeDescriptor, schema);
    Optional<Constructor<?>> constructor =
        Arrays.stream(generatedTypeDescriptor.getRawType().getDeclaredConstructors())
            .filter(c -> !Modifier.isPrivate(c.getModifiers()))
            .filter(c -> matchConstructor(c, schemaTypes))
            .findAny();
    return constructor
        .map(
            c ->
                JavaBeanUtils.getConstructorCreator(
                    generatedTypeDescriptor,
                    c,
                    schema,
                    fieldValueTypeSupplier,
                    new DefaultTypeConversionsFactory()))
        .orElse(null);
  }

  private static boolean matchConstructor(
      Constructor<?> constructor, List<FieldValueTypeInformation> getterTypes) {
    if (constructor.getParameters().length != getterTypes.size()) {
      return false;
    }

    Map<String, FieldValueTypeInformation> typeMap =
        getterTypes.stream()
            .collect(
                Collectors.toMap(
                    f ->
                        ReflectUtils.stripGetterPrefix(
                            Preconditions.checkNotNull(
                                    f.getMethod(), JavaBeanUtils.GETTER_WITH_NULL_METHOD_ERROR)
                                .getName()),
                    Function.identity()));

    boolean valid = true;
    // Verify that constructor parameters match (name and type) the inferred schema.
    for (Parameter parameter : constructor.getParameters()) {
      FieldValueTypeInformation type = typeMap.get(parameter.getName());
      if (type == null || type.getRawType() != parameter.getType()) {
        valid = false;
        break;
      }
    }
    if (valid) {
      return valid;
    }

    // Extensions add a $ suffix
    for (Parameter parameter : constructor.getParameters()) {
      String name = parameter.getName();
      if (!name.endsWith("$")) {
        return false;
      }
      name = name.substring(0, name.length() - 1);
      FieldValueTypeInformation type = typeMap.get(name);
      if (type == null || type.getRawType() != parameter.getType()) {
        return false;
      }
    }

    return true;
  }

  /**
   * Try to find an accessible builder class for creating an AutoValue class. Otherwise return null.
   */
  public static @Nullable SchemaUserTypeCreator getBuilderCreator(
      Class<?> clazz, Schema schema, FieldValueTypeSupplier fieldValueTypeSupplier) {
    Class<?> builderClass = getAutoValueGeneratedBuilder(clazz);
    if (builderClass == null) {
      return null;
    }

    Map<String, FieldValueTypeInformation> setterTypes =
        ReflectUtils.getMethods(builderClass).stream()
            .filter(ReflectUtils::isSetter)
            .map(m -> FieldValueTypeInformation.forSetter(TypeDescriptor.of(builderClass), m))
            .collect(Collectors.toMap(FieldValueTypeInformation::getName, Function.identity()));

    List<FieldValueTypeInformation> setterMethods =
        Lists.newArrayList(); // The builder methods to call in order.
    List<FieldValueTypeInformation> schemaTypes =
        fieldValueTypeSupplier.get(TypeDescriptor.of(clazz), schema);
    for (FieldValueTypeInformation type : schemaTypes) {
      String autoValueFieldName =
          ReflectUtils.stripGetterPrefix(
              Preconditions.checkNotNull(
                      type.getMethod(), JavaBeanUtils.GETTER_WITH_NULL_METHOD_ERROR)
                  .getName());

      FieldValueTypeInformation setterType = setterTypes.get(autoValueFieldName);
      if (setterType == null) {
        throw new RuntimeException(
            "AutoValue builder class "
                + builderClass
                + " did not contain "
                + "a setter for "
                + autoValueFieldName);
      }
      setterMethods.add(setterType);
    }

    Method buildMethod =
        ReflectUtils.getMethods(builderClass).stream()
            .filter(m -> m.getName().equals("build"))
            .findAny()
            .orElseThrow(() -> new RuntimeException("No build method in builder"));
    return createBuilderCreator(builderClass, setterMethods, buildMethod, schema, schemaTypes);
  }

  private static final ByteBuddy BYTE_BUDDY = new ByteBuddy();

  private static SchemaUserTypeCreator createBuilderCreator(
      Class<?> builderClass,
      List<FieldValueTypeInformation> setterMethods,
      Method buildMethod,
      Schema schema,
      List<FieldValueTypeInformation> types) {
    try {
      DynamicType.Builder<SchemaUserTypeCreator> builder =
          BYTE_BUDDY
              .with(new InjectPackageStrategy(builderClass))
              .subclass(SchemaUserTypeCreator.class)
              .method(ElementMatchers.named("create"))
              .intercept(
                  new BuilderCreateInstruction(types, setterMethods, builderClass, buildMethod));
      return builder
          .visit(new ForDeclaredMethods().writerFlags(ClassWriter.COMPUTE_FRAMES))
          .make()
          .load(ReflectHelpers.findClassLoader(), getClassLoadingStrategy(builderClass))
          .getLoaded()
          .getDeclaredConstructor()
          .newInstance();
    } catch (InstantiationException
        | IllegalAccessException
        | NoSuchMethodException
        | InvocationTargetException e) {
      throw new RuntimeException(
          "Unable to generate a creator for class " + builderClass + " with schema " + schema);
    }
  }

  static class BuilderCreateInstruction implements Implementation {
    private final List<FieldValueTypeInformation> setters;
    private final Class<?> builderClass;
    private final Method buildMethod;

    BuilderCreateInstruction(
        List<FieldValueTypeInformation> types,
        List<FieldValueTypeInformation> setters,
        Class<?> builderClass,
        Method buildMethod) {
      this.setters = setters;
      this.builderClass = builderClass;
      this.buildMethod = buildMethod;
    }

    @Override
    public InstrumentedType prepare(InstrumentedType instrumentedType) {
      return instrumentedType;
    }

    @Override
    public ByteCodeAppender appender(final Target implementationTarget) {
      TypeConversionsFactory typeConversionsFactory = new DefaultTypeConversionsFactory();
      ForLoadedType loadedBuilder = new ForLoadedType(builderClass);
      return (methodVisitor, implementationContext, instrumentedMethod) -> {
        // this + method parameters.
        int numLocals = 1 + instrumentedMethod.getParameters().size();

        StackManipulation stackManipulation =
            new StackManipulation.Compound(
                TypeCreation.of(loadedBuilder),
                Duplication.SINGLE,
                MethodInvocation.invoke(
                    loadedBuilder
                        .getDeclaredMethods()
                        .filter(
                            ElementMatchers.isConstructor().and(ElementMatchers.takesArguments(0)))
                        .getOnly()));

        TypeConversion<Type> convertType = typeConversionsFactory.createTypeConversion(true);
        for (int i = 0; i < setters.size(); ++i) {
          Method setterMethod = checkNotNull(setters.get(i).getMethod());
          Parameter parameter = setterMethod.getParameters()[0];
          ForLoadedType convertedType =
              new ForLoadedType(
                  (Class) convertType.convert(TypeDescriptor.of(parameter.getParameterizedType())));

          StackManipulation readParameter =
              new StackManipulation.Compound(
                  MethodVariableAccess.REFERENCE.loadFrom(1),
                  IntegerConstant.forValue(i),
                  ArrayAccess.REFERENCE.load(),
                  TypeCasting.to(convertedType));

          stackManipulation =
              new StackManipulation.Compound(
                  stackManipulation,
                  Duplication.SINGLE,
                  typeConversionsFactory
                      .createSetterConversions(readParameter)
                      .convert(TypeDescriptor.of(parameter.getType())),
                  MethodInvocation.invoke(new ForLoadedMethod(setterMethod)),
                  Removal.SINGLE);
        }

        stackManipulation =
            new StackManipulation.Compound(
                stackManipulation,
                MethodInvocation.invoke(new ForLoadedMethod(buildMethod)),
                MethodReturn.REFERENCE);

        StackManipulation.Size size = stackManipulation.apply(methodVisitor, implementationContext);
        return new Size(size.getMaximalSize(), numLocals);
      };
    }
  }
}
