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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.schemas.FieldValueGetter;
import org.apache.beam.sdk.schemas.FieldValueSetter;
import org.apache.beam.sdk.schemas.FieldValueTypeInformation;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaUserTypeCreator;
import org.apache.beam.sdk.schemas.utils.ByteBuddyUtils.ConstructorCreateInstruction;
import org.apache.beam.sdk.schemas.utils.ByteBuddyUtils.InjectPackageStrategy;
import org.apache.beam.sdk.schemas.utils.ByteBuddyUtils.StaticFactoryMethodInstruction;
import org.apache.beam.sdk.schemas.utils.ByteBuddyUtils.TypeConversionsFactory;
import org.apache.beam.sdk.schemas.utils.ReflectUtils.ClassWithSchema;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.ByteBuddy;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.asm.AsmVisitorWrapper;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.description.method.MethodDescription.ForLoadedMethod;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.dynamic.DynamicType;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.dynamic.scaffold.InstrumentedType;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.implementation.FixedValue;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.implementation.Implementation;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.implementation.bytecode.ByteCodeAppender;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.implementation.bytecode.ByteCodeAppender.Size;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.implementation.bytecode.Removal;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.implementation.bytecode.StackManipulation;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.implementation.bytecode.member.MethodInvocation;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.implementation.bytecode.member.MethodReturn;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.implementation.bytecode.member.MethodVariableAccess;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.jar.asm.ClassWriter;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.matcher.ElementMatchers;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;

/** A set of utilities to generate getter and setter classes for JavaBean objects. */
@Experimental(Kind.SCHEMAS)
public class JavaBeanUtils {
  /** Create a {@link Schema} for a Java Bean class. */
  public static Schema schemaFromJavaBeanClass(
      Class<?> clazz, FieldValueTypeSupplier fieldValueTypeSupplier) {
    return StaticSchemaInference.schemaFromClass(clazz, fieldValueTypeSupplier);
  }

  // Make sure that there are matching setters and getters.
  public static void validateJavaBean(
      List<FieldValueTypeInformation> getters,
      List<FieldValueTypeInformation> setters,
      Schema schema) {

    Map<String, FieldValueTypeInformation> setterMap = new HashMap<>();
    int bound = schema.getFieldCount();
    for (int i = 0; i < bound; i++) {
      Integer integer = i;
      if (setterMap.put(schema.getField(integer).getName(), setters.get(integer)) != null) {
        throw new IllegalStateException("Duplicate key");
      }
    }

    final String HELP_STRING =
        "In order to infer a Schema from a Java Bean, it must have a constructor annotated with @SchemaCreate, or it must have a compatible setter for every getter used as a Schema field.";

    for (FieldValueTypeInformation type : getters) {
      FieldValueTypeInformation setterType = setterMap.get(type.getName());
      if (setterType == null) {
        throw new RuntimeException(
            String.format(
                "Java Bean '%s' contains a getter for field '%s', but does not contain a matching setter. %s",
                type.getMethod().getDeclaringClass(), type.getName(), HELP_STRING));
      }
      if (!type.getType().equals(setterType.getType())) {
        throw new RuntimeException(
            String.format(
                "Java Bean '%s' contains a setter for field '%s' that has a mismatching type. %s",
                type.getMethod().getDeclaringClass(), type.getName(), HELP_STRING));
      }
      if (!type.isNullable() == setterType.isNullable()) {
        throw new RuntimeException(
            String.format(
                "Java Bean '%s' contains a setter for field '%s' that has a mismatching nullable attribute. %s",
                type.getMethod().getDeclaringClass(), type.getName(), HELP_STRING));
      }
    }
  }

  // Static ByteBuddy instance used by all helpers.
  private static final ByteBuddy BYTE_BUDDY = new ByteBuddy();

  private static final Map<ClassWithSchema, List<FieldValueTypeInformation>> CACHED_FIELD_TYPES =
      Maps.newConcurrentMap();

  public static List<FieldValueTypeInformation> getFieldTypes(
      Class<?> clazz, Schema schema, FieldValueTypeSupplier fieldValueTypeSupplier) {
    return CACHED_FIELD_TYPES.computeIfAbsent(
        ClassWithSchema.create(clazz, schema), c -> fieldValueTypeSupplier.get(clazz, schema));
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
    return CACHED_GETTERS.computeIfAbsent(
        ClassWithSchema.create(clazz, schema),
        c -> {
          List<FieldValueTypeInformation> types = fieldValueTypeSupplier.get(clazz, schema);
          return types.stream()
              .map(t -> createGetter(t, typeConversionsFactory))
              .collect(Collectors.toList());
        });
  }

  public static <T> FieldValueGetter createGetter(
      FieldValueTypeInformation typeInformation, TypeConversionsFactory typeConversionsFactory) {
    DynamicType.Builder<FieldValueGetter> builder =
        ByteBuddyUtils.subclassGetterInterface(
            BYTE_BUDDY,
            typeInformation.getMethod().getDeclaringClass(),
            typeConversionsFactory.createTypeConversion(false).convert(typeInformation.getType()));
    builder = implementGetterMethods(builder, typeInformation, typeConversionsFactory);
    try {
      return builder
          .visit(new AsmVisitorWrapper.ForDeclaredMethods().writerFlags(ClassWriter.COMPUTE_FRAMES))
          .make()
          .load(
              ReflectHelpers.findClassLoader(
                  typeInformation.getMethod().getDeclaringClass().getClassLoader()),
              ClassLoadingStrategy.Default.INJECTION)
          .getLoaded()
          .getDeclaredConstructor()
          .newInstance();
    } catch (InstantiationException
        | IllegalAccessException
        | NoSuchMethodException
        | InvocationTargetException e) {
      throw new RuntimeException(
          "Unable to generate a getter for getter '" + typeInformation.getMethod() + "'");
    }
  }

  private static DynamicType.Builder<FieldValueGetter> implementGetterMethods(
      DynamicType.Builder<FieldValueGetter> builder,
      FieldValueTypeInformation typeInformation,
      TypeConversionsFactory typeConversionsFactory) {
    return builder
        .method(ElementMatchers.named("name"))
        .intercept(FixedValue.reference(typeInformation.getName()))
        .method(ElementMatchers.named("get"))
        .intercept(new InvokeGetterInstruction(typeInformation, typeConversionsFactory));
  }

  // The list of setters for a class is cached, so we only create the classes the first time
  // getSetters is called.
  private static final Map<ClassWithSchema, List<FieldValueSetter>> CACHED_SETTERS =
      Maps.newConcurrentMap();

  /**
   * Return the list of {@link FieldValueSetter}s for a Java Bean class
   *
   * <p>The returned list is ordered by the order of fields in the schema.
   */
  public static List<FieldValueSetter> getSetters(
      Class<?> clazz,
      Schema schema,
      FieldValueTypeSupplier fieldValueTypeSupplier,
      TypeConversionsFactory typeConversionsFactory) {
    return CACHED_SETTERS.computeIfAbsent(
        ClassWithSchema.create(clazz, schema),
        c -> {
          List<FieldValueTypeInformation> types = fieldValueTypeSupplier.get(clazz, schema);
          return types.stream()
              .map(t -> createSetter(t, typeConversionsFactory))
              .collect(Collectors.toList());
        });
  }

  public static FieldValueSetter createSetter(
      FieldValueTypeInformation typeInformation, TypeConversionsFactory typeConversionsFactory) {
    DynamicType.Builder<FieldValueSetter> builder =
        ByteBuddyUtils.subclassSetterInterface(
            BYTE_BUDDY,
            typeInformation.getMethod().getDeclaringClass(),
            typeConversionsFactory.createTypeConversion(false).convert(typeInformation.getType()));
    builder = implementSetterMethods(builder, typeInformation, typeConversionsFactory);
    try {
      return builder
          .visit(new AsmVisitorWrapper.ForDeclaredMethods().writerFlags(ClassWriter.COMPUTE_FRAMES))
          .make()
          .load(
              ReflectHelpers.findClassLoader(
                  typeInformation.getMethod().getDeclaringClass().getClassLoader()),
              ClassLoadingStrategy.Default.INJECTION)
          .getLoaded()
          .getDeclaredConstructor()
          .newInstance();
    } catch (InstantiationException
        | IllegalAccessException
        | NoSuchMethodException
        | InvocationTargetException e) {
      throw new RuntimeException(
          "Unable to generate a setter for setter '" + typeInformation.getMethod() + "'");
    }
  }

  private static DynamicType.Builder<FieldValueSetter> implementSetterMethods(
      DynamicType.Builder<FieldValueSetter> builder,
      FieldValueTypeInformation fieldValueTypeInformation,
      TypeConversionsFactory typeConversionsFactory) {
    return builder
        .method(ElementMatchers.named("name"))
        .intercept(FixedValue.reference(fieldValueTypeInformation.getName()))
        .method(ElementMatchers.named("set"))
        .intercept(new InvokeSetterInstruction(fieldValueTypeInformation, typeConversionsFactory));
  }

  // The list of constructors for a class is cached, so we only create the classes the first time
  // getConstructor is called.
  public static final Map<ClassWithSchema, SchemaUserTypeCreator> CACHED_CREATORS =
      Maps.newConcurrentMap();

  public static SchemaUserTypeCreator getConstructorCreator(
      Class clazz,
      Constructor constructor,
      Schema schema,
      FieldValueTypeSupplier fieldValueTypeSupplier,
      TypeConversionsFactory typeConversionsFactory) {
    return CACHED_CREATORS.computeIfAbsent(
        ClassWithSchema.create(clazz, schema),
        c -> {
          List<FieldValueTypeInformation> types = fieldValueTypeSupplier.get(clazz, schema);
          return createConstructorCreator(
              clazz, constructor, schema, types, typeConversionsFactory);
        });
  }

  public static <T> SchemaUserTypeCreator createConstructorCreator(
      Class<T> clazz,
      Constructor<T> constructor,
      Schema schema,
      List<FieldValueTypeInformation> types,
      TypeConversionsFactory typeConversionsFactory) {
    try {
      DynamicType.Builder<SchemaUserTypeCreator> builder =
          BYTE_BUDDY
              .with(new InjectPackageStrategy(clazz))
              .subclass(SchemaUserTypeCreator.class)
              .method(ElementMatchers.named("create"))
              .intercept(
                  new ConstructorCreateInstruction(
                      types, clazz, constructor, typeConversionsFactory));
      return builder
          .visit(new AsmVisitorWrapper.ForDeclaredMethods().writerFlags(ClassWriter.COMPUTE_FRAMES))
          .make()
          .load(
              ReflectHelpers.findClassLoader(clazz.getClassLoader()),
              ClassLoadingStrategy.Default.INJECTION)
          .getLoaded()
          .getDeclaredConstructor()
          .newInstance();
    } catch (InstantiationException
        | IllegalAccessException
        | NoSuchMethodException
        | InvocationTargetException e) {
      throw new RuntimeException(
          "Unable to generate a creator for class " + clazz + " with schema " + schema);
    }
  }

  public static SchemaUserTypeCreator getStaticCreator(
      Class clazz,
      Method creator,
      Schema schema,
      FieldValueTypeSupplier fieldValueTypeSupplier,
      TypeConversionsFactory typeConversionsFactory) {
    return CACHED_CREATORS.computeIfAbsent(
        ClassWithSchema.create(clazz, schema),
        c -> {
          List<FieldValueTypeInformation> types = fieldValueTypeSupplier.get(clazz, schema);
          return createStaticCreator(clazz, creator, schema, types, typeConversionsFactory);
        });
  }

  public static <T> SchemaUserTypeCreator createStaticCreator(
      Class<T> clazz,
      Method creator,
      Schema schema,
      List<FieldValueTypeInformation> types,
      TypeConversionsFactory typeConversionsFactory) {
    try {
      DynamicType.Builder<SchemaUserTypeCreator> builder =
          BYTE_BUDDY
              .with(new InjectPackageStrategy(clazz))
              .subclass(SchemaUserTypeCreator.class)
              .method(ElementMatchers.named("create"))
              .intercept(
                  new StaticFactoryMethodInstruction(
                      types, clazz, creator, typeConversionsFactory));

      return builder
          .visit(new AsmVisitorWrapper.ForDeclaredMethods().writerFlags(ClassWriter.COMPUTE_FRAMES))
          .make()
          .load(
              ReflectHelpers.findClassLoader(clazz.getClassLoader()),
              ClassLoadingStrategy.Default.INJECTION)
          .getLoaded()
          .getDeclaredConstructor()
          .newInstance();
    } catch (InstantiationException
        | IllegalAccessException
        | NoSuchMethodException
        | InvocationTargetException e) {
      throw new RuntimeException(
          "Unable to generate a creator for " + clazz + " with schema " + schema);
    }
  }

  // Implements a method to read a public getter out of an object.
  private static class InvokeGetterInstruction implements Implementation {
    private final FieldValueTypeInformation typeInformation;
    private final TypeConversionsFactory typeConversionsFactory;

    InvokeGetterInstruction(
        FieldValueTypeInformation typeInformation, TypeConversionsFactory typeConversionsFactory) {
      this.typeInformation = typeInformation;
      this.typeConversionsFactory = typeConversionsFactory;
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

        // StackManipulation that will read the value from the class field.
        StackManipulation readValue =
            new StackManipulation.Compound(
                // Method param is offset 1 (offset 0 is the this parameter).
                MethodVariableAccess.REFERENCE.loadFrom(1),
                // Invoke the getter
                MethodInvocation.invoke(new ForLoadedMethod(typeInformation.getMethod())));

        StackManipulation stackManipulation =
            new StackManipulation.Compound(
                typeConversionsFactory
                    .createGetterConversions(readValue)
                    .convert(typeInformation.getType()),
                MethodReturn.REFERENCE);

        StackManipulation.Size size = stackManipulation.apply(methodVisitor, implementationContext);
        return new Size(size.getMaximalSize(), numLocals);
      };
    }
  }

  // Implements a method to write a public set out on an object.
  private static class InvokeSetterInstruction implements Implementation {
    // Setter method that will be invoked
    private FieldValueTypeInformation fieldValueTypeInformation;
    private final TypeConversionsFactory typeConversionsFactory;

    InvokeSetterInstruction(
        FieldValueTypeInformation fieldValueTypeInformation,
        TypeConversionsFactory typeConversionsFactory) {
      this.fieldValueTypeInformation = fieldValueTypeInformation;
      this.typeConversionsFactory = typeConversionsFactory;
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

        // The instruction to read the field.
        StackManipulation readField = MethodVariableAccess.REFERENCE.loadFrom(2);

        Method method = fieldValueTypeInformation.getMethod();
        boolean setterMethodReturnsVoid = method.getReturnType().equals(Void.TYPE);
        // Read the object onto the stack.
        StackManipulation stackManipulation =
            new StackManipulation.Compound(
                // Object param is offset 1.
                MethodVariableAccess.REFERENCE.loadFrom(1),
                // Do any conversions necessary.
                typeConversionsFactory
                    .createSetterConversions(readField)
                    .convert(fieldValueTypeInformation.getType()),
                // Now update the field and return void.
                MethodInvocation.invoke(new ForLoadedMethod(method)));
        if (!setterMethodReturnsVoid) {
          // Discard return type;
          stackManipulation = new StackManipulation.Compound(stackManipulation, Removal.SINGLE);
        }
        stackManipulation = new StackManipulation.Compound(stackManipulation, MethodReturn.VOID);

        StackManipulation.Size size = stackManipulation.apply(methodVisitor, implementationContext);
        return new Size(size.getMaximalSize(), numLocals);
      };
    }
  }
}
