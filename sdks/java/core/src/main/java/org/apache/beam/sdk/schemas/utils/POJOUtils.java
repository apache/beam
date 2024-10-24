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

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.asm.AsmVisitorWrapper;
import net.bytebuddy.description.field.FieldDescription.ForLoadedField;
import net.bytebuddy.description.type.TypeDescription.ForLoadedType;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.dynamic.scaffold.InstrumentedType;
import net.bytebuddy.implementation.FixedValue;
import net.bytebuddy.implementation.Implementation;
import net.bytebuddy.implementation.bytecode.ByteCodeAppender;
import net.bytebuddy.implementation.bytecode.ByteCodeAppender.Size;
import net.bytebuddy.implementation.bytecode.Duplication;
import net.bytebuddy.implementation.bytecode.StackManipulation;
import net.bytebuddy.implementation.bytecode.TypeCreation;
import net.bytebuddy.implementation.bytecode.assign.TypeCasting;
import net.bytebuddy.implementation.bytecode.collection.ArrayAccess;
import net.bytebuddy.implementation.bytecode.constant.IntegerConstant;
import net.bytebuddy.implementation.bytecode.member.FieldAccess;
import net.bytebuddy.implementation.bytecode.member.MethodInvocation;
import net.bytebuddy.implementation.bytecode.member.MethodReturn;
import net.bytebuddy.implementation.bytecode.member.MethodVariableAccess;
import net.bytebuddy.jar.asm.ClassWriter;
import net.bytebuddy.matcher.ElementMatchers;
import org.apache.beam.sdk.schemas.FieldValueGetter;
import org.apache.beam.sdk.schemas.FieldValueSetter;
import org.apache.beam.sdk.schemas.FieldValueTypeInformation;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaUserTypeCreator;
import org.apache.beam.sdk.schemas.utils.ByteBuddyUtils.ConstructorCreateInstruction;
import org.apache.beam.sdk.schemas.utils.ByteBuddyUtils.InjectPackageStrategy;
import org.apache.beam.sdk.schemas.utils.ByteBuddyUtils.StaticFactoryMethodInstruction;
import org.apache.beam.sdk.schemas.utils.ByteBuddyUtils.TypeConversion;
import org.apache.beam.sdk.schemas.utils.ByteBuddyUtils.TypeConversionsFactory;
import org.apache.beam.sdk.schemas.utils.ReflectUtils.TypeDescriptorWithSchema;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.checkerframework.checker.nullness.qual.Nullable;

/** A set of utilities to generate getter and setter classes for POJOs. */
@SuppressWarnings({
  "nullness", // TODO(https://github.com/apache/beam/issues/20497)
  "rawtypes" // TODO(https://github.com/apache/beam/issues/20447)
})
public class POJOUtils {

  public static Schema schemaFromPojoClass(
      TypeDescriptor<?> typeDescriptor, FieldValueTypeSupplier fieldValueTypeSupplier) {
    return StaticSchemaInference.schemaFromClass(typeDescriptor, fieldValueTypeSupplier);
  }

  // Static ByteBuddy instance used by all helpers.
  private static final ByteBuddy BYTE_BUDDY = new ByteBuddy();

  private static final Map<TypeDescriptorWithSchema<?>, List<FieldValueTypeInformation>>
      CACHED_FIELD_TYPES = Maps.newConcurrentMap();

  public static List<FieldValueTypeInformation> getFieldTypes(
      TypeDescriptor<?> typeDescriptor,
      Schema schema,
      FieldValueTypeSupplier fieldValueTypeSupplier) {
    return CACHED_FIELD_TYPES.computeIfAbsent(
        TypeDescriptorWithSchema.create(typeDescriptor, schema),
        c -> fieldValueTypeSupplier.get(typeDescriptor, schema));
  }

  // The list of getters for a class is cached, so we only create the classes the first time
  // getSetters is called.
  private static final Map<TypeDescriptorWithSchema, List<FieldValueGetter>> CACHED_GETTERS =
      Maps.newConcurrentMap();

  public static List<FieldValueGetter> getGetters(
      TypeDescriptor<?> typeDescriptor,
      Schema schema,
      FieldValueTypeSupplier fieldValueTypeSupplier,
      TypeConversionsFactory typeConversionsFactory) {
    // Return the getters ordered by their position in the schema.
    return CACHED_GETTERS.computeIfAbsent(
        TypeDescriptorWithSchema.create(typeDescriptor, schema),
        c -> {
          List<FieldValueTypeInformation> types =
              fieldValueTypeSupplier.get(typeDescriptor, schema);
          List<FieldValueGetter> getters =
              types.stream()
                  .map(t -> createGetter(t, typeConversionsFactory))
                  .collect(Collectors.toList());
          if (getters.size() != schema.getFieldCount()) {
            throw new RuntimeException(
                "Was not able to generate getters for schema: "
                    + schema
                    + " class: "
                    + typeDescriptor);
          }
          return getters;
        });
  }

  // The list of constructors for a class is cached, so we only create the classes the first time
  // getConstructor is called.
  public static final Map<TypeDescriptorWithSchema, SchemaUserTypeCreator> CACHED_CREATORS =
      Maps.newConcurrentMap();

  public static <T> SchemaUserTypeCreator getSetFieldCreator(
      TypeDescriptor<T> typeDescriptor,
      Schema schema,
      FieldValueTypeSupplier fieldValueTypeSupplier,
      TypeConversionsFactory typeConversionsFactory) {
    return CACHED_CREATORS.computeIfAbsent(
        TypeDescriptorWithSchema.create(typeDescriptor, schema),
        c -> {
          List<FieldValueTypeInformation> types =
              fieldValueTypeSupplier.get(typeDescriptor, schema);
          return createSetFieldCreator(
              typeDescriptor.getRawType(), schema, types, typeConversionsFactory);
        });
  }

  private static <T> SchemaUserTypeCreator createSetFieldCreator(
      Class<T> clazz,
      Schema schema,
      List<FieldValueTypeInformation> types,
      TypeConversionsFactory typeConversionsFactory) {
    // Get the list of class fields ordered by schema.
    List<Field> fields =
        types.stream().map(FieldValueTypeInformation::getField).collect(Collectors.toList());
    try {
      DynamicType.Builder<SchemaUserTypeCreator> builder =
          BYTE_BUDDY
              .with(new InjectPackageStrategy(clazz))
              .subclass(SchemaUserTypeCreator.class)
              .method(ElementMatchers.named("create"))
              .intercept(new SetFieldCreateInstruction(fields, clazz, typeConversionsFactory));

      return builder
          .visit(new AsmVisitorWrapper.ForDeclaredMethods().writerFlags(ClassWriter.COMPUTE_FRAMES))
          .make()
          .load(
              ReflectHelpers.findClassLoader(clazz.getClassLoader()),
              getClassLoadingStrategy(clazz))
          .getLoaded()
          .getDeclaredConstructor()
          .newInstance();
    } catch (InstantiationException
        | IllegalAccessException
        | IllegalStateException
        | NoSuchMethodException
        | InvocationTargetException e) {
      throw new RuntimeException(
          String.format(
              "Unable to generate a creator for POJO '%s' with inferred schema: %s%nNote POJOs must have a zero-argument constructor, or a constructor annotated with @SchemaCreate.",
              clazz, schema));
    }
  }

  public static SchemaUserTypeCreator getConstructorCreator(
      TypeDescriptor<?> typeDescriptor,
      Constructor constructor,
      Schema schema,
      FieldValueTypeSupplier fieldValueTypeSupplier,
      TypeConversionsFactory typeConversionsFactory) {
    return CACHED_CREATORS.computeIfAbsent(
        TypeDescriptorWithSchema.create(typeDescriptor, schema),
        c -> {
          List<FieldValueTypeInformation> types =
              fieldValueTypeSupplier.get(typeDescriptor, schema);
          return createConstructorCreator(
              typeDescriptor.getRawType(), constructor, schema, types, typeConversionsFactory);
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
              getClassLoadingStrategy(clazz))
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

  public static SchemaUserTypeCreator getStaticCreator(
      TypeDescriptor<?> typeDescriptor,
      Method creator,
      Schema schema,
      FieldValueTypeSupplier fieldValueTypeSupplier,
      TypeConversionsFactory typeConversionsFactory) {
    return CACHED_CREATORS.computeIfAbsent(
        TypeDescriptorWithSchema.create(typeDescriptor, schema),
        c -> {
          List<FieldValueTypeInformation> types =
              fieldValueTypeSupplier.get(typeDescriptor, schema);
          return createStaticCreator(
              typeDescriptor.getRawType(), creator, schema, types, typeConversionsFactory);
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
          .load(ReflectHelpers.findClassLoader(), getClassLoadingStrategy(clazz))
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

  /**
   * Generate the following {@link FieldValueSetter} class for the {@link Field}.
   *
   * <pre><code>
   *   class Getter implements {@literal FieldValueGetter<POJO, FieldType>} {
   *     {@literal @}Override public String name() { return field.getName(); }
   *     {@literal @}Override public Class type() { return field.getType(); }
   *     {@literal @}Override public FieldType get(POJO pojo) {
   *        return convert(pojo.field);
   *      }
   *   }
   * </code></pre>
   */
  @SuppressWarnings("unchecked")
  static @Nullable <ObjectT, ValueT> FieldValueGetter<ObjectT, ValueT> createGetter(
      FieldValueTypeInformation typeInformation, TypeConversionsFactory typeConversionsFactory) {
    Field field = typeInformation.getField();
    DynamicType.Builder<FieldValueGetter> builder =
        ByteBuddyUtils.subclassGetterInterface(
            BYTE_BUDDY,
            field.getDeclaringClass(),
            typeConversionsFactory
                .createTypeConversion(false)
                .convert(TypeDescriptor.of(field.getType())));
    builder =
        implementGetterMethods(builder, field, typeInformation.getName(), typeConversionsFactory);
    try {
      return builder
          .visit(new AsmVisitorWrapper.ForDeclaredMethods().writerFlags(ClassWriter.COMPUTE_FRAMES))
          .make()
          .load(
              ReflectHelpers.findClassLoader(field.getDeclaringClass().getClassLoader()),
              getClassLoadingStrategy(field.getDeclaringClass()))
          .getLoaded()
          .getDeclaredConstructor()
          .newInstance();
    } catch (InstantiationException
        | IllegalAccessException
        | NoSuchMethodException
        | InvocationTargetException e) {
      throw new RuntimeException("Unable to generate a getter for field '" + field + "'.", e);
    }
  }

  private static DynamicType.Builder<FieldValueGetter> implementGetterMethods(
      DynamicType.Builder<FieldValueGetter> builder,
      Field field,
      String name,
      TypeConversionsFactory typeConversionsFactory) {
    return builder
        .visit(new AsmVisitorWrapper.ForDeclaredMethods().writerFlags(ClassWriter.COMPUTE_FRAMES))
        .method(ElementMatchers.named("name"))
        .intercept(FixedValue.reference(name))
        .method(ElementMatchers.named("get"))
        .intercept(new ReadFieldInstruction(field, typeConversionsFactory));
  }

  // The list of setters for a class is cached, so we only create the classes the first time
  // getSetters is called.
  private static final Map<TypeDescriptorWithSchema, List<FieldValueSetter>> CACHED_SETTERS =
      Maps.newConcurrentMap();

  public static List<FieldValueSetter> getSetters(
      TypeDescriptor<?> typeDescriptor,
      Schema schema,
      FieldValueTypeSupplier fieldValueTypeSupplier,
      TypeConversionsFactory typeConversionsFactory) {
    // Return the setters, ordered by their position in the schema.
    return CACHED_SETTERS.computeIfAbsent(
        TypeDescriptorWithSchema.create(typeDescriptor, schema),
        c -> {
          List<FieldValueTypeInformation> types =
              fieldValueTypeSupplier.get(typeDescriptor, schema);
          return types.stream()
              .map(t -> createSetter(t, typeConversionsFactory))
              .collect(Collectors.toList());
        });
  }

  /**
   * Generate the following {@link FieldValueSetter} class for the {@link Field}.
   *
   * <pre><code>
   *   class Setter implements {@literal FieldValueSetter<POJO, FieldType>} {
   *     {@literal @}Override public String name() { return field.getName(); }
   *     {@literal @}Override public Class type() { return field.getType(); }
   *     {@literal @}Override public Type elementType() { return elementType; }
   *     {@literal @}Override public Type mapKeyType() { return mapKeyType; }
   *     {@literal @}Override public Type mapValueType() { return mapValueType; }
   *     {@literal @}Override public void set(POJO pojo, FieldType value) {
   *        pojo.field = convert(value);
   *      }
   *   }
   * </code></pre>
   */
  @SuppressWarnings("unchecked")
  private static <ObjectT, ValueT> FieldValueSetter<ObjectT, ValueT> createSetter(
      FieldValueTypeInformation typeInformation, TypeConversionsFactory typeConversionsFactory) {
    Field field = typeInformation.getField();
    DynamicType.Builder<FieldValueSetter> builder =
        ByteBuddyUtils.subclassSetterInterface(
            BYTE_BUDDY,
            field.getDeclaringClass(),
            typeConversionsFactory
                .createTypeConversion(false)
                .convert(TypeDescriptor.of(field.getType())));
    builder = implementSetterMethods(builder, field, typeConversionsFactory);
    try {
      return builder
          .visit(new AsmVisitorWrapper.ForDeclaredMethods().writerFlags(ClassWriter.COMPUTE_FRAMES))
          .make()
          .load(
              ReflectHelpers.findClassLoader(field.getDeclaringClass().getClassLoader()),
              getClassLoadingStrategy(field.getDeclaringClass()))
          .getLoaded()
          .getDeclaredConstructor()
          .newInstance();
    } catch (InstantiationException
        | IllegalAccessException
        | NoSuchMethodException
        | InvocationTargetException e) {
      throw new RuntimeException("Unable to generate a getter for field '" + field + "'.", e);
    }
  }

  private static DynamicType.Builder<FieldValueSetter> implementSetterMethods(
      DynamicType.Builder<FieldValueSetter> builder,
      Field field,
      TypeConversionsFactory typeConversionsFactory) {
    return builder
        .visit(new AsmVisitorWrapper.ForDeclaredMethods().writerFlags(ClassWriter.COMPUTE_FRAMES))
        .method(ElementMatchers.named("name"))
        .intercept(FixedValue.reference(field.getName()))
        .method(ElementMatchers.named("set"))
        .intercept(new SetFieldInstruction(field, typeConversionsFactory));
  }

  // Implements a method to read a public field out of an object.
  static class ReadFieldInstruction implements Implementation {
    // Field that will be read.
    private final Field field;
    private final TypeConversionsFactory typeConversionsFactory;

    ReadFieldInstruction(Field field, TypeConversionsFactory typeConversionsFactory) {
      this.field = field;
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
                // Read the field from the object.
                FieldAccess.forField(new ForLoadedField(field)).read());

        StackManipulation stackManipulation =
            new StackManipulation.Compound(
                typeConversionsFactory
                    .createGetterConversions(readValue)
                    .convert(TypeDescriptor.of(field.getGenericType())),
                MethodReturn.REFERENCE);

        StackManipulation.Size size = stackManipulation.apply(methodVisitor, implementationContext);
        return new Size(size.getMaximalSize(), numLocals);
      };
    }
  }

  // Implements a method to set a public field in an object.
  static class SetFieldInstruction implements Implementation {
    // Field that will be read.
    private Field field;
    private final TypeConversionsFactory typeConversionsFactory;

    SetFieldInstruction(Field field, TypeConversionsFactory typeConversionsFactory) {
      this.field = field;
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

        // Read the object onto the stack.
        StackManipulation stackManipulation =
            new StackManipulation.Compound(
                // Object param is offset 1.
                MethodVariableAccess.REFERENCE.loadFrom(1),
                // Do any conversions necessary.
                typeConversionsFactory
                    .createSetterConversions(readField)
                    .convert(TypeDescriptor.of(field.getType())),
                // Now update the field and return void.
                FieldAccess.forField(new ForLoadedField(field)).write(),
                MethodReturn.VOID);

        StackManipulation.Size size = stackManipulation.apply(methodVisitor, implementationContext);
        return new Size(size.getMaximalSize(), numLocals);
      };
    }
  }

  // Implements a method to construct an object.
  static class SetFieldCreateInstruction implements Implementation {
    private final List<Field> fields;
    private final Class pojoClass;
    private final TypeConversionsFactory typeConversionsFactory;

    SetFieldCreateInstruction(
        List<Field> fields, Class pojoClass, TypeConversionsFactory typeConversionsFactory) {
      this.fields = fields;
      this.pojoClass = pojoClass;
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

        // Create the POJO class.
        ForLoadedType loadedType = new ForLoadedType(pojoClass);
        StackManipulation stackManipulation =
            new StackManipulation.Compound(
                TypeCreation.of(loadedType),
                Duplication.SINGLE,
                MethodInvocation.invoke(
                    loadedType
                        .getDeclaredMethods()
                        .filter(
                            ElementMatchers.isConstructor().and(ElementMatchers.takesArguments(0)))
                        .getOnly()));

        // The types in the POJO might be the types returned by Beam's Row class,
        // so we have to convert the types used by Beam's Row class.
        TypeConversion<Type> convertType = typeConversionsFactory.createTypeConversion(true);
        for (int i = 0; i < fields.size(); ++i) {
          Field field = fields.get(i);

          ForLoadedType convertedType =
              new ForLoadedType((Class) convertType.convert(TypeDescriptor.of(field.getType())));

          // The instruction to read the parameter.
          StackManipulation readParameter =
              new StackManipulation.Compound(
                  MethodVariableAccess.REFERENCE.loadFrom(1),
                  IntegerConstant.forValue(i),
                  ArrayAccess.REFERENCE.load(),
                  TypeCasting.to(convertedType));

          StackManipulation updateField =
              new StackManipulation.Compound(
                  // Duplicate object reference.
                  Duplication.SINGLE,
                  // Do any conversions necessary.
                  typeConversionsFactory
                      .createSetterConversions(readParameter)
                      .convert(TypeDescriptor.of(field.getType())),
                  // Now update the field.
                  FieldAccess.forField(new ForLoadedField(field)).write());
          stackManipulation = new StackManipulation.Compound(stackManipulation, updateField);
        }
        stackManipulation =
            new StackManipulation.Compound(stackManipulation, MethodReturn.REFERENCE);

        StackManipulation.Size size = stackManipulation.apply(methodVisitor, implementationContext);
        return new Size(size.getMaximalSize(), numLocals);
      };
    }
  }
}
