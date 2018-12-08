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

import com.google.common.collect.Maps;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.field.FieldDescription.ForLoadedField;
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
import net.bytebuddy.implementation.bytecode.TypeCreation;
import net.bytebuddy.implementation.bytecode.assign.TypeCasting;
import net.bytebuddy.implementation.bytecode.collection.ArrayAccess;
import net.bytebuddy.implementation.bytecode.constant.IntegerConstant;
import net.bytebuddy.implementation.bytecode.member.FieldAccess;
import net.bytebuddy.implementation.bytecode.member.MethodInvocation;
import net.bytebuddy.implementation.bytecode.member.MethodReturn;
import net.bytebuddy.implementation.bytecode.member.MethodVariableAccess;
import net.bytebuddy.matcher.ElementMatchers;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.schemas.FieldValueGetter;
import org.apache.beam.sdk.schemas.FieldValueSetter;
import org.apache.beam.sdk.schemas.FieldValueTypeInformation;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaUserTypeCreator;
import org.apache.beam.sdk.schemas.utils.ByteBuddyUtils.ConvertType;
import org.apache.beam.sdk.schemas.utils.ByteBuddyUtils.ConvertValueForGetter;
import org.apache.beam.sdk.schemas.utils.ReflectUtils.ClassWithSchema;
import org.apache.beam.sdk.schemas.utils.StaticSchemaInference.TypeInformation;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.apache.beam.sdk.values.TypeDescriptor;

/** A set of utilities yo generate getter and setter classes for POJOs. */
@Experimental(Kind.SCHEMAS)
public class POJOUtils {
  public static Schema schemaFromPojoClass(Class<?> clazz) {
    // We should cache the field order.
    Function<Class, List<TypeInformation>> getTypesForClass =
        c ->
            ReflectUtils.getFields(c)
                .stream()
                .map(TypeInformation::forField)
                .collect(Collectors.toList());
    return StaticSchemaInference.schemaFromClass(clazz, getTypesForClass);
  }

  // Static ByteBuddy instance used by all helpers.
  private static final ByteBuddy BYTE_BUDDY = new ByteBuddy();

  private static final Map<ClassWithSchema, List<FieldValueTypeInformation>> CACHED_FIELD_TYPES =
      Maps.newConcurrentMap();

  public static List<FieldValueTypeInformation> getFieldTypes(
      Class<?> clazz, Schema schema, FieldNamePolicy fieldNamePolicy) {
    SerializableFunction<String, String> transformName = fieldNamePolicy.get(clazz, schema);
    return CACHED_FIELD_TYPES.computeIfAbsent(
        new ClassWithSchema(clazz, schema),
        c -> {
          Map<String, FieldValueTypeInformation> typeInformationMap =
              ReflectUtils.getFields(clazz)
                  .stream()
                  .map(f -> FieldValueTypeInformation.of(f, transformName))
                  .collect(
                      Collectors.toMap(FieldValueTypeInformation::getName, Function.identity()));
          return schema
              .getFields()
              .stream()
              .map(f -> typeInformationMap.get(f.getName()))
              .collect(Collectors.toList());
        });
  }

  // The list of getters for a class is cached, so we only create the classes the first time
  // getSetters is called.
  private static final Map<ClassWithSchema, List<FieldValueGetter>> CACHED_GETTERS =
      Maps.newConcurrentMap();

  public static List<FieldValueGetter> getGetters(
      Class<?> clazz, Schema schema, FieldNamePolicy fieldNamePolicy) {
    SerializableFunction<String, String> transformName = fieldNamePolicy.get(clazz, schema);
    // Return the getters ordered by their position in the schema.
    return CACHED_GETTERS.computeIfAbsent(
        new ClassWithSchema(clazz, schema),
        c -> {
          Map<String, FieldValueGetter> getterMap =
              ReflectUtils.getFields(clazz)
                  .stream()
                  .map(f -> POJOUtils.createGetter(f, transformName))
                  .filter(Objects::nonNull)
                  .collect(Collectors.toMap(FieldValueGetter::name, Function.identity()));
          List<FieldValueGetter> getters =
              schema
                  .getFields()
                  .stream()
                  .map(Schema.Field::getName)
                  .map(getterMap::get)
                  .filter(Objects::nonNull)
                  .collect(Collectors.toList());
          if (getters.size() != schema.getFieldCount()) {
            throw new RuntimeException(
                "Was not able to generate getters for schema: " + schema + " class: " + clazz);
          }
          return getters;
        });
  }

  // The list of constructors for a class is cached, so we only create the classes the first time
  // getConstructor is called.
  public static final Map<ClassWithSchema, SchemaUserTypeCreator> CACHED_CREATORS =
      Maps.newConcurrentMap();

  public static <T> SchemaUserTypeCreator getCreator(
      Class<T> clazz, Schema schema, FieldNamePolicy fieldNamePolicy) {
    SerializableFunction<String, String> transformName = fieldNamePolicy.get(clazz, schema);
    return CACHED_CREATORS.computeIfAbsent(
        new ClassWithSchema(clazz, schema), c -> createCreator(clazz, schema, transformName));
  }

  private static <T> SchemaUserTypeCreator createCreator(
      Class<T> clazz, Schema schema, SerializableFunction<String, String> transformName) {
    // Get the list of class fields ordered by schema.
    Map<String, Field> fieldMap =
        ReflectUtils.getFields(clazz)
            .stream()
            .collect(Collectors.toMap(f -> transformName.apply(f.getName()), Function.identity()));

    List<Field> fields =
        schema
            .getFields()
            .stream()
            .map(f -> fieldMap.get(f.getName()))
            .collect(Collectors.toList());

    try {
      DynamicType.Builder<SchemaUserTypeCreator> builder =
          BYTE_BUDDY
              .subclass(SchemaUserTypeCreator.class)
              .method(ElementMatchers.named("create"))
              .intercept(new CreateInstruction(fields, clazz));

      return builder
          .make()
          .load(ReflectHelpers.findClassLoader(), ClassLoadingStrategy.Default.INJECTION)
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
   *      {@literal @}Override public FieldType get(POJO pojo) {
   *        return convert(pojo.field);
   *      }
   *   }
   * </code></pre>
   */
  @SuppressWarnings("unchecked")
  static <ObjectT, ValueT> FieldValueGetter<ObjectT, ValueT> createGetter(
      Field field, SerializableFunction<String, String> fieldNamePolicy) {
    String newName = fieldNamePolicy.apply(field.getName());
    if (newName == null) {
      return null;
    }
    DynamicType.Builder<FieldValueGetter> builder =
        ByteBuddyUtils.subclassGetterInterface(
            BYTE_BUDDY,
            field.getDeclaringClass(),
            new ConvertType(false).convert(TypeDescriptor.of(field.getType())));
    builder = implementGetterMethods(builder, field, newName);
    try {
      return builder
          .make()
          .load(ReflectHelpers.findClassLoader(), ClassLoadingStrategy.Default.INJECTION)
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
      DynamicType.Builder<FieldValueGetter> builder, Field field, String name) {
    return builder
        .method(ElementMatchers.named("name"))
        .intercept(FixedValue.reference(name))
        .method(ElementMatchers.named("get"))
        .intercept(new ReadFieldInstruction(field));
  }

  // The list of setters for a class is cached, so we only create the classes the first time
  // getSetters is called.
  private static final Map<ClassWithSchema, List<FieldValueSetter>> CACHED_SETTERS =
      Maps.newConcurrentMap();

  public static List<FieldValueSetter> getSetters(Class<?> clazz, Schema schema) {
    // Return the setters, ordered by their position in the schema.
    return CACHED_SETTERS.computeIfAbsent(
        new ClassWithSchema(clazz, schema),
        c -> {
          Map<String, FieldValueSetter> setterMap =
              ReflectUtils.getFields(clazz)
                  .stream()
                  .map(POJOUtils::createSetter)
                  .collect(Collectors.toMap(FieldValueSetter::name, Function.identity()));
          return schema
              .getFields()
              .stream()
              .map(f -> setterMap.get(f.getName()))
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
  private static <ObjectT, ValueT> FieldValueSetter<ObjectT, ValueT> createSetter(Field field) {
    DynamicType.Builder<FieldValueSetter> builder =
        ByteBuddyUtils.subclassSetterInterface(
            BYTE_BUDDY,
            field.getDeclaringClass(),
            new ConvertType(false).convert(TypeDescriptor.of(field.getType())));
    builder = implementSetterMethods(builder, field);
    try {
      return builder
          .make()
          .load(ReflectHelpers.findClassLoader(), ClassLoadingStrategy.Default.INJECTION)
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
      DynamicType.Builder<FieldValueSetter> builder, Field field) {
    return builder
        .method(ElementMatchers.named("name"))
        .intercept(FixedValue.reference(field.getName()))
        .method(ElementMatchers.named("set"))
        .intercept(new SetFieldInstruction(field));
  }

  // Implements a method to read a public field out of an object.
  static class ReadFieldInstruction implements Implementation {
    // Field that will be read.
    private final Field field;

    ReadFieldInstruction(Field field) {
      this.field = field;
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
                new ConvertValueForGetter(readValue).convert(TypeDescriptor.of(field.getType())),
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

    SetFieldInstruction(Field field) {
      this.field = field;
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
                new ByteBuddyUtils.ConvertValueForSetter(readField)
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
  static class CreateInstruction implements Implementation {
    private final List<Field> fields;
    private final Class pojoClass;

    CreateInstruction(List<Field> fields, Class pojoClass) {
      this.fields = fields;
      this.pojoClass = pojoClass;
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
        ConvertType convertType = new ConvertType(true);
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
                  new ByteBuddyUtils.ConvertValueForSetter(readParameter)
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
