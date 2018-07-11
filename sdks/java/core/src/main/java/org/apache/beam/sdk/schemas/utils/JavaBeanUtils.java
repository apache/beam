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
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.method.MethodDescription.ForLoadedMethod;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.dynamic.scaffold.InstrumentedType;
import net.bytebuddy.implementation.FixedValue;
import net.bytebuddy.implementation.Implementation;
import net.bytebuddy.implementation.bytecode.ByteCodeAppender;
import net.bytebuddy.implementation.bytecode.ByteCodeAppender.Size;
import net.bytebuddy.implementation.bytecode.StackManipulation;
import net.bytebuddy.implementation.bytecode.member.MethodInvocation;
import net.bytebuddy.implementation.bytecode.member.MethodReturn;
import net.bytebuddy.implementation.bytecode.member.MethodVariableAccess;
import net.bytebuddy.matcher.ElementMatchers;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.schemas.FieldValueGetter;
import org.apache.beam.sdk.schemas.FieldValueSetter;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.utils.ByteBuddyUtils.ConvertType;
import org.apache.beam.sdk.schemas.utils.ByteBuddyUtils.ConvertValueForGetter;
import org.apache.beam.sdk.schemas.utils.ReflectUtils.ClassWithSchema;
import org.apache.beam.sdk.schemas.utils.StaticSchemaInference.TypeInformation;
import org.apache.beam.sdk.util.common.ReflectHelpers;

/** A set of utilities to generate getter and setter classes for JavaBean objects. */
@Experimental(Kind.SCHEMAS)
public class JavaBeanUtils {
  /** Create a {@link Schema} for a Java Bean class. */
  public static Schema schemaFromJavaBeanClass(Class<?> clazz) {
    return StaticSchemaInference.schemaFromClass(clazz, JavaBeanUtils::typeInformationFromClass);
  }

  private static List<TypeInformation> typeInformationFromClass(Class<?> clazz) {
    try {
      List<TypeInformation> getterTypes =
          ReflectUtils.getMethods(clazz)
              .stream()
              .filter(ReflectUtils::isGetter)
              .map(m -> TypeInformation.forGetter(m))
              .collect(Collectors.toList());

      Map<String, TypeInformation> setterTypes =
          ReflectUtils.getMethods(clazz)
              .stream()
              .filter(ReflectUtils::isSetter)
              .map(m -> TypeInformation.forSetter(m))
              .collect(Collectors.toMap(TypeInformation::getName, Function.identity()));
      validateJavaBean(getterTypes, setterTypes);
      return getterTypes;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  // Make sure that there are matching setters and getters.
  private static void validateJavaBean(
      List<TypeInformation> getters, Map<String, TypeInformation> setters) {
    for (TypeInformation type : getters) {
      TypeInformation setterType = setters.get(type.getName());
      if (setterType == null) {
        throw new RuntimeException(
            "JavaBean contained a getter for field "
                + type.getName()
                + "but did not contain a matching setter.");
      }
      if (!type.equals(setterType)) {
        throw new RuntimeException(
            "JavaBean contained setter for field "
                + type.getName()
                + " that had a mismatching type.");
      }
      if (!type.isNullable() == setterType.isNullable()) {
        throw new RuntimeException(
            "JavaBean contained setter for field "
                + type.getName()
                + " that had a mismatching nullable attribute.");
      }
    }
  }

  // Static ByteBuddy instance used by all helpers.
  private static final ByteBuddy BYTE_BUDDY = new ByteBuddy();

  // The list of getters for a class is cached, so we only create the classes the first time
  // getSetters is called.
  private static final Map<ClassWithSchema, List<FieldValueGetter>> CACHED_GETTERS =
      Maps.newConcurrentMap();

  /**
   * Return the list of {@link FieldValueGetter}s for a Java Bean class
   *
   * <p>The returned list is ordered by the order of fields in the schema.
   */
  public static List<FieldValueGetter> getGetters(Class<?> clazz, Schema schema) {
    return CACHED_GETTERS.computeIfAbsent(
        new ClassWithSchema(clazz, schema),
        c -> {
          try {
            Map<String, FieldValueGetter> getterMap =
                ReflectUtils.getMethods(clazz)
                    .stream()
                    .filter(ReflectUtils::isGetter)
                    .map(JavaBeanUtils::createGetter)
                    .collect(Collectors.toMap(FieldValueGetter::name, Function.identity()));
            return schema
                .getFields()
                .stream()
                .map(f -> getterMap.get(f.getName()))
                .collect(Collectors.toList());
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
  }

  private static <T> FieldValueGetter createGetter(Method getterMethod) {
    TypeInformation typeInformation = TypeInformation.forGetter(getterMethod);
    DynamicType.Builder<FieldValueGetter> builder =
        ByteBuddyUtils.subclassGetterInterface(
            BYTE_BUDDY,
            getterMethod.getDeclaringClass(),
            new ConvertType().convert(typeInformation.getType()));
    builder = implementGetterMethods(builder, getterMethod);
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
      throw new RuntimeException("Unable to generate a getter for getter '" + getterMethod + "'");
    }
  }

  private static DynamicType.Builder<FieldValueGetter> implementGetterMethods(
      DynamicType.Builder<FieldValueGetter> builder, Method method) {
    TypeInformation typeInformation = TypeInformation.forGetter(method);
    return builder
        .method(ElementMatchers.named("name"))
        .intercept(FixedValue.reference(typeInformation.getName()))
        .method(ElementMatchers.named("type"))
        .intercept(FixedValue.reference(typeInformation.getType().getRawType()))
        .method(ElementMatchers.named("get"))
        .intercept(new InvokeGetterInstruction(method));
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
  public static List<FieldValueSetter> getSetters(Class<?> clazz, Schema schema) {
    return CACHED_SETTERS.computeIfAbsent(
        new ClassWithSchema(clazz, schema),
        c -> {
          try {
            Map<String, FieldValueSetter> setterMap =
                ReflectUtils.getMethods(clazz)
                    .stream()
                    .filter(ReflectUtils::isSetter)
                    .map(JavaBeanUtils::createSetter)
                    .collect(Collectors.toMap(FieldValueSetter::name, Function.identity()));
            return schema
                .getFields()
                .stream()
                .map(f -> setterMap.get(f.getName()))
                .collect(Collectors.toList());
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
  }

  private static <T> FieldValueSetter createSetter(Method setterMethod) {
    TypeInformation typeInformation = TypeInformation.forSetter(setterMethod);
    DynamicType.Builder<FieldValueSetter> builder =
        ByteBuddyUtils.subclassSetterInterface(
            BYTE_BUDDY,
            setterMethod.getDeclaringClass(),
            new ConvertType().convert(typeInformation.getType()));
    builder = implementSetterMethods(builder, setterMethod);
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
      throw new RuntimeException("Unable to generate a setter for setter '" + setterMethod + "'");
    }
  }

  private static DynamicType.Builder<FieldValueSetter> implementSetterMethods(
      DynamicType.Builder<FieldValueSetter> builder, Method method) {
    TypeInformation typeInformation = TypeInformation.forSetter(method);
    return builder
        .method(ElementMatchers.named("name"))
        .intercept(FixedValue.reference(typeInformation.getName()))
        .method(ElementMatchers.named("type"))
        .intercept(FixedValue.reference(typeInformation.getType().getRawType()))
        .method(ElementMatchers.named("elementType"))
        .intercept(ByteBuddyUtils.getArrayComponentType(typeInformation.getType()))
        .method(ElementMatchers.named("mapKeyType"))
        .intercept(ByteBuddyUtils.getMapKeyType(typeInformation.getType()))
        .method(ElementMatchers.named("mapValueType"))
        .intercept(ByteBuddyUtils.getMapValueType(typeInformation.getType()))
        .method(ElementMatchers.named("set"))
        .intercept(new InvokeSetterInstruction(method));
  }

  // Implements a method to read a public getter out of an object.
  private static class InvokeGetterInstruction implements Implementation {
    // Getter method that wil be invoked
    private Method method;

    InvokeGetterInstruction(Method method) {
      this.method = method;
    }

    @Override
    public InstrumentedType prepare(InstrumentedType instrumentedType) {
      return instrumentedType;
    }

    @Override
    public ByteCodeAppender appender(final Target implementationTarget) {
      return (methodVisitor, implementationContext, instrumentedMethod) -> {
        TypeInformation typeInformation = TypeInformation.forGetter(method);
        // this + method parameters.
        int numLocals = 1 + instrumentedMethod.getParameters().size();

        // StackManipulation that will read the value from the class field.
        StackManipulation readValue =
            new StackManipulation.Compound(
                // Method param is offset 1 (offset 0 is the this parameter).
                MethodVariableAccess.REFERENCE.loadFrom(1),
                // Invoke the getter
                MethodInvocation.invoke(new ForLoadedMethod(method)));

        StackManipulation stackManipulation =
            new StackManipulation.Compound(
                new ConvertValueForGetter(readValue).convert(typeInformation.getType()),
                MethodReturn.REFERENCE);

        StackManipulation.Size size = stackManipulation.apply(methodVisitor, implementationContext);
        return new Size(size.getMaximalSize(), numLocals);
      };
    }
  }

  // Implements a method to write a public set out on an object.
  private static class InvokeSetterInstruction implements Implementation {
    // Setter method that wil be invoked
    private Method method;

    InvokeSetterInstruction(Method method) {
      this.method = method;
    }

    @Override
    public InstrumentedType prepare(InstrumentedType instrumentedType) {
      return instrumentedType;
    }

    @Override
    public ByteCodeAppender appender(final Target implementationTarget) {
      return (methodVisitor, implementationContext, instrumentedMethod) -> {
        TypeInformation typeInformation = TypeInformation.forSetter(method);
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
                    .convert(typeInformation.getType()),
                // Now update the field and return void.
                MethodInvocation.invoke(new ForLoadedMethod(method)),
                MethodReturn.VOID);

        StackManipulation.Size size = stackManipulation.apply(methodVisitor, implementationContext);
        return new Size(size.getMaximalSize(), numLocals);
      };
    }
  }
}
