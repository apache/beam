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
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.method.MethodDescription.ForLoadedMethod;
import net.bytebuddy.description.type.TypeDescription.ForLoadedType;
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
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.utils.ByteBuddyUtils.ConvertType;
import org.apache.beam.sdk.schemas.utils.ByteBuddyUtils.ConvertValueForGetter;
import org.apache.beam.sdk.schemas.utils.StaticSchemaInference.TypeInformation;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.reflect.FieldValueGetter;
import org.apache.beam.sdk.values.reflect.FieldValueSetter;

@Experimental(Kind.SCHEMAS)
public class JavaBeanUtils {
  enum MethodType { GETTER, SETTER }

  public static Schema schemaFromJavaBeanClass(Class<?> clazz) {
      return StaticSchemaInference.schemaFromClass(clazz, JavaBeanUtils::typeInformationFromClass);
  }

  private static List<TypeInformation> typeInformationFromClass(Class<?> clazz) {
    try {
      List<TypeInformation> getters =
          Arrays.stream(ReflectUtils.getDeclaredMethodsInOrder(clazz))
              .filter(JavaBeanUtils::isGetter)
              .map(m -> toTypeInformation(m, MethodType.GETTER))
              .collect(Collectors.toList());

      Map<String, TypeInformation> setters =
          Arrays.stream(ReflectUtils.getDeclaredMethodsInOrder(clazz))
              .filter(JavaBeanUtils::isSetter)
              .map(m -> toTypeInformation(m, MethodType.SETTER))
              .collect(Collectors.toMap(TypeInformation::getName, Function.identity()));
      validateJavaBean(getters, setters);
      return getters;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }


  private static void validateJavaBean(
      List<TypeInformation> getters, Map<String, TypeInformation> setters) {
    for (TypeInformation type : getters) {
      TypeInformation setterType = setters.get(type.getName());
      if (setterType == null) {
        throw new RuntimeException("JavaBean contained a setter for field " + type.getName()
        + "but did not contain a matching getter.");
      }
      if (!type.equals(setterType)) {
        throw new RuntimeException(
            "JavaBean contained mismatching setter for field" + type.getName());
      }
    }
  }

  private static TypeInformation toTypeInformation(
      Method method, MethodType methodType) {
    String name;
    if (methodType.equals(MethodType.GETTER)) {
      if (method.getName().startsWith("get")) {
        name = stripPrefix(method.getName(), "get");
      } else if (method.getName().startsWith("is")) {
        name = stripPrefix(method.getName(), "is");
      } else {
        throw new RuntimeException("Getter has wrong prefix " + method.getName());
      }
      boolean nullable = method.getAnnotation(Nullable.class) != null;
      return new TypeInformation(name, TypeDescriptor.of(method.getGenericReturnType()), nullable);
    } else if (methodType.equals(MethodType.SETTER)) {
      if (method.getName().startsWith("set")) {
        name = stripPrefix(method.getName(), "get");
      } else {
        throw new RuntimeException("Setter has wrong prefix " + method.getName());
      }
      if (method.getParameterCount() != 1) {
        throw new RuntimeException("Setter methods should take a single argument.");
      }
      TypeDescriptor type = TypeDescriptor.of(method.getGenericParameterTypes()[0]);
      boolean nullable = Arrays.stream(method.getParameterAnnotations()[0])
          .anyMatch(Nullable.class::isInstance);
      return new TypeInformation(name, type, nullable);
    }
    return null;
  }

  private static String stripPrefix(String methodName, String prefix) {
    String firstLetter = methodName.substring(prefix.length(), prefix.length() + 1).toLowerCase();

    return (methodName.length() == prefix.length() + 1)
        ? firstLetter
        : (firstLetter + methodName.substring(prefix.length() + 1, methodName.length()));
  }

  private static boolean isGetter(Method method) {
    if (!Modifier.isPublic(method.getModifiers())) {
      return false;
    }
    if (Void.TYPE.equals(method.getReturnType())) {
      return false;
    }
    if (method.getName().startsWith("get") && method.getName().length() > 3) {
      return  true;
    }
    return (method.getName().startsWith("is")
        && method.getName().length() > 2
        && (Boolean.TYPE.equals(method.getReturnType())
        || Boolean.class.equals(method.getReturnType())));
  }

  private static boolean isSetter(Method method) {
    return Modifier.isPublic(method.getModifiers())
        && Void.TYPE.equals(method.getReturnType())
        && method.getName().startsWith("set");
  }

  // Static ByteBuddy instance used by all helpers.
  private static final ByteBuddy BYTE_BUDDY = new ByteBuddy();

  // The list of getters for a class is cached, so we only create the classes the first time
  // getSetters is called.
  private static Map<Class, List<FieldValueGetter>> CACHED_GETTERS = Maps.newConcurrentMap();
  public static List<FieldValueGetter> getGetters(Class<?> clazz) {
      return CACHED_GETTERS.computeIfAbsent(clazz, c -> {
        try {
          return Arrays.stream(ReflectUtils.getDeclaredMethodsInOrder(clazz))
              .filter(JavaBeanUtils::isGetter).map(JavaBeanUtils::createGetter)
              .collect(Collectors.toList());
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      });
  }

  private static <T> FieldValueGetter createGetter(Method getterMethod) {
    TypeInformation typeInformation = toTypeInformation(getterMethod, MethodType.GETTER);
    DynamicType.Builder<FieldValueGetter> builder = ByteBuddyUtils.subclassGetterInterface(
        BYTE_BUDDY,
        getterMethod.getDeclaringClass(),
        new ConvertType().convert(typeInformation.getType()));
    builder = implementGetterMethods(builder, getterMethod);
    try {
      return builder
          .make()
          .load(
              POJOUtils.class.getClassLoader(),
              ClassLoadingStrategy.Default.INJECTION)
          .getLoaded()
          .getDeclaredConstructor().newInstance();
    } catch (InstantiationException | IllegalAccessException
        | NoSuchMethodException | InvocationTargetException e) {
      throw new RuntimeException(
          "Unable to generate a getter for getter '" + getterMethod + "'");
    }
  }


  static private DynamicType.Builder<FieldValueGetter> implementGetterMethods(
      DynamicType.Builder<FieldValueGetter> builder, Method method) {
    TypeInformation typeInformation = toTypeInformation(method, MethodType.GETTER);
    return builder
        .method(ElementMatchers.named("name"))
        .intercept(FixedValue.reference(typeInformation.getName()))
        .method(ElementMatchers.named("type"))
        .intercept(FixedValue.reference(typeInformation.getType().getRawType()))
        .method(ElementMatchers.named("get"))
        .intercept(new InvokeGetterInstruction(method));
  }

  // Implements a method to read a public getter out of an object.
  public static class InvokeGetterInstruction implements Implementation {
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
        TypeInformation typeInformation = toTypeInformation(method, MethodType.GETTER);
        // this + method parameters.
        int numLocals = 1 + instrumentedMethod.getParameters().size();

        // StackManipulation that will read the value from the class field.
        StackManipulation readValue = new StackManipulation.Compound(
            // Method param is offset 1 (offset 0 is the this parameter).
            MethodVariableAccess.REFERENCE.loadFrom(1),
            // Invoke the getter
            MethodInvocation.invoke(new ForLoadedMethod(method)));

        StackManipulation stackManipulation = new StackManipulation.Compound(
            new ConvertValueForGetter(readValue).convert(typeInformation.getType()),
            MethodReturn.REFERENCE);

        StackManipulation.Size size = stackManipulation.apply(methodVisitor, implementationContext);
        return new Size(size.getMaximalSize(), numLocals);
      };
    }
  }

  // The list of setters for a class is cached, so we only create the classes the first time
  // getSetters is called.
  private static Map<Class, List<FieldValueSetter>> CACHED_SETTERS = Maps.newConcurrentMap();
  public static List<FieldValueSetter> getSetters(Class<?> clazz) {
    return CACHED_SETTERS.computeIfAbsent(clazz, c -> {
      try {
        return Arrays.stream(ReflectUtils.getDeclaredMethodsInOrder(clazz))
            .filter(JavaBeanUtils::isSetter).map(JavaBeanUtils::createSetter)
            .collect(Collectors.toList());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
  }

  private static <T> FieldValueSetter createSetter(Method setterMethod) {
    TypeInformation typeInformation = toTypeInformation(setterMethod, MethodType.SETTER);
    DynamicType.Builder<FieldValueSetter> builder = ByteBuddyUtils.subclassSetterInterface(
        BYTE_BUDDY,
        setterMethod.getDeclaringClass(),
        new ConvertType().convert(typeInformation.getType()));
    builder = implementSetterMethods(builder, setterMethod);
    try {
      return builder
          .make()
          .load(
              POJOUtils.class.getClassLoader(),
              ClassLoadingStrategy.Default.INJECTION)
          .getLoaded()
          .getDeclaredConstructor().newInstance();
    } catch (InstantiationException | IllegalAccessException
        | NoSuchMethodException | InvocationTargetException e) {
      throw new RuntimeException(
          "Unable to generate a getter for getter '" + setterMethod + "'");
    }
  }

  static private DynamicType.Builder<FieldValueSetter> implementSetterMethods(
      DynamicType.Builder<FieldValueSetter> builder, Method method) {
    TypeInformation typeInformation = toTypeInformation(method, MethodType.SETTER);
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
  public static class InvokeSetterInstruction implements Implementation {
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
        TypeInformation typeInformation = toTypeInformation(method, MethodType.SETTER);
        // this + method parameters.
        int numLocals = 1 + instrumentedMethod.getParameters().size();

        // The instruction to read the field.
        StackManipulation readField = MethodVariableAccess.REFERENCE.loadFrom(2);

        // Read the object onto the stack.
        StackManipulation stackManipulation = new StackManipulation.Compound(
            // Object param is offset 1.
            MethodVariableAccess.REFERENCE.loadFrom(1),
            // Do any conversions necessary.
            new ByteBuddyUtils.ConvertValueForSetter(readField).convert(typeInformation.getType()),
            // Now update the field and return void.
            MethodInvocation.invoke(new ForLoadedMethod(method)),
            MethodReturn.VOID);

        StackManipulation.Size size = stackManipulation.apply(methodVisitor, implementationContext);
        return new Size(size.getMaximalSize(), numLocals);
      };
    }
  }
}
