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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.lang.reflect.Field;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
import net.bytebuddy.implementation.bytecode.assign.Assigner;
import net.bytebuddy.implementation.bytecode.assign.Assigner.Typing;
import net.bytebuddy.implementation.bytecode.assign.TypeCasting;
import net.bytebuddy.implementation.bytecode.member.FieldAccess;
import net.bytebuddy.implementation.bytecode.member.MethodInvocation;
import net.bytebuddy.implementation.bytecode.member.MethodReturn;
import net.bytebuddy.implementation.bytecode.member.MethodVariableAccess;
import net.bytebuddy.matcher.ElementMatchers;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.values.reflect.FieldValueGetter;
import org.apache.beam.sdk.values.reflect.FieldValueSetter;
import org.joda.time.ReadableInstant;

@Experimental(Kind.SCHEMAS)
public class POJOUtils {
  public static Schema schemaFromClass(Class<?> clazz) {
    Schema.Builder builder = Schema.builder();
    for (java.lang.reflect.Field field : getFields(clazz)) {
      // TODO: look for nullable annotation.
      builder.addField(field.getName(), fieldFromType(field.getGenericType()));
    }
    return builder.build();
  }

  private static java.lang.reflect.Field[] getFields(Class<?> clazz) {
    java.lang.reflect.Field[] fieldsList;
    Map<String, java.lang.reflect.Field> fields = new LinkedHashMap<>();
    do {
      if (clazz.getPackage() != null && clazz.getPackage().getName().startsWith("java."))
        break;                                   // skip java built-in classes
      for (java.lang.reflect.Field field : clazz.getDeclaredFields())
        if ((field.getModifiers() & (Modifier.TRANSIENT | Modifier.STATIC)) == 0)
          checkArgument(fields.put(field.getName(), field) == null,
            clazz.getSimpleName() + " contains two fields named: " + field);
      clazz = clazz.getSuperclass();
    } while (clazz != null);
    fieldsList = fields.values().toArray(new java.lang.reflect.Field[0]);
    return fieldsList;
  }

  private static Schema.FieldType fieldFromType(java.lang.reflect.Type type) {
    if (type.equals(Byte.class) || type.equals(byte.class)) {
      return FieldType.BYTE;
    }  else if (type.equals(Short.class) || type.equals(short.class)) {
      return FieldType.INT16;
    } else if (type.equals(Integer.class) || type.equals(int.class)) {
      return FieldType.INT32;
    } else if (type.equals(Long.class) || type.equals(long.class)) {
      return FieldType.INT64;
    } else if (type.equals(BigDecimal.class)) {
      return FieldType.DECIMAL;
    } else if (type.equals(Float.class) || type.equals(float.class)) {
      return FieldType.FLOAT;
    } else if (type.equals(Double.class) || type.equals(double.class)) {
      return FieldType.DOUBLE;
    } else if (type.equals(Boolean.class) || type.equals(boolean.class)) {
      return FieldType.BOOLEAN;
    } else if (type instanceof GenericArrayType) {
      Type component = ((GenericArrayType)type).getGenericComponentType();
      if (component.equals(Byte.class) || component.equals(byte.class)) {
        return FieldType.BYTES;
      }
      return FieldType.array(fieldFromType(component));
    } else if (type instanceof ParameterizedType) {
      ParameterizedType ptype = (ParameterizedType) type;
      Class raw = (Class) ptype.getRawType();
      java.lang.reflect.Type[] params = ptype.getActualTypeArguments();
      if (Collection.class.isAssignableFrom(raw)) {
        checkArgument(params.length == 1);
        if (params[0].equals(Byte.class) || params[0].equals(byte.class)) {
          return FieldType.BYTES;
        } else {
          return FieldType.array(fieldFromType(params[0]));
        }
      } else if (Map.class.isAssignableFrom(raw)) {
        java.lang.reflect.Type keyType = params[0];
        java.lang.reflect.Type valueType = params[1];
        // TODO: Assert that key is a primitive type.
        return FieldType.map(fieldFromType(keyType), fieldFromType(valueType));
      }
    } else if (type instanceof Class) {
      // TODO: memoize schemas for classes.
      Class clazz = (Class)type;
      if (clazz.isArray()) {
        Class componentType = clazz.getComponentType();
        if (componentType == Byte.TYPE) {
          return FieldType.BYTES;
        }
        return FieldType.array(fieldFromType(componentType));
      }
      if (CharSequence.class.isAssignableFrom(clazz)) {
        return FieldType.STRING;
      } else if (ReadableInstant.class.isAssignableFrom(clazz)) {
        return FieldType.DATETIME;
      } else if (ByteBuffer.class.isAssignableFrom(clazz)) {
        return FieldType.BYTES;
      }
      return FieldType.row(schemaFromClass(clazz));
    }
    return null;
  }

  private static final ByteBuddy BYTE_BUDDY = new ByteBuddy();

  public static Map<Class, List<FieldValueGetter>> CACHED_GETTERS =
      Maps.newConcurrentMap();
  public static List<FieldValueGetter> getGetters(Class<?> clazz) {
    return CACHED_GETTERS.computeIfAbsent(clazz,
    c -> {
      List<FieldValueGetter> getters = Lists.newArrayList();
      for (java.lang.reflect.Field field : getFields(c)) {
        getters.add(createGetter(field));
      }
      return getters;
    });
  }

  static <T> FieldValueGetter createGetter(Field field) {
    DynamicType.Builder<FieldValueGetter> builder = subclassGetterInterface(BYTE_BUDDY);
    builder = implementMethods(builder, field);
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
          "Unable to generate a getter for field '" + field + "'.", e);
    }
  }

  static DynamicType.Builder<FieldValueGetter> subclassGetterInterface(ByteBuddy byteBuddy) {
   // TypeDescription.Generic getterGenericType =
    //    TypeDescription.Generic.Builder.rawType(FieldValueGetter.class).build();

    return byteBuddy.subclass(FieldValueGetter.class);
  }

  static DynamicType.Builder<FieldValueGetter> implementMethods(
      DynamicType.Builder<FieldValueGetter> builder,
      Field field) {
    builder = builder
        .method(ElementMatchers.named("name"))
        .intercept(FixedValue.reference(field.getName()));
    builder = builder
        .method(ElementMatchers.named("type"))
        .intercept(FixedValue.reference(field.getType()));

    // TODO: Set the right value.
    boolean isRecursive = false;
    builder = builder
        .method(ElementMatchers.named("isRecursive"))
        .intercept(FixedValue.reference(isRecursive));

    // TODO: handle recursive fields.
    builder = builder
        .method(ElementMatchers.named("get"))
        .intercept(new ReadFieldInstruction(field));

    return builder;
  }

  public static class ReadFieldInstruction implements Implementation {
    // Field that will be read.
    private Field field;

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

        ForLoadedType fieldType = new ForLoadedType(field.getType());
        ForLoadedType objectType = new ForLoadedType(field.getDeclaringClass());
        StackManipulation stackManipulation = new StackManipulation.Compound(
            // Method param is offset 1 (offset 0 is the this parameter).
            MethodVariableAccess.REFERENCE.loadFrom(1),
            // Downcast the Object to the expected type of the POJO.
            TypeCasting.to(objectType),
            // Read the field from the object.
            FieldAccess.forField(new ForLoadedField(field)).read());
        if (fieldType.isPrimitive()) {
          stackManipulation = new StackManipulation.Compound(
              stackManipulation,
              // Since the field is a primitive type, we need to box it for return.
              Assigner.DEFAULT.assign(
                  fieldType.asGenericType(),
                  fieldType.asBoxed().asGenericType(),
                  Typing.STATIC));
        }
        // TODO: We need to potentially copy into type expected by the Row getters.
        stackManipulation = new StackManipulation.Compound(
            stackManipulation,
            MethodReturn.REFERENCE);

        StackManipulation.Size size = stackManipulation.apply(methodVisitor, implementationContext);
        return new Size(size.getMaximalSize(), numLocals);
      };
    }
  }

  public static class SetFieldInstruction implements Implementation {
    // Field that will be read.
    private Field field;

    SetFieldInstruction(Field field) {
      this.field = field;
    }

    @Override
    public InstrumentedType prepare(InstrumentedType instrumentedType) {
      return instrumentedType;
    }

    // The setter might be called with a different subclass of ReadableInstant than the one stored
    // in this POJO. We must extract the value passed into the setter and copy it into an instance
    // that the POJO can accept.
    private StackManipulation captureDateTime(StackManipulation previous, ForLoadedType fieldType) {
      ForLoadedType readableInstantType = new ForLoadedType(ReadableInstant.class);
      return new StackManipulation.Compound(
          previous,
          // Create a new instance of the target type.
          TypeCreation.of(fieldType),
          Duplication.SINGLE,
          // Load the parameter and cast it to a ReadableInstant.
          MethodVariableAccess.REFERENCE.loadFrom(2),
          TypeCasting.to(readableInstantType),
          // Call ReadableInstant.getMillis to extract the millis since the epoch.
          MethodInvocation.invoke(readableInstantType
              .getDeclaredMethods()
              .filter(ElementMatchers.named("getMillis"))
              .getOnly()),
          // All subclasses of ReadableInstant contain a ()(long) constructor that takes in a millis
          // argument. Call that constructor of the field to initialize it.
          MethodInvocation.invoke(fieldType
              .getDeclaredMethods()
              .filter(ElementMatchers.isConstructor()
                  .and(ElementMatchers.takesArguments(ForLoadedType.of(long.class))))
              .getOnly()));
    }

    private StackManipulation captureIntoByteBuffer(StackManipulation previous) {
      ForLoadedType byteBufferType = new ForLoadedType(ByteBuffer.class);
      ForLoadedType byteArrayType = new ForLoadedType(byte[].class);

      // We currently assume that a byte[] setter will always accept a parameter of type byte[].
      return new StackManipulation.Compound(
          previous,
          // Load the parameter and cast it to a byte[].
          MethodVariableAccess.REFERENCE.loadFrom(2),
          TypeCasting.to(byteArrayType),
          // Create a new ByteBuffer that wraps this byte[].
          MethodInvocation.invoke(byteBufferType
              .getDeclaredMethods()
              .filter(ElementMatchers.named("wrap")
                  .and(ElementMatchers.takesArguments(byteArrayType)))
              .getOnly()));
    }

    @Override
    public ByteCodeAppender appender(final Target implementationTarget) {
      return (methodVisitor, implementationContext, instrumentedMethod) -> {
        // this + method parameters.
        int numLocals = 1 + instrumentedMethod.getParameters().size();

        ForLoadedType fieldType = new ForLoadedType(field.getType());
        ForLoadedType objectType = new ForLoadedType(field.getDeclaringClass());

        StackManipulation stackManipulation = new StackManipulation.Compound(
            // Object param is offset 1.
            MethodVariableAccess.REFERENCE.loadFrom(1),
            TypeCasting.to(objectType));
        if (ReadableInstant.class.isAssignableFrom(field.getType())) {
          stackManipulation = captureDateTime(stackManipulation, fieldType);
        } else if (field.getType().equals(ByteBuffer.class)) {
          stackManipulation = captureIntoByteBuffer(stackManipulation);
        } else {
          // Otherwise cast to the matching field type.
          stackManipulation = new StackManipulation.Compound(
              stackManipulation,
              // Value param is offset 2.
              MethodVariableAccess.REFERENCE.loadFrom(2),
              TypeCasting.to(fieldType.asBoxed()));
        }

        if (fieldType.isPrimitive()) {
          // If the field type is primitive, then unbox the parameter before trying to assign it
          // to the field.
          stackManipulation = new StackManipulation.Compound(
              stackManipulation,
              Assigner.DEFAULT.assign(
                fieldType.asBoxed().asGenericType(),
                fieldType.asUnboxed().asGenericType(),
                Typing.STATIC));
        }


        stackManipulation = new StackManipulation.Compound(
            stackManipulation,
            FieldAccess.forField(new ForLoadedField(field)).write(),
            MethodReturn.VOID);
        StackManipulation.Size size = stackManipulation.apply(methodVisitor, implementationContext);
        return new Size(size.getMaximalSize(), numLocals);
      };
    }
  }

  public static Map<Class, List<FieldValueSetter>> CACHED_SETTERS =
      Maps.newConcurrentMap();
  public static List<FieldValueSetter> getSetters(Class<?> clazz) {
    return CACHED_SETTERS.computeIfAbsent(clazz,
        c -> {
          List<FieldValueSetter> getters = Lists.newArrayList();
          for (java.lang.reflect.Field field : getFields(c)) {
            getters.add(createSetter(field));
          }
          return getters;
        });
  }

  static <T> FieldValueSetter createSetter(Field field) {
    DynamicType.Builder<FieldValueSetter> builder = subclassSetterInterface(BYTE_BUDDY);
    builder = implementSetterMethods(builder, field);
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
          "Unable to generate a getter for field '" + field + "'.", e);
    }
  }

  static DynamicType.Builder<FieldValueSetter> subclassSetterInterface(ByteBuddy byteBuddy) {
    return byteBuddy.subclass(FieldValueSetter.class);
  }

  // If the Field is a container type, returns the element type. Otherwise returns a null reference.
  static Implementation getArrayComponentType(Field field) {
    Type fieldType = field.getGenericType();
    if (fieldType instanceof GenericArrayType) {
      Type component = ((GenericArrayType) fieldType).getGenericComponentType();
      if (!component.equals(Byte.class) && !component.equals(byte.class)) {
        return FixedValue.reference(component);
      }
    } else if (fieldType instanceof ParameterizedType) {
      ParameterizedType ptype = (ParameterizedType) fieldType;
      Class raw = (Class) ptype.getRawType();
      java.lang.reflect.Type[] params = ptype.getActualTypeArguments();
      if (Collection.class.isAssignableFrom(raw)) {
        checkArgument(params.length == 1);
        if (!params[0].equals(Byte.class) && !params[0].equals(byte.class)) {
          return FixedValue.reference(params[0]);
        }
      }
    } else if (fieldType instanceof Class) {
      Class clazz = (Class) fieldType;
      if (clazz.isArray()) {
        Class componentType = clazz.getComponentType();
        if (componentType != Byte.TYPE) {
          return FixedValue.reference(componentType);
        }
      }
    }
    return FixedValue.nullValue();
  }

  // If the Field is a map type, returns the key or value type. Otherwise returns a null reference.
  static Implementation getMapType(Field field, int index) {
    Type fieldType = field.getGenericType();
    if (fieldType instanceof ParameterizedType) {
      ParameterizedType ptype = (ParameterizedType) fieldType;
      Class raw = (Class) ptype.getRawType();
      java.lang.reflect.Type[] params = ptype.getActualTypeArguments();
      if (Map.class.isAssignableFrom(raw)) {
        return FixedValue.reference(params[index]);
      }
    }
    return FixedValue.nullValue();
  }


  static DynamicType.Builder<FieldValueSetter> implementSetterMethods(
      DynamicType.Builder<FieldValueSetter> builder,
      Field field) {

    builder = builder
        .method(ElementMatchers.named("name"))
        .intercept(FixedValue.reference(field.getName()));
    builder = builder
        .method(ElementMatchers.named("type"))
        .intercept(FixedValue.reference(field.getType()));
    builder = builder
        .method(ElementMatchers.named("elementType"))
        .intercept(getArrayComponentType(field));
    builder = builder
        .method(ElementMatchers.named("mapKeyType"))
        .intercept(getMapType(field, 0));
    builder = builder
        .method(ElementMatchers.named("mapValueType"))
        .intercept(getMapType(field, 1));
    builder = builder
        .method(ElementMatchers.named("set"))
        .intercept(new SetFieldInstruction(field));
    return builder;
  }
}
