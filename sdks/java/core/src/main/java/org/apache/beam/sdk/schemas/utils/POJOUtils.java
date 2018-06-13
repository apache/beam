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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.stream.Collectors;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.field.FieldDescription.ForLoadedField;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.description.type.TypeDescription.ForLoadedType;
import net.bytebuddy.description.type.TypeDescription.Generic.OfGenericArray;
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
import net.bytebuddy.implementation.bytecode.TypeCreation;
import net.bytebuddy.implementation.bytecode.assign.Assigner;
import net.bytebuddy.implementation.bytecode.assign.Assigner.Typing;
import net.bytebuddy.implementation.bytecode.assign.TypeCasting;
import net.bytebuddy.implementation.bytecode.collection.ArrayFactory;
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
import org.apache.commons.lang3.ArrayUtils;
import org.joda.time.DateTime;
import org.joda.time.ReadableDateTime;
import org.joda.time.ReadableInstant;

@Experimental(Kind.SCHEMAS)
public class POJOUtils {
  static final ForLoadedType ARRAYS_TYPE = new ForLoadedType(Arrays.class);
  static final ForLoadedType ARRAY_UTILS_TYPE = new ForLoadedType(ArrayUtils.class);
  static final ForLoadedType BYTE_TYPE = new ForLoadedType(byte.class);
  private static ForLoadedType READABLE_INSTANT_TYPE = new ForLoadedType(ReadableInstant.class);
  private static ForLoadedType DATE_TIME_TYPE = new ForLoadedType(DateTime.class);
  private static ForLoadedType LIST_TYPE = new ForLoadedType(List.class);
  private static ForLoadedType BYTE_BUFFER_TYPE = new ForLoadedType(ByteBuffer.class);
  private static ForLoadedType BYTE_ARRAY_TYPE = new ForLoadedType(byte[].class);
  private static ForLoadedType CHAR_SEQUENCE_TYPE = new ForLoadedType(CharSequence.class);

  public static Schema schemaFromClass(Class<?> clazz) {
    Schema.Builder builder = Schema.builder();
    for (java.lang.reflect.Field field : getFields(clazz)) {
      // TODO: look for nullable annotation.
      builder.addField(field.getName(), fieldFromType(field.getGenericType()));
    }
    return builder.build();
  }

  // Get all public, non-static, non-transient fields.
  private static List<java.lang.reflect.Field> getFields(Class<?> clazz) {
    Map<String, java.lang.reflect.Field> fields = new LinkedHashMap<>();
    do {
      if (clazz.getPackage() != null && clazz.getPackage().getName().startsWith("java."))
        break;                                   // skip java built-in classes
      for (java.lang.reflect.Field field : clazz.getDeclaredFields()) {
        if ((field.getModifiers() & (Modifier.TRANSIENT | Modifier.STATIC)) == 0) {
          if ((field.getModifiers() & Modifier.PUBLIC) != 0) {
            checkArgument(fields.put(field.getName(), field) == null,
                clazz.getSimpleName() + " contains two fields named: " + field);
          }
        }
      }
      clazz = clazz.getSuperclass();
    } while (clazz != null);
    return Lists.newArrayList(fields.values());
  }

  // Map a Java field type to a Beam Schema FieldType.
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
        FieldType keyType = fieldFromType(params[0]);
        FieldType valueType = fieldFromType(params[1]);
        checkArgument(keyType.getTypeName().isPrimitiveType(),
            "Only primitive types can be map keys");
        return FieldType.map(keyType, valueType);
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

  // Static ByteBuddy instance used by all helpers.
  private static final ByteBuddy BYTE_BUDDY = new ByteBuddy();

  // We memoize the list of getters for a class.
  public static Map<Class, List<FieldValueGetter>> CACHED_GETTERS =
      Maps.newConcurrentMap();
  public static List<FieldValueGetter> getGetters(Class<?> clazz) {
    return CACHED_GETTERS.computeIfAbsent(clazz,
        (c) -> getFields(c).stream().map(POJOUtils::createGetter).collect(Collectors.toList()));
  }

  static <T> FieldValueGetter createGetter(Field field) {
    DynamicType.Builder<FieldValueGetter> builder = subclassGetterInterface(BYTE_BUDDY);
    builder = implementGetterMethods(builder, field);
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
    return byteBuddy.subclass(FieldValueGetter.class);
  }

  static DynamicType.Builder<FieldValueGetter> implementGetterMethods(
      DynamicType.Builder<FieldValueGetter> builder,
      Field field) {
    builder = builder
        .method(ElementMatchers.named("name"))
        .intercept(FixedValue.reference(field.getName()));
    builder = builder
        .method(ElementMatchers.named("type"))
        .intercept(FixedValue.reference(field.getType()));
    builder = builder
        .method(ElementMatchers.named("get"))
        .intercept(new ReadFieldInstruction(field));

    return builder;
  }

  // Implements a method to read a public field out of an object.
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

    private StackManipulation captureArray(ForLoadedType fieldType,
                                           StackManipulation stackManipulation) {
      // Row always expects to get an Iterable back for array types. Wrap this array into a
      // List using Arrays.asList before returning.
      if (fieldType.getComponentType().isPrimitive()) {
        // Arrays.asList doesn't take primitive arrays, so convert first.
        stackManipulation = new StackManipulation.Compound(
            stackManipulation,
            MethodInvocation.invoke(ARRAY_UTILS_TYPE.getDeclaredMethods()
                .filter(ElementMatchers.isStatic()
                    .and(ElementMatchers.named("toObject"))
                    .and(ElementMatchers.takesArguments(fieldType)))
                .getOnly()));
      }
      stackManipulation = new StackManipulation.Compound(
          stackManipulation,
          MethodInvocation.invoke(ARRAYS_TYPE.getDeclaredMethods()
              .filter(ElementMatchers.isStatic()
                  .and(ElementMatchers.named("asList")))
              .getOnly()));
      return stackManipulation;
    }

    private StackManipulation loadField(ForLoadedType objectType, ForLoadedType fieldType) {
      return new StackManipulation.Compound(
          // Method param is offset 1 (offset 0 is the this parameter).
          MethodVariableAccess.REFERENCE.loadFrom(1),
          // Downcast the Object to the expected type of the POJO.
          TypeCasting.to(objectType),
          // Read the field from the object.
          FieldAccess.forField(new ForLoadedField(field)).read());
    }

    private StackManipulation captureDateTime(ForLoadedType objectType, ForLoadedType fieldType) {
      return new StackManipulation.Compound(
          // Create a new instance of the target type.
          TypeCreation.of(DATE_TIME_TYPE),
          Duplication.SINGLE,
          loadField(objectType, fieldType),
          TypeCasting.to(READABLE_INSTANT_TYPE),
          // Call ReadableInstant.getMillis to extract the millis since the epoch.
          MethodInvocation.invoke(READABLE_INSTANT_TYPE
              .getDeclaredMethods()
              .filter(ElementMatchers.named("getMillis"))
              .getOnly()),
          // Construct a DateTime object contaiing
          MethodInvocation.invoke(DATE_TIME_TYPE
              .getDeclaredMethods()
              .filter(ElementMatchers.isConstructor()
                  .and(ElementMatchers.takesArguments(ForLoadedType.of(long.class))))
              .getOnly()));
    }

    @Override
    public ByteCodeAppender appender(final Target implementationTarget) {
      return (methodVisitor, implementationContext, instrumentedMethod) -> {
        // this + method parameters.
        int numLocals = 1 + instrumentedMethod.getParameters().size();

        // Type of the field we are reading.
        ForLoadedType fieldType = new ForLoadedType(field.getType());
        // Type of the object containing the field.
        ForLoadedType objectType = new ForLoadedType(field.getDeclaringClass());
        StackManipulation stackManipulation;
        if (fieldType.isPrimitive()) {
          // Since the field is a primitive type, we need to box it for return.
          stackManipulation = new StackManipulation.Compound(
              loadField(objectType, fieldType),
              Assigner.DEFAULT.assign(
                  fieldType.asGenericType(),
                  fieldType.asBoxed().asGenericType(),
                  Typing.STATIC));
        } else if (fieldType.isArray() && !fieldType.getComponentType().equals(BYTE_TYPE)) {
          // Byte arrays are special, so leave those alone.
          stackManipulation = loadField(objectType, fieldType);
          stackManipulation = captureArray(fieldType, stackManipulation);
        } else if (ReadableInstant.class.isAssignableFrom(field.getType()) &&
            !ReadableDateTime.class.isAssignableFrom(field.getType())) {
          // If the POJO contains an Instant that is not a ReadableDateTime, we must make it a
          // ReadableDateTime before returning it.
          stackManipulation = captureDateTime(objectType, fieldType);
        } else if (ByteBuffer.class.isAssignableFrom(field.getType())) {
          // We must extract the array from the ByteBuffer before returning.
          // NOTE: we only support array-backed byte buffers in these POJOs. Others (e.g. mmaped
          // files) are not supported.
          stackManipulation = loadField(objectType, fieldType);
          stackManipulation = new StackManipulation.Compound(
              stackManipulation,
              MethodInvocation.invoke(BYTE_BUFFER_TYPE.getDeclaredMethods()
                  .filter(ElementMatchers.named("array")
                      .and(ElementMatchers.returns(BYTE_ARRAY_TYPE)))
                  .getOnly()));
        } else if (CharSequence.class.isAssignableFrom(field.getType())
          && !String.class.isAssignableFrom(field.getType())) {
          stackManipulation = loadField(objectType, fieldType);
          stackManipulation = new StackManipulation.Compound(
              stackManipulation,
              MethodInvocation.invoke(CHAR_SEQUENCE_TYPE.getDeclaredMethods()
                  .filter(ElementMatchers.named("toString"))
                  .getOnly()));
        } else {
          stackManipulation = loadField(objectType, fieldType);
        }
        stackManipulation = new StackManipulation.Compound(
            stackManipulation,
            MethodReturn.REFERENCE);

        StackManipulation.Size size = stackManipulation.apply(methodVisitor, implementationContext);
        return new Size(size.getMaximalSize(), numLocals);
      };
    }
  }

  // Implements a method to set a public field in an object.
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
    private StackManipulation captureDateTime(StackManipulation stackManipulation,
                                              ForLoadedType fieldType) {
      return new StackManipulation.Compound(
          stackManipulation,
          // Create a new instance of the target type.
          TypeCreation.of(fieldType),
          Duplication.SINGLE,
          // Load the parameter and cast it to a ReadableInstant.
          MethodVariableAccess.REFERENCE.loadFrom(2),
          TypeCasting.to(READABLE_INSTANT_TYPE),
          // Call ReadableInstant.getMillis to extract the millis since the epoch.
          MethodInvocation.invoke(READABLE_INSTANT_TYPE
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

    private StackManipulation captureIntoByteBuffer(StackManipulation stackManipulation) {
      // We currently assume that a byte[] setter will always accept a parameter of type byte[].
      return new StackManipulation.Compound(
          stackManipulation,
          // Load the parameter and cast it to a byte[].
          MethodVariableAccess.REFERENCE.loadFrom(2),
          TypeCasting.to(BYTE_ARRAY_TYPE),
          // Create a new ByteBuffer that wraps this byte[].
          MethodInvocation.invoke(BYTE_BUFFER_TYPE
              .getDeclaredMethods()
              .filter(ElementMatchers.named("wrap")
                  .and(ElementMatchers.takesArguments(BYTE_ARRAY_TYPE)))
              .getOnly()));
    }

    private StackManipulation captureIntoArray(ForLoadedType fieldType,
                                               StackManipulation stackManipulation) {
      // The type of the array containing the (possibly) boxed values.
      TypeDescription arrayType =
          TypeDescription.Generic.Builder.rawType(
              fieldType.getComponentType().asBoxed()).asArray().build().asErasure();

      // Extract an array from the collection.
      stackManipulation = new StackManipulation.Compound(
          stackManipulation,
          MethodVariableAccess.REFERENCE.loadFrom(2),
          TypeCasting.to(LIST_TYPE),
          // Call Collection.ToArray(T[[]) to extract the array.
          ArrayFactory.forType(fieldType.getComponentType().asBoxed().asGenericType())
              .withValues(Collections.emptyList()),
          MethodInvocation.invoke(LIST_TYPE
              .getDeclaredMethods()
              .filter(ElementMatchers.named("toArray")
                  .and(ElementMatchers.takesArguments(1)))
              .getOnly()),
          // Cast the result to T[].
          TypeCasting.to(arrayType));
      if (fieldType.getComponentType().isPrimitive()) {
        // The array we extract will be an array of objects. If the pojo field is an array of
        // primitive types, we need to then convert to an array of unboxed objects.
        stackManipulation = new StackManipulation.Compound(
            stackManipulation,
            MethodInvocation.invoke(ARRAY_UTILS_TYPE
                .getDeclaredMethods()
                .filter(ElementMatchers.named("toPrimitive")
                    .and(ElementMatchers.takesArguments(arrayType)))
                .getOnly()));
      }
      return stackManipulation;
    }

    StackManipulation captureIntoCharSequence(ForLoadedType fieldType,
                                              StackManipulation stackManipulation) {
      return new StackManipulation.Compound(
          stackManipulation,
          TypeCreation.of(fieldType),
          Duplication.SINGLE,
          // Load the parameter and cast it to a CharSequence.
          MethodVariableAccess.REFERENCE.loadFrom(2),
          TypeCasting.to(CHAR_SEQUENCE_TYPE),
          // Create an element of the field type that wraps this one..
          MethodInvocation.invoke(fieldType
              .getDeclaredMethods()
              .filter(ElementMatchers.isConstructor()
                  .and(ElementMatchers.takesArguments(CHAR_SEQUENCE_TYPE)))
              .getOnly()));
    }

    @Override
    public ByteCodeAppender appender(final Target implementationTarget) {
      return (methodVisitor, implementationContext, instrumentedMethod) -> {
        // this + method parameters.
        int numLocals = 1 + instrumentedMethod.getParameters().size();

        // The type of the field being set.
        ForLoadedType fieldType = new ForLoadedType(field.getType());
        // The type of the object containing the field.
        ForLoadedType objectType = new ForLoadedType(field.getDeclaringClass());

        StackManipulation stackManipulation = new StackManipulation.Compound(
            // Object param is offset 1.
            MethodVariableAccess.REFERENCE.loadFrom(1),
            TypeCasting.to(objectType));
        if (ReadableInstant.class.isAssignableFrom(field.getType())) {
          stackManipulation = captureDateTime(stackManipulation, fieldType);
        } else if (field.getType().equals(ByteBuffer.class)) {
          stackManipulation = captureIntoByteBuffer(stackManipulation);
        } else if (CharSequence.class.isAssignableFrom(field.getType())
            && !field.getType().isAssignableFrom(String.class)) {
          stackManipulation = captureIntoCharSequence(fieldType, stackManipulation);
        } else if (field.getType().isArray() && !fieldType.getComponentType().equals(BYTE_TYPE)) {
          // Byte arrays are special, so leave them alone.
          stackManipulation = captureIntoArray(fieldType, stackManipulation);
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
        // Now generate the instruction to write the field.
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
        c -> getFields(c).stream().map(POJOUtils::createSetter).collect(Collectors.toList()));
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
}
