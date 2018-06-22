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

import java.lang.reflect.Field;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.description.type.TypeDescription.ForLoadedType;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.implementation.FixedValue;
import net.bytebuddy.implementation.Implementation;
import net.bytebuddy.implementation.bytecode.Duplication;
import net.bytebuddy.implementation.bytecode.StackManipulation;
import net.bytebuddy.implementation.bytecode.StackManipulation.Compound;
import net.bytebuddy.implementation.bytecode.TypeCreation;
import net.bytebuddy.implementation.bytecode.assign.Assigner;
import net.bytebuddy.implementation.bytecode.assign.Assigner.Typing;
import net.bytebuddy.implementation.bytecode.assign.TypeCasting;
import net.bytebuddy.implementation.bytecode.collection.ArrayFactory;
import net.bytebuddy.implementation.bytecode.member.MethodInvocation;
import net.bytebuddy.matcher.ElementMatchers;
import org.apache.beam.sdk.values.reflect.FieldValueGetter;
import org.apache.beam.sdk.values.reflect.FieldValueSetter;
import org.apache.commons.lang3.ArrayUtils;
import org.joda.time.DateTime;
import org.joda.time.ReadableDateTime;
import org.joda.time.ReadableInstant;

class ByteBuddyUtils {
  static final ForLoadedType ARRAYS_TYPE = new ForLoadedType(Arrays.class);
  static final ForLoadedType ARRAY_UTILS_TYPE = new ForLoadedType(ArrayUtils.class);
  private static ForLoadedType BYTE_ARRAY_TYPE = new ForLoadedType(byte[].class);
  private static ForLoadedType BYTE_BUFFER_TYPE = new ForLoadedType(ByteBuffer.class);
  static final ForLoadedType BYTE_TYPE = new ForLoadedType(byte.class);
  private static ForLoadedType CHAR_SEQUENCE_TYPE = new ForLoadedType(CharSequence.class);
  private static ForLoadedType DATE_TIME_TYPE = new ForLoadedType(DateTime.class);
  private static ForLoadedType LIST_TYPE = new ForLoadedType(List.class);
  private static ForLoadedType READABLE_INSTANT_TYPE = new ForLoadedType(ReadableInstant.class);

  public static <T> DynamicType.Builder<FieldValueGetter> subclassGetterInterface(
      ByteBuddy byteBuddy, Class<T> clazz) {
    TypeDescription.Generic getterGenericType =
        TypeDescription.Generic.Builder.parameterizedType(FieldValueGetter.class, clazz).build();
    return (DynamicType.Builder<FieldValueGetter>) byteBuddy.subclass(getterGenericType);
  }

  public static <T> DynamicType.Builder<FieldValueSetter> subclassSetterInterface(
      ByteBuddy byteBuddy, Class<T> clazz) {
    TypeDescription.Generic setterGenericType =
        TypeDescription.Generic.Builder.parameterizedType(FieldValueSetter.class, clazz).build();
    return (DynamicType.Builder<FieldValueSetter>) byteBuddy.subclass(setterGenericType);
  }

  /**
   * Takes a {@link StackManipulation} that returns a value. Prepares this value to be returned
   * by a getter. {@link org.apache.beam.sdk.values.Row} needs getters to return specific types,
   * but we allow user pojos to contain different but equivalent types. Therefore we must convert
   * some of these types before returning.
   */
  public static StackManipulation getValueForRow(
      StackManipulation readValue,  // Reads the value from the object.
      Class<?> valueClass) {
    ForLoadedType valueType = new ForLoadedType(valueClass);

    if (valueType.isArray() && !valueType.getComponentType().equals(BYTE_TYPE)) {
      // Byte arrays are special, so leave those alone.
      return new Compound(captureArrayForGetter(readValue, valueType));
    } else if (ReadableInstant.class.isAssignableFrom(valueClass) &&
        !ReadableDateTime.class.isAssignableFrom(valueClass)) {
      // If the POJO contains an Instant that is not a ReadableDateTime, we must make it a
      // ReadableDateTime before returning it.
      return captureDateTimeForGetter(readValue);
    } else if (ByteBuffer.class.isAssignableFrom(valueClass)) {
      return captureByteBufferForGetter(readValue);
    } else if (CharSequence.class.isAssignableFrom(valueClass)
        && !String.class.isAssignableFrom(valueClass)) {
      return captureStringForGetter(readValue);
    } else if (valueType.isPrimitive()) {
      return capturePrimitiveForGetter(readValue, valueType);
    } else {
      return readValue;
    }
  }

  // Prepare a value read from a Row to be set to a value of type valueClass. Does any necessary
  // conversions.
  public static StackManipulation prepareSetValueFromRow(
      StackManipulation readValue, Class<?> valueClass) {
    ForLoadedType valueType = new ForLoadedType(valueClass);
    StackManipulation stackManipulation;
    if (ReadableInstant.class.isAssignableFrom(valueClass)) {
      stackManipulation = captureDateTimeForSetter(readValue, valueType);
    } else if (valueClass.equals(ByteBuffer.class)) {
      stackManipulation = captureIntoByteBufferForSetter(readValue);
    } else if (CharSequence.class.isAssignableFrom(valueClass)
        && !valueClass.isAssignableFrom(String.class)) {
      stackManipulation = captureIntoCharSequenceForSetter(valueType, readValue);
    } else if (valueType.isArray() && !valueType.getComponentType().equals(BYTE_TYPE)) {
      // Byte arrays are special, so leave them alone.
      stackManipulation = captureIntoArrayForSetter(valueType, readValue);
    } else {
      // Otherwise cast to the matching field type.
      stackManipulation = new StackManipulation.Compound(
          readValue,
          TypeCasting.to(valueType.asBoxed()));
    }

    if (valueType.isPrimitive()) {
      // If the field type is primitive, then unbox the parameter before trying to assign it
      // to the field.
      stackManipulation = new StackManipulation.Compound(
          stackManipulation,
          Assigner.DEFAULT.assign(
              valueType.asBoxed().asGenericType(),
              valueType.asUnboxed().asGenericType(),
              Typing.STATIC));
    }
    return stackManipulation;
  }

  // If the Field is a container type, returns the element type. Otherwise returns a null reference.
  static Implementation getArrayComponentType(Type valueType) {
    if (valueType instanceof GenericArrayType) {
      Type component = ((GenericArrayType) valueType).getGenericComponentType();
      if (!component.equals(Byte.class) && !component.equals(byte.class)) {
        return FixedValue.reference(component);
      }
    } else if (valueType instanceof ParameterizedType) {
      ParameterizedType ptype = (ParameterizedType) valueType;
      Class raw = (Class) ptype.getRawType();
      java.lang.reflect.Type[] params = ptype.getActualTypeArguments();
      if (Collection.class.isAssignableFrom(raw)) {
        checkArgument(params.length == 1);
        if (!params[0].equals(Byte.class) && !params[0].equals(byte.class)) {
          return FixedValue.reference(params[0]);
        }
      }
    } else if (valueType instanceof Class) {
      Class clazz = (Class) valueType;
      if (clazz.isArray()) {
        Class componentType = clazz.getComponentType();
        if (componentType != Byte.TYPE) {
          return FixedValue.reference(componentType);
        }
      }
    }
    return FixedValue.nullValue();
  }

  // If the Field is a map type, returns the key type, otherwise returns a null reference.
  @Nullable
  public static Implementation getMapKeyType(Type valueType) {
    return getMapType(valueType, 0);
  }

  // If the Field is a map type, returns the value type, otherwise returns a null reference.
  @Nullable
  public static Implementation getMapValueType(Type valueType) {
    return getMapType(valueType, 1);
  }

  // If the Field is a map type, returns the key or value type (0 is key type, 1 is value).
  // Otherwise returns a null reference.
  public static Implementation getMapType(Type valueType, int index) {
    if (valueType instanceof ParameterizedType) {
      ParameterizedType ptype = (ParameterizedType) valueType;
      Class raw = (Class) ptype.getRawType();
      java.lang.reflect.Type[] params = ptype.getActualTypeArguments();
      if (Map.class.isAssignableFrom(raw)) {
        return FixedValue.reference(params[index]);
      }
    }
    return FixedValue.nullValue();
  }

  private static StackManipulation captureArrayForGetter(
      StackManipulation readValue, ForLoadedType valueType) {
    // return isComponentTypePrimitive ? Arrays.asList(ArrayUtils.toObject(value))
    //     : Arrays.asList(value);
    StackManipulation stackManipulation = readValue;
    // Row always expects to get an Iterable back for array types. Wrap this array into a
    // List using Arrays.asList before returning.
    StackManipulation toObject = null;
    if (valueType.getComponentType().isPrimitive()) {
      // Arrays.asList doesn't take primitive arrays, so convert first using ArrayUtils.toObject.
      stackManipulation = new Compound(
          stackManipulation,
          MethodInvocation.invoke(ARRAY_UTILS_TYPE.getDeclaredMethods()
              .filter(ElementMatchers.isStatic()
                  .and(ElementMatchers.named("toObject"))
                  .and(ElementMatchers.takesArguments(valueType)))
              .getOnly()));
    }
    return new Compound(
        stackManipulation,
        MethodInvocation.invoke(
            ARRAYS_TYPE.getDeclaredMethods()
                .filter(ElementMatchers.isStatic().and(ElementMatchers.named("asList")))
                .getOnly()));
  }

  private static StackManipulation captureDateTimeForGetter(StackManipulation readValue) {
    // return new DateTime(value.getMillis());
    return new StackManipulation.Compound(
        // Create a new instance of the target type.
        TypeCreation.of(DATE_TIME_TYPE),
        Duplication.SINGLE,
        readValue,
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

  private static StackManipulation captureByteBufferForGetter(StackManipulation readValue) {
    // return value.array();

    // We must extract the array from the ByteBuffer before returning.
    // NOTE: we only support array-backed byte buffers in these POJOs. Others (e.g. mmaped
    // files) are not supported.
    return new Compound(
        readValue,
        MethodInvocation.invoke(BYTE_BUFFER_TYPE.getDeclaredMethods()
            .filter(ElementMatchers.named("array")
                .and(ElementMatchers.returns(BYTE_ARRAY_TYPE)))
            .getOnly()));
  }

  private static StackManipulation captureStringForGetter(StackManipulation readValue) {
    // return value.toString();
    return new Compound(
        readValue,
        MethodInvocation.invoke(CHAR_SEQUENCE_TYPE.getDeclaredMethods()
            .filter(ElementMatchers.named("toString"))
            .getOnly()));
  }

  private static StackManipulation capturePrimitiveForGetter(
      StackManipulation readValue, ForLoadedType valueType) {
    // Box the primitive type.
    return new Compound(
        readValue,
        Assigner.DEFAULT.assign(
            valueType.asGenericType(),
            valueType.asBoxed().asGenericType(),
            Typing.STATIC));
  }

  // The setter might be called with a different subclass of ReadableInstant than the one stored
  // in this POJO. We must extract the value passed into the setter and copy it into an instance
  // that the POJO can accept.
  private static StackManipulation captureDateTimeForSetter(
      StackManipulation readValue, ForLoadedType fieldType) {
    // return new T(value.getMillis);
    return new Compound(
        // Create a new instance of the target type.
        TypeCreation.of(fieldType),
        Duplication.SINGLE,
        // Load the parameter and cast it to a ReadableInstant.
        readValue,
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

  private static StackManipulation captureIntoByteBufferForSetter(StackManipulation readValue) {
    // return ByteBuffer.wrap((byte[]) value);

    // We currently assume that a byte[] setter will always accept a parameter of type byte[].
    return new Compound(
        readValue,   // Load the value.
        TypeCasting.to(BYTE_ARRAY_TYPE),  // Cast to a byte[]
        // Create a new ByteBuffer that wraps this byte[].
        MethodInvocation.invoke(BYTE_BUFFER_TYPE
            .getDeclaredMethods()
            .filter(ElementMatchers.named("wrap")
                .and(ElementMatchers.takesArguments(BYTE_ARRAY_TYPE)))
            .getOnly()));
  }

  private static StackManipulation captureIntoCharSequenceForSetter(
      ForLoadedType fieldType, StackManipulation readValue) {
    // return new T(value.toString()).

    return new StackManipulation.Compound(
        TypeCreation.of(fieldType),
        Duplication.SINGLE,
        // Load the parameter and cast it to a CharSequence.
        readValue,
        TypeCasting.to(CHAR_SEQUENCE_TYPE),
        // Create an element of the field type that wraps this one.
        MethodInvocation.invoke(fieldType
            .getDeclaredMethods()
            .filter(ElementMatchers.isConstructor()
                .and(ElementMatchers.takesArguments(CHAR_SEQUENCE_TYPE)))
            .getOnly()));
  }

  private static StackManipulation captureIntoArrayForSetter(
      ForLoadedType fieldType, StackManipulation readValue) {
    // T[] toArray = (T[]) value.toArray(new T[0]);
    // return isPrimitive ? toArray : ArrayUtils.toPrimitive(toArray);

    // The type of the array containing the (possibly) boxed values.
    TypeDescription arrayType =
        TypeDescription.Generic.Builder.rawType(
            fieldType.getComponentType().asBoxed()).asArray().build().asErasure();

    // Extract an array from the collection.
    StackManipulation stackManipulation = new Compound(
        readValue,
        TypeCasting.to(LIST_TYPE),
        // Call Collection.toArray(T[[]) to extract the array. Push new T[0] on the stack before
        // calling toArray.
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
}
