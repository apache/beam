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

import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.description.type.TypeDescription.ForLoadedType;
import net.bytebuddy.dynamic.DynamicType;
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
import org.apache.avro.generic.GenericFixed;
import org.apache.beam.sdk.schemas.FieldValueGetter;
import org.apache.beam.sdk.schemas.FieldValueSetter;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeParameter;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.ClassUtils;
import org.joda.time.Instant;
import org.joda.time.ReadableInstant;

class ByteBuddyUtils {
  private static final ForLoadedType ARRAYS_TYPE = new ForLoadedType(Arrays.class);
  private static final ForLoadedType ARRAY_UTILS_TYPE = new ForLoadedType(ArrayUtils.class);
  private static final ForLoadedType BYTE_ARRAY_TYPE = new ForLoadedType(byte[].class);
  private static final ForLoadedType BYTE_BUFFER_TYPE = new ForLoadedType(ByteBuffer.class);
  private static final ForLoadedType CHAR_SEQUENCE_TYPE = new ForLoadedType(CharSequence.class);
  private static final ForLoadedType INSTANT_TYPE = new ForLoadedType(Instant.class);
  private static final ForLoadedType LIST_TYPE = new ForLoadedType(List.class);
  private static final ForLoadedType READABLE_INSTANT_TYPE =
      new ForLoadedType(ReadableInstant.class);

  // Create a new FieldValueGetter subclass.
  @SuppressWarnings("unchecked")
  static DynamicType.Builder<FieldValueGetter> subclassGetterInterface(
      ByteBuddy byteBuddy, Type objectType, Type fieldType) {
    TypeDescription.Generic getterGenericType =
        TypeDescription.Generic.Builder.parameterizedType(
                FieldValueGetter.class, objectType, fieldType)
            .build();
    return (DynamicType.Builder<FieldValueGetter>) byteBuddy.subclass(getterGenericType);
  }

  // Create a new FieldValueSetter subclass.
  @SuppressWarnings("unchecked")
  static DynamicType.Builder<FieldValueSetter> subclassSetterInterface(
      ByteBuddy byteBuddy, Type objectType, Type fieldType) {
    TypeDescription.Generic setterGenericType =
        TypeDescription.Generic.Builder.parameterizedType(
                FieldValueSetter.class, objectType, fieldType)
            .build();
    return (DynamicType.Builder<FieldValueSetter>) byteBuddy.subclass(setterGenericType);
  }

  // Base class used below to convert types.
  @SuppressWarnings("unchecked")
  abstract static class TypeConversion<T> {
    public T convert(TypeDescriptor typeDescriptor) {
      if (typeDescriptor.isArray()
          && !typeDescriptor.getComponentType().getRawType().equals(byte.class)) {
        // Byte arrays are special, so leave those alone.
        return convertArray(typeDescriptor);
      } else if (typeDescriptor.isSubtypeOf(TypeDescriptor.of(Collection.class))) {
        return convertCollection(typeDescriptor);
      } else if (typeDescriptor.isSubtypeOf(TypeDescriptor.of(Map.class))) {
        return convertMap(typeDescriptor);
      } else if (typeDescriptor.isSubtypeOf(TypeDescriptor.of(ReadableInstant.class))) {
        return convertDateTime(typeDescriptor);
      } else if (typeDescriptor.isSubtypeOf(TypeDescriptor.of(ByteBuffer.class))) {
        return convertByteBuffer(typeDescriptor);
      } else if (typeDescriptor.isSubtypeOf(TypeDescriptor.of(GenericFixed.class))) {
        // TODO: Refactor AVRO-specific check into separate class.
        return convertGenericFixed(typeDescriptor);
      } else if (typeDescriptor.isSubtypeOf(TypeDescriptor.of(CharSequence.class))) {
        return convertCharSequence(typeDescriptor);
      } else if (typeDescriptor.getRawType().isPrimitive()) {
        return convertPrimitive(typeDescriptor);
      } else {
        return convertDefault(typeDescriptor);
      }
    }

    protected abstract T convertArray(TypeDescriptor<?> type);

    protected abstract T convertCollection(TypeDescriptor<?> type);

    protected abstract T convertMap(TypeDescriptor<?> type);

    protected abstract T convertDateTime(TypeDescriptor<?> type);

    protected abstract T convertByteBuffer(TypeDescriptor<?> type);

    protected abstract T convertGenericFixed(TypeDescriptor<?> type);

    protected abstract T convertCharSequence(TypeDescriptor<?> type);

    protected abstract T convertPrimitive(TypeDescriptor<?> type);

    protected abstract T convertDefault(TypeDescriptor<?> type);
  }

  /**
   * Give a Java type, returns the Java type expected for use with Row. For example, both {@link
   * StringBuffer} and {@link String} are represented as a {@link String} in Row. This determines
   * what the return type of the getter will be. For instance, the following POJO class:
   *
   * <pre><code>
   * class POJO {
   *   StringBuffer str;
   *   int[] array;
   * }
   * </code></pre>
   *
   * Generates the following getters:
   *
   * <pre><code>{@literal FieldValueGetter<POJO, String>}</code></pre>
   *
   * <pre><code>{@literal FieldValueGetter<POJO, List<Integer>>}</code></pre>
   */
  static class ConvertType extends TypeConversion<Type> {
    private boolean returnRawTypes;

    public ConvertType(boolean returnRawTypes) {
      this.returnRawTypes = returnRawTypes;
    }

    @Override
    protected Type convertArray(TypeDescriptor<?> type) {
      TypeDescriptor ret = createListType(type);
      return returnRawTypes ? ret.getRawType() : ret.getType();
    }

    @Override
    protected Type convertCollection(TypeDescriptor<?> type) {
      return Collection.class;
    }

    @Override
    protected Type convertMap(TypeDescriptor<?> type) {
      return Map.class;
    }

    @Override
    protected Type convertDateTime(TypeDescriptor<?> type) {
      return Instant.class;
    }

    @Override
    protected Type convertByteBuffer(TypeDescriptor<?> type) {
      return byte[].class;
    }

    @Override
    protected Type convertGenericFixed(TypeDescriptor<?> type) {
      return byte[].class;
    }

    @Override
    protected Type convertCharSequence(TypeDescriptor<?> type) {
      return String.class;
    }

    @Override
    protected Type convertPrimitive(TypeDescriptor<?> type) {
      return ClassUtils.primitiveToWrapper(type.getRawType());
    }

    @Override
    protected Type convertDefault(TypeDescriptor<?> type) {
      return returnRawTypes ? type.getRawType() : type.getType();
    }

    @SuppressWarnings("unchecked")
    private <ElementT> TypeDescriptor<List<ElementT>> createListType(TypeDescriptor<?> type) {
      TypeDescriptor componentType =
          TypeDescriptor.of(ClassUtils.primitiveToWrapper(type.getComponentType().getRawType()));
      return new TypeDescriptor<List<ElementT>>() {}.where(
          new TypeParameter<ElementT>() {}, componentType);
    }
  }

  /**
   * Takes a {@link StackManipulation} that returns a value. Prepares this value to be returned by a
   * getter. {@link org.apache.beam.sdk.values.Row} needs getters to return specific types, but we
   * allow user objects to contain different but equivalent types. Therefore we must convert some of
   * these types before returning. These conversions correspond to the ones defined in {@link
   * ConvertType}. This class generates the code to do these conversion.
   */
  static class ConvertValueForGetter extends TypeConversion<StackManipulation> {
    // The code that reads the value.
    private final StackManipulation readValue;

    ConvertValueForGetter(StackManipulation readValue) {
      this.readValue = readValue;
    }

    @Override
    protected StackManipulation convertArray(TypeDescriptor<?> type) {
      // Generate the following code:
      // return isComponentTypePrimitive ? Arrays.asList(ArrayUtils.toObject(value))
      //     : Arrays.asList(value);

      ForLoadedType loadedType = new ForLoadedType(type.getRawType());
      StackManipulation stackManipulation = readValue;
      // Row always expects to get an Iterable back for array types. Wrap this array into a
      // List using Arrays.asList before returning.
      if (loadedType.getComponentType().isPrimitive()) {
        // Arrays.asList doesn't take primitive arrays, so convert first using ArrayUtils.toObject.
        stackManipulation =
            new Compound(
                stackManipulation,
                MethodInvocation.invoke(
                    ARRAY_UTILS_TYPE
                        .getDeclaredMethods()
                        .filter(
                            ElementMatchers.isStatic()
                                .and(ElementMatchers.named("toObject"))
                                .and(ElementMatchers.takesArguments(loadedType)))
                        .getOnly()));
      }
      return new Compound(
          stackManipulation,
          MethodInvocation.invoke(
              ARRAYS_TYPE
                  .getDeclaredMethods()
                  .filter(ElementMatchers.isStatic().and(ElementMatchers.named("asList")))
                  .getOnly()));
    }

    @Override
    protected StackManipulation convertCollection(TypeDescriptor<?> type) {
      return readValue;
    }

    @Override
    protected StackManipulation convertMap(TypeDescriptor<?> type) {
      return readValue;
    }

    @Override
    protected StackManipulation convertDateTime(TypeDescriptor<?> type) {
      // If the class type is an Instant, then return it.
      if (Instant.class.isAssignableFrom(type.getRawType())) {
        return readValue;
      }
      // Otherwise, generate the following code:
      //   return new Instant(value.getMillis());

      return new StackManipulation.Compound(
          // Create a new instance of the target type.
          TypeCreation.of(INSTANT_TYPE),
          Duplication.SINGLE,
          readValue,
          TypeCasting.to(READABLE_INSTANT_TYPE),
          // Call ReadableInstant.getMillis to extract the millis since the epoch.
          MethodInvocation.invoke(
              READABLE_INSTANT_TYPE
                  .getDeclaredMethods()
                  .filter(ElementMatchers.named("getMillis"))
                  .getOnly()),
          // Construct a DateTime object containing the millis.
          MethodInvocation.invoke(
              INSTANT_TYPE
                  .getDeclaredMethods()
                  .filter(
                      ElementMatchers.isConstructor()
                          .and(ElementMatchers.takesArguments(ForLoadedType.of(long.class))))
                  .getOnly()));
    }

    @Override
    protected StackManipulation convertByteBuffer(TypeDescriptor<?> type) {
      // Generate the following code:
      // return value.array();

      // We must extract the array from the ByteBuffer before returning.
      // NOTE: we only support array-backed byte buffers in these POJOs. Others (e.g. mmaped
      // files) are not supported.
      return new Compound(
          readValue,
          MethodInvocation.invoke(
              BYTE_BUFFER_TYPE
                  .getDeclaredMethods()
                  .filter(
                      ElementMatchers.named("array").and(ElementMatchers.returns(BYTE_ARRAY_TYPE)))
                  .getOnly()));
    }

    @Override
    protected StackManipulation convertGenericFixed(TypeDescriptor<?> type) {
      // TODO: Refactor AVRO-specific code into separate class.

      // Generate the following code:
      // return value.bytes();

      return new Compound(
          readValue,
          MethodInvocation.invoke(
              new ForLoadedType(GenericFixed.class)
                  .getDeclaredMethods()
                  .filter(
                      ElementMatchers.named("bytes").and(ElementMatchers.returns(BYTE_ARRAY_TYPE)))
                  .getOnly()));
    }

    @Override
    protected StackManipulation convertCharSequence(TypeDescriptor<?> type) {
      // If the member is a String, then return it.
      if (type.isSubtypeOf(TypeDescriptor.of(String.class))) {
        return readValue;
      }

      // Otherwise, generate the following code:
      // return value.toString();
      return new Compound(
          readValue,
          MethodInvocation.invoke(
              CHAR_SEQUENCE_TYPE
                  .getDeclaredMethods()
                  .filter(ElementMatchers.named("toString"))
                  .getOnly()));
    }

    @Override
    protected StackManipulation convertPrimitive(TypeDescriptor<?> type) {
      ForLoadedType loadedType = new ForLoadedType(type.getRawType());
      // Box the primitive type.
      return new Compound(
          readValue,
          Assigner.DEFAULT.assign(
              loadedType.asGenericType(), loadedType.asBoxed().asGenericType(), Typing.STATIC));
    }

    @Override
    protected StackManipulation convertDefault(TypeDescriptor<?> type) {
      return readValue;
    }
  }

  /**
   * Row is going to call the setter with its internal Java type, however the user object being set
   * might have a different type internally. For example, Row will be calling set with a {@link
   * String} type (for string fields), but the user type might have a {@link StringBuffer} member
   * there. This class generates code to convert between these types.
   */
  static class ConvertValueForSetter extends TypeConversion<StackManipulation> {
    StackManipulation readValue;

    ConvertValueForSetter(StackManipulation readValue) {
      this.readValue = readValue;
    }

    @Override
    protected StackManipulation convertArray(TypeDescriptor<?> type) {
      // Generate the following code:
      // T[] toArray = (T[]) value.toArray(new T[0]);
      // return isPrimitive ? toArray : ArrayUtils.toPrimitive(toArray);

      ForLoadedType loadedType = new ForLoadedType(type.getRawType());
      // The type of the array containing the (possibly) boxed values.
      TypeDescription arrayType =
          TypeDescription.Generic.Builder.rawType(loadedType.getComponentType().asBoxed())
              .asArray()
              .build()
              .asErasure();

      // Extract an array from the collection.
      StackManipulation stackManipulation =
          new Compound(
              readValue,
              TypeCasting.to(LIST_TYPE),
              // Call Collection.toArray(T[[]) to extract the array. Push new T[0] on the stack
              // before
              // calling toArray.
              ArrayFactory.forType(loadedType.getComponentType().asBoxed().asGenericType())
                  .withValues(Collections.emptyList()),
              MethodInvocation.invoke(
                  LIST_TYPE
                      .getDeclaredMethods()
                      .filter(
                          ElementMatchers.named("toArray").and(ElementMatchers.takesArguments(1)))
                      .getOnly()),
              // Cast the result to T[].
              TypeCasting.to(arrayType));

      if (loadedType.getComponentType().isPrimitive()) {
        // The array we extract will be an array of objects. If the pojo field is an array of
        // primitive types, we need to then convert to an array of unboxed objects.
        stackManipulation =
            new StackManipulation.Compound(
                stackManipulation,
                MethodInvocation.invoke(
                    ARRAY_UTILS_TYPE
                        .getDeclaredMethods()
                        .filter(
                            ElementMatchers.named("toPrimitive")
                                .and(ElementMatchers.takesArguments(arrayType)))
                        .getOnly()));
      }
      return stackManipulation;
    }

    @Override
    protected StackManipulation convertCollection(TypeDescriptor<?> type) {
      return readValue;
    }

    @Override
    protected StackManipulation convertMap(TypeDescriptor<?> type) {
      return readValue;
    }

    @Override
    protected StackManipulation convertDateTime(TypeDescriptor<?> type) {
      // The setter might be called with a different subclass of ReadableInstant than the one stored
      // in this POJO. We must extract the value passed into the setter and copy it into an instance
      // that the POJO can accept.

      // Generate the following code:
      // return new T(value.getMillis());

      ForLoadedType loadedType = new ForLoadedType(type.getRawType());
      return new Compound(
          // Create a new instance of the target type.
          TypeCreation.of(loadedType),
          Duplication.SINGLE,
          // Load the parameter and cast it to a ReadableInstant.
          readValue,
          TypeCasting.to(READABLE_INSTANT_TYPE),
          // Call ReadableInstant.getMillis to extract the millis since the epoch.
          MethodInvocation.invoke(
              READABLE_INSTANT_TYPE
                  .getDeclaredMethods()
                  .filter(ElementMatchers.named("getMillis"))
                  .getOnly()),
          // All subclasses of ReadableInstant contain a ()(long) constructor that takes in a millis
          // argument. Call that constructor of the field to initialize it.
          MethodInvocation.invoke(
              loadedType
                  .getDeclaredMethods()
                  .filter(
                      ElementMatchers.isConstructor()
                          .and(ElementMatchers.takesArguments(ForLoadedType.of(long.class))))
                  .getOnly()));
    }

    @Override
    protected StackManipulation convertByteBuffer(TypeDescriptor<?> type) {
      // Generate the following code:
      // return ByteBuffer.wrap((byte[]) value);

      // We currently assume that a byte[] setter will always accept a parameter of type byte[].
      return new Compound(
          readValue,
          TypeCasting.to(BYTE_ARRAY_TYPE),
          // Create a new ByteBuffer that wraps this byte[].
          MethodInvocation.invoke(
              BYTE_BUFFER_TYPE
                  .getDeclaredMethods()
                  .filter(
                      ElementMatchers.named("wrap")
                          .and(ElementMatchers.takesArguments(BYTE_ARRAY_TYPE)))
                  .getOnly()));
    }

    @Override
    protected StackManipulation convertGenericFixed(TypeDescriptor<?> type) {
      // Generate the following code:
      // return T((byte[]) value);

      // TODO: Refactor AVRO-specific code out of this class.
      ForLoadedType loadedType = new ForLoadedType(type.getRawType());
      return new Compound(
          TypeCreation.of(loadedType),
          Duplication.SINGLE,
          readValue,
          TypeCasting.to(BYTE_ARRAY_TYPE),
          // Create a new instance that wraps this byte[].
          MethodInvocation.invoke(
              loadedType
                  .getDeclaredMethods()
                  .filter(
                      ElementMatchers.isConstructor()
                          .and(ElementMatchers.takesArguments(BYTE_ARRAY_TYPE)))
                  .getOnly()));
    }

    @Override
    protected StackManipulation convertCharSequence(TypeDescriptor<?> type) {
      // If the type is a String, just return it.
      if (type.getRawType().isAssignableFrom(String.class)) {
        return readValue;
      }

      // Otherwise, generate the following code:
      // return new T((CharacterSequence) value).

      ForLoadedType loadedType = new ForLoadedType(type.getRawType());
      return new StackManipulation.Compound(
          TypeCreation.of(loadedType),
          Duplication.SINGLE,
          // Load the parameter and cast it to a CharSequence.
          readValue,
          TypeCasting.to(CHAR_SEQUENCE_TYPE),
          // Create an element of the field type that wraps this one.
          MethodInvocation.invoke(
              loadedType
                  .getDeclaredMethods()
                  .filter(
                      ElementMatchers.isConstructor()
                          .and(ElementMatchers.takesArguments(CHAR_SEQUENCE_TYPE)))
                  .getOnly()));
    }

    @Override
    protected StackManipulation convertPrimitive(TypeDescriptor<?> type) {
      ForLoadedType valueType = new ForLoadedType(type.getRawType());
      // Unbox the type.
      return new StackManipulation.Compound(
          readValue,
          Assigner.DEFAULT.assign(
              valueType.asBoxed().asGenericType(),
              valueType.asUnboxed().asGenericType(),
              Typing.STATIC));
    }

    @Override
    protected StackManipulation convertDefault(TypeDescriptor<?> type) {
      return readValue;
    }
  }
}
