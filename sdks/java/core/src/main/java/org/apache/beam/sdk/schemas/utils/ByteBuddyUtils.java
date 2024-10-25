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

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.NamingStrategy;
import net.bytebuddy.NamingStrategy.SuffixingRandom.BaseNameResolver;
import net.bytebuddy.asm.AsmVisitorWrapper;
import net.bytebuddy.description.method.MethodDescription.ForLoadedConstructor;
import net.bytebuddy.description.method.MethodDescription.ForLoadedMethod;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.description.type.TypeDescription.ForLoadedType;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.dynamic.scaffold.InstrumentedType;
import net.bytebuddy.implementation.Implementation;
import net.bytebuddy.implementation.Implementation.Context;
import net.bytebuddy.implementation.bytecode.ByteCodeAppender;
import net.bytebuddy.implementation.bytecode.Duplication;
import net.bytebuddy.implementation.bytecode.StackManipulation;
import net.bytebuddy.implementation.bytecode.StackManipulation.Compound;
import net.bytebuddy.implementation.bytecode.TypeCreation;
import net.bytebuddy.implementation.bytecode.assign.Assigner;
import net.bytebuddy.implementation.bytecode.assign.Assigner.Typing;
import net.bytebuddy.implementation.bytecode.assign.TypeCasting;
import net.bytebuddy.implementation.bytecode.collection.ArrayAccess;
import net.bytebuddy.implementation.bytecode.collection.ArrayFactory;
import net.bytebuddy.implementation.bytecode.constant.IntegerConstant;
import net.bytebuddy.implementation.bytecode.constant.NullConstant;
import net.bytebuddy.implementation.bytecode.member.FieldAccess;
import net.bytebuddy.implementation.bytecode.member.MethodInvocation;
import net.bytebuddy.implementation.bytecode.member.MethodReturn;
import net.bytebuddy.implementation.bytecode.member.MethodVariableAccess;
import net.bytebuddy.jar.asm.ClassWriter;
import net.bytebuddy.jar.asm.Label;
import net.bytebuddy.jar.asm.MethodVisitor;
import net.bytebuddy.jar.asm.Opcodes;
import net.bytebuddy.matcher.ElementMatchers;
import net.bytebuddy.utility.RandomString;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.schemas.FieldValueGetter;
import org.apache.beam.sdk.schemas.FieldValueSetter;
import org.apache.beam.sdk.schemas.FieldValueTypeInformation;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeParameter;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Function;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Collections2;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.primitives.Primitives;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.ClassUtils;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;
import org.joda.time.ReadableInstant;
import org.joda.time.ReadablePartial;
import org.joda.time.base.BaseLocal;

@Internal
@SuppressWarnings({
  "keyfor",
  "nullness", // TODO(https://github.com/apache/beam/issues/20497)
  "rawtypes"
})
public class ByteBuddyUtils {
  private static final ForLoadedType ARRAYS_TYPE = new ForLoadedType(Arrays.class);
  private static final ForLoadedType ARRAY_UTILS_TYPE = new ForLoadedType(ArrayUtils.class);
  private static final ForLoadedType BYTE_ARRAY_TYPE = new ForLoadedType(byte[].class);
  private static final ForLoadedType BYTE_BUFFER_TYPE = new ForLoadedType(ByteBuffer.class);
  private static final ForLoadedType CHAR_SEQUENCE_TYPE = new ForLoadedType(CharSequence.class);
  private static final ForLoadedType INSTANT_TYPE = new ForLoadedType(Instant.class);
  private static final ForLoadedType DATE_TIME_ZONE_TYPE = new ForLoadedType(DateTimeZone.class);
  private static final ForLoadedType COLLECTION_TYPE = new ForLoadedType(Collection.class);
  private static final ForLoadedType READABLE_INSTANT_TYPE =
      new ForLoadedType(ReadableInstant.class);
  private static final ForLoadedType READABLE_PARTIAL_TYPE =
      new ForLoadedType(ReadablePartial.class);
  private static final ForLoadedType INTEGER_TYPE = new ForLoadedType(Integer.class);
  private static final ForLoadedType ENUM_TYPE = new ForLoadedType(Enum.class);
  private static final ForLoadedType BYTE_BUDDY_UTILS_TYPE =
      new ForLoadedType(ByteBuddyUtils.class);

  /**
   * A naming strategy for ByteBuddy classes.
   *
   * <p>We always inject the generator classes in the same same package as the user's target class.
   * This way, if the class fields or methods are package private, our generated class can still
   * access them.
   */
  public static class InjectPackageStrategy extends NamingStrategy.AbstractBase {
    /** A resolver for the base name for naming the unnamed type. */
    private static final BaseNameResolver baseNameResolver =
        BaseNameResolver.ForUnnamedType.INSTANCE;

    private static final String SUFFIX = "SchemaCodeGen";

    private final RandomString randomString;

    private final @Nullable String targetPackage;

    public InjectPackageStrategy(Class<?> baseType) {
      randomString = new RandomString();
      this.targetPackage = baseType.getPackage() != null ? baseType.getPackage().getName() : null;
    }

    @Override
    protected String name(TypeDescription superClass) {
      String baseName = baseNameResolver.resolve(superClass);
      int lastDot = baseName.lastIndexOf('.');
      String className = baseName.substring(lastDot, baseName.length());
      // If the target class is in a prohibited package (java.*) then leave the original package
      // alone.
      String realPackage =
          overridePackage(targetPackage) ? targetPackage : superClass.getPackage().getName();
      return realPackage + className + "$" + SUFFIX + "$" + randomString.nextString();
    }

    private static boolean overridePackage(@Nullable String targetPackage) {
      return targetPackage != null && !targetPackage.startsWith("java.");
    }
  };

  static class IfNullElse implements StackManipulation {
    private final StackManipulation readValue;
    private final StackManipulation onNull;
    private final StackManipulation onNotNull;

    IfNullElse(StackManipulation readValue, StackManipulation onNull, StackManipulation onNotNull) {
      this.readValue = readValue;
      this.onNull = onNull;
      this.onNotNull = onNotNull;
    }

    @Override
    public boolean isValid() {
      return true;
    }

    @Override
    public StackManipulation.Size apply(MethodVisitor methodVisitor, Context context) {
      StackManipulation.Size size = new StackManipulation.Size(0, 0);
      size = size.aggregate(readValue.apply(methodVisitor, context));
      Label label = new Label();
      Label skipLabel = new Label();
      methodVisitor.visitJumpInsn(Opcodes.IFNONNULL, label);
      size = size.aggregate(new StackManipulation.Size(-1, 0));
      size = size.aggregate(onNull.apply(methodVisitor, context));
      methodVisitor.visitJumpInsn(Opcodes.GOTO, skipLabel);
      size = size.aggregate(new StackManipulation.Size(0, 1));
      methodVisitor.visitLabel(label);
      // We set COMPUTE_FRAMES on our builders, which causes ASM to calculate the correct frame
      // information to insert here.
      size = size.aggregate(onNotNull.apply(methodVisitor, context));
      methodVisitor.visitLabel(skipLabel);
      return size;
    }
  }

  // This StackManipulation returns onNotNull if the result of readValue is not null. Otherwise it
  // returns null.
  static class ShortCircuitReturnNull extends IfNullElse {
    ShortCircuitReturnNull(StackManipulation readValue, StackManipulation onNotNull) {
      super(readValue, NullConstant.INSTANCE, onNotNull);
    }
  }

  // Create a new FieldValueGetter subclass.
  @SuppressWarnings("unchecked")
  public static DynamicType.Builder<FieldValueGetter> subclassGetterInterface(
      ByteBuddy byteBuddy, Type objectType, Type fieldType) {
    TypeDescription.Generic getterGenericType =
        TypeDescription.Generic.Builder.parameterizedType(
                FieldValueGetter.class, objectType, fieldType)
            .build();
    return (DynamicType.Builder<FieldValueGetter>)
        byteBuddy.with(new InjectPackageStrategy((Class) objectType)).subclass(getterGenericType);
  }

  // Create a new FieldValueSetter subclass.
  @SuppressWarnings("unchecked")
  public static DynamicType.Builder<FieldValueSetter> subclassSetterInterface(
      ByteBuddy byteBuddy, Type objectType, Type fieldType) {
    TypeDescription.Generic setterGenericType =
        TypeDescription.Generic.Builder.parameterizedType(
                FieldValueSetter.class, objectType, fieldType)
            .build();
    return (DynamicType.Builder<FieldValueSetter>)
        byteBuddy.with(new InjectPackageStrategy((Class) objectType)).subclass(setterGenericType);
  }

  public interface TypeConversionsFactory {
    TypeConversion<Type> createTypeConversion(boolean returnRawTypes);

    TypeConversion<StackManipulation> createGetterConversions(StackManipulation readValue);

    TypeConversion<StackManipulation> createSetterConversions(StackManipulation readValue);
  }

  public static class DefaultTypeConversionsFactory implements TypeConversionsFactory {
    @Override
    public TypeConversion<Type> createTypeConversion(boolean returnRawTypes) {
      return new ConvertType(returnRawTypes);
    }

    @Override
    public TypeConversion<StackManipulation> createGetterConversions(StackManipulation readValue) {
      return new ConvertValueForGetter(readValue);
    }

    @Override
    public TypeConversion<StackManipulation> createSetterConversions(StackManipulation readValue) {
      return new ConvertValueForSetter(readValue);
    }
  }

  // Base class used below to convert types.
  @SuppressWarnings("unchecked")
  public abstract static class TypeConversion<T> {
    public T convert(TypeDescriptor typeDescriptor) {
      if (typeDescriptor.isArray()
          && !typeDescriptor.getComponentType().getRawType().equals(byte.class)) {
        // Byte arrays are special, so leave those alone.
        return convertArray(typeDescriptor);
      } else if (typeDescriptor.isSubtypeOf(TypeDescriptor.of(Map.class))) {
        return convertMap(typeDescriptor);
      } else if (typeDescriptor.isSubtypeOf(TypeDescriptor.of(ReadableInstant.class))) {
        return convertDateTime(typeDescriptor);
      } else if (typeDescriptor.isSubtypeOf(TypeDescriptor.of(ReadablePartial.class))) {
        return convertDateTime(typeDescriptor);
      } else if (typeDescriptor.isSubtypeOf(TypeDescriptor.of(ByteBuffer.class))) {
        return convertByteBuffer(typeDescriptor);
      } else if (typeDescriptor.isSubtypeOf(TypeDescriptor.of(CharSequence.class))) {
        return convertCharSequence(typeDescriptor);
      } else if (typeDescriptor.getRawType().isPrimitive()) {
        return convertPrimitive(typeDescriptor);
      } else if (typeDescriptor.getRawType().isEnum()) {
        return convertEnum(typeDescriptor);
      } else if (typeDescriptor.isSubtypeOf(TypeDescriptor.of(Iterable.class))) {
        if (typeDescriptor.isSubtypeOf(TypeDescriptor.of(List.class))) {
          return convertList(typeDescriptor);
        } else if (typeDescriptor.isSubtypeOf(TypeDescriptor.of(Collection.class))) {
          return convertCollection(typeDescriptor);
        } else {
          return convertIterable(typeDescriptor);
        }
      } else {
        return convertDefault(typeDescriptor);
      }
    }

    protected StackManipulation shortCircuitReturnNull(
        StackManipulation readValue, StackManipulation onNotNull) {
      return new ShortCircuitReturnNull(readValue, onNotNull);
    }

    protected abstract T convertArray(TypeDescriptor<?> type);

    protected abstract T convertIterable(TypeDescriptor<?> type);

    protected abstract T convertCollection(TypeDescriptor<?> type);

    protected abstract T convertList(TypeDescriptor<?> type);

    protected abstract T convertMap(TypeDescriptor<?> type);

    protected abstract T convertDateTime(TypeDescriptor<?> type);

    protected abstract T convertByteBuffer(TypeDescriptor<?> type);

    protected abstract T convertCharSequence(TypeDescriptor<?> type);

    protected abstract T convertPrimitive(TypeDescriptor<?> type);

    protected abstract T convertEnum(TypeDescriptor<?> type);

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
  public static class ConvertType extends TypeConversion<Type> {
    private boolean returnRawTypes;

    protected ConvertType(boolean returnRawTypes) {
      this.returnRawTypes = returnRawTypes;
    }

    @Override
    protected Type convertArray(TypeDescriptor<?> type) {
      TypeDescriptor ret = createCollectionType(type.getComponentType());
      return returnRawTypes ? ret.getRawType() : ret.getType();
    }

    @Override
    protected Type convertCollection(TypeDescriptor<?> type) {
      TypeDescriptor ret = createCollectionType(ReflectUtils.getIterableComponentType(type));
      return returnRawTypes ? ret.getRawType() : ret.getType();
    }

    @Override
    protected Type convertList(TypeDescriptor<?> type) {
      TypeDescriptor ret = createCollectionType(ReflectUtils.getIterableComponentType(type));
      return returnRawTypes ? ret.getRawType() : ret.getType();
    }

    @Override
    protected Type convertIterable(TypeDescriptor<?> type) {
      TypeDescriptor ret = createIterableType(ReflectUtils.getIterableComponentType(type));
      return returnRawTypes ? ret.getRawType() : ret.getType();
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
    protected Type convertCharSequence(TypeDescriptor<?> type) {
      return String.class;
    }

    @Override
    protected Type convertPrimitive(TypeDescriptor<?> type) {
      return ClassUtils.primitiveToWrapper(type.getRawType());
    }

    @Override
    protected Type convertEnum(TypeDescriptor<?> type) {
      return Integer.class;
    }

    @Override
    protected Type convertDefault(TypeDescriptor<?> type) {
      return returnRawTypes ? type.getRawType() : type.getType();
    }

    @SuppressWarnings("unchecked")
    private <ElementT> TypeDescriptor<Collection<ElementT>> createCollectionType(
        TypeDescriptor<?> componentType) {
      TypeDescriptor wrappedComponentType =
          TypeDescriptor.of(ClassUtils.primitiveToWrapper(componentType.getRawType()));
      return new TypeDescriptor<Collection<ElementT>>() {}.where(
          new TypeParameter<ElementT>() {}, wrappedComponentType);
    }

    @SuppressWarnings("unchecked")
    private <ElementT> TypeDescriptor<Iterable<ElementT>> createIterableType(
        TypeDescriptor<?> componentType) {
      TypeDescriptor wrappedComponentType =
          TypeDescriptor.of(ClassUtils.primitiveToWrapper(componentType.getRawType()));
      return new TypeDescriptor<Iterable<ElementT>>() {}.where(
          new TypeParameter<ElementT>() {}, wrappedComponentType);
    }
  }

  private static final ByteBuddy BYTE_BUDDY = new ByteBuddy();

  // When processing a container (e.g. List<T>) we need to recursively process the element type.
  // This function
  // generates a subclass of Function that can be used to recursively transform each element of the
  // container.
  static Class createCollectionTransformFunction(
      Type fromType, Type toType, Function<StackManipulation, StackManipulation> convertElement) {
    // Generate a TypeDescription for the class we want to generate.
    TypeDescription.Generic functionGenericType =
        TypeDescription.Generic.Builder.parameterizedType(
                Function.class, Primitives.wrap((Class) fromType), Primitives.wrap((Class) toType))
            .build();

    DynamicType.Builder<Function> builder =
        (DynamicType.Builder<Function>)
            BYTE_BUDDY
                .with(new InjectPackageStrategy((Class) fromType))
                .subclass(functionGenericType)
                .method(ElementMatchers.named("apply"))
                .intercept(
                    new Implementation() {
                      @Override
                      public ByteCodeAppender appender(Target target) {
                        return (methodVisitor, implementationContext, instrumentedMethod) -> {
                          // this + method parameters.
                          int numLocals = 1 + instrumentedMethod.getParameters().size();

                          StackManipulation readValue = MethodVariableAccess.REFERENCE.loadFrom(1);
                          StackManipulation stackManipulation =
                              new StackManipulation.Compound(
                                  convertElement.apply(readValue), MethodReturn.REFERENCE);

                          StackManipulation.Size size =
                              stackManipulation.apply(methodVisitor, implementationContext);
                          return new ByteCodeAppender.Size(size.getMaximalSize(), numLocals);
                        };
                      }

                      @Override
                      public InstrumentedType prepare(InstrumentedType instrumentedType) {
                        return instrumentedType;
                      }
                    });

    return builder
        .visit(new AsmVisitorWrapper.ForDeclaredMethods().writerFlags(ClassWriter.COMPUTE_FRAMES))
        .make()
        .load(
            ReflectHelpers.findClassLoader(((Class) fromType).getClassLoader()),
            getClassLoadingStrategy(
                ((Class) fromType).getClassLoader() == null ? Function.class : (Class) fromType))
        .getLoaded();
  }

  // A function to transform a container, special casing List and Collection types. This is used in
  // byte-buddy
  // generated code.
  public static <FromT, DestT> Iterable<DestT> transformContainer(
      Iterable<FromT> iterable, Function<FromT, DestT> function) {
    if (iterable instanceof List) {
      return Lists.transform((List<FromT>) iterable, function);
    } else if (iterable instanceof Collection) {
      return Collections2.transform((Collection<FromT>) iterable, function);
    } else {
      return Iterables.transform(iterable, function);
    }
  }

  static StackManipulation createTransformingContainer(
      ForLoadedType functionType, StackManipulation readValue) {
    StackManipulation stackManipulation =
        new Compound(
            readValue,
            TypeCreation.of(functionType),
            Duplication.SINGLE,
            MethodInvocation.invoke(
                functionType
                    .getDeclaredMethods()
                    .filter(ElementMatchers.isConstructor().and(ElementMatchers.takesArguments(0)))
                    .getOnly()),
            MethodInvocation.invoke(
                BYTE_BUDDY_UTILS_TYPE
                    .getDeclaredMethods()
                    .filter(ElementMatchers.named("transformContainer"))
                    .getOnly()));
    return stackManipulation;
  }

  public static <K1, V1, K2, V2> TransformingMap<K1, V1, K2, V2> getTransformingMap(
      Map<K1, V1> sourceMap, Function<K1, K2> keyFunction, Function<V1, V2> valueFunction) {
    return new TransformingMap<>(sourceMap, keyFunction, valueFunction);
  }

  public static class TransformingMap<K1, V1, K2, V2> implements Map<K2, V2>, Serializable {
    private final Map<K2, V2> delegateMap;

    public TransformingMap(
        Map<K1, V1> sourceMap, Function<K1, K2> keyFunction, Function<V1, V2> valueFunction) {
      if (sourceMap instanceof SortedMap) {
        delegateMap =
            (Map<K2, V2>)
                Maps.newTreeMap(); // We don't support copying the comparator. Makes no sense if key
        // is changing.
      } else {
        delegateMap = Maps.newHashMap();
      }
      for (Map.Entry<K1, V1> entry : sourceMap.entrySet()) {
        delegateMap.put(keyFunction.apply(entry.getKey()), valueFunction.apply(entry.getValue()));
      }
    }

    @Override
    public int size() {
      return delegateMap.size();
    }

    @Override
    public boolean isEmpty() {
      return delegateMap.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
      return delegateMap.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
      return delegateMap.containsValue(value);
    }

    @Override
    public V2 get(Object key) {
      return delegateMap.get(key);
    }

    @Override
    public V2 put(K2 key, V2 value) {
      return delegateMap.put(key, value);
    }

    @Override
    public V2 remove(Object key) {
      return delegateMap.remove(key);
    }

    @Override
    public void putAll(Map<? extends K2, ? extends V2> m) {
      delegateMap.putAll(m);
    }

    @Override
    public void clear() {
      delegateMap.clear();
      ;
    }

    @Override
    public Set<K2> keySet() {
      return delegateMap.keySet();
    }

    @Override
    public Collection<V2> values() {
      return delegateMap.values();
    }

    @Override
    public Set<Entry<K2, V2>> entrySet() {
      return delegateMap.entrySet();
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TransformingMap<?, ?, ?, ?> that = (TransformingMap<?, ?, ?, ?>) o;
      return Objects.equals(delegateMap, that.delegateMap);
    }

    @Override
    public int hashCode() {
      return Objects.hash(delegateMap);
    }

    @Override
    public String toString() {
      return delegateMap.toString();
    }
  }

  /**
   * Takes a {@link StackManipulation} that returns a value. Prepares this value to be returned by a
   * getter. {@link org.apache.beam.sdk.values.Row} needs getters to return specific types, but we
   * allow user objects to contain different but equivalent types. Therefore we must convert some of
   * these types before returning. These conversions correspond to the ones defined in {@link
   * ConvertType}. This class generates the code to do these conversion.
   */
  public static class ConvertValueForGetter extends TypeConversion<StackManipulation> {
    // The code that reads the value.
    protected final StackManipulation readValue;

    protected ConvertValueForGetter(StackManipulation readValue) {
      this.readValue = readValue;
    }

    protected TypeConversionsFactory getFactory() {
      return new DefaultTypeConversionsFactory();
    }

    @Override
    protected StackManipulation convertArray(TypeDescriptor<?> type) {
      // Generate the following code:
      // return isComponentTypePrimitive ? Arrays.asList(ArrayUtils.toObject(value))
      //     : Arrays.asList(value);

      TypeDescriptor<?> componentType = type.getComponentType();
      ForLoadedType loadedArrayType = new ForLoadedType(type.getRawType());
      StackManipulation readArrayValue = readValue;
      // Row always expects to get an Iterable back for array types. Wrap this array into a
      // List using Arrays.asList before returning.
      if (loadedArrayType.getComponentType().isPrimitive()) {
        // Arrays.asList doesn't take primitive arrays, so convert first using ArrayUtils.toObject.
        readArrayValue =
            new Compound(
                readArrayValue,
                MethodInvocation.invoke(
                    ARRAY_UTILS_TYPE
                        .getDeclaredMethods()
                        .filter(
                            ElementMatchers.isStatic()
                                .and(ElementMatchers.named("toObject"))
                                .and(ElementMatchers.takesArguments(loadedArrayType)))
                        .getOnly()));

        componentType = TypeDescriptor.of(Primitives.wrap(componentType.getRawType()));
      }
      // Now convert to a List object.
      StackManipulation readListValue =
          new Compound(
              readArrayValue,
              MethodInvocation.invoke(
                  ARRAYS_TYPE
                      .getDeclaredMethods()
                      .filter(ElementMatchers.isStatic().and(ElementMatchers.named("asList")))
                      .getOnly()));

      // Generate a SerializableFunction to convert the element-type objects.
      StackManipulation stackManipulation;
      final TypeDescriptor finalComponentType = ReflectUtils.boxIfPrimitive(componentType);
      if (!finalComponentType.hasUnresolvedParameters()) {
        Type convertedComponentType =
            getFactory().createTypeConversion(true).convert(componentType);
        ForLoadedType functionType =
            new ForLoadedType(
                createCollectionTransformFunction(
                    componentType.getRawType(),
                    convertedComponentType,
                    (s) -> getFactory().createGetterConversions(s).convert(finalComponentType)));
        stackManipulation = createTransformingContainer(functionType, readListValue);
      } else {
        stackManipulation = readListValue;
      }
      return new ShortCircuitReturnNull(readValue, stackManipulation);
    }

    @Override
    protected StackManipulation convertIterable(TypeDescriptor<?> type) {
      TypeDescriptor componentType = ReflectUtils.getIterableComponentType(type);
      Type convertedComponentType = getFactory().createTypeConversion(true).convert(componentType);

      final TypeDescriptor finalComponentType = ReflectUtils.boxIfPrimitive(componentType);
      if (!finalComponentType.hasUnresolvedParameters()) {
        ForLoadedType functionType =
            new ForLoadedType(
                createCollectionTransformFunction(
                    componentType.getRawType(),
                    convertedComponentType,
                    (s) -> getFactory().createGetterConversions(s).convert(finalComponentType)));
        StackManipulation stackManipulation = createTransformingContainer(functionType, readValue);
        return new ShortCircuitReturnNull(readValue, stackManipulation);
      } else {
        return readValue;
      }
    }

    @Override
    protected StackManipulation convertCollection(TypeDescriptor<?> type) {
      TypeDescriptor componentType = ReflectUtils.getIterableComponentType(type);
      Type convertedComponentType = getFactory().createTypeConversion(true).convert(componentType);
      final TypeDescriptor finalComponentType = ReflectUtils.boxIfPrimitive(componentType);
      if (!finalComponentType.hasUnresolvedParameters()) {
        ForLoadedType functionType =
            new ForLoadedType(
                createCollectionTransformFunction(
                    componentType.getRawType(),
                    convertedComponentType,
                    (s) -> getFactory().createGetterConversions(s).convert(finalComponentType)));
        StackManipulation stackManipulation = createTransformingContainer(functionType, readValue);
        return new ShortCircuitReturnNull(readValue, stackManipulation);
      } else {
        return readValue;
      }
    }

    @Override
    protected StackManipulation convertList(TypeDescriptor<?> type) {
      TypeDescriptor componentType = ReflectUtils.getIterableComponentType(type);
      Type convertedComponentType = getFactory().createTypeConversion(true).convert(componentType);
      final TypeDescriptor finalComponentType = ReflectUtils.boxIfPrimitive(componentType);
      if (!finalComponentType.hasUnresolvedParameters()) {
        ForLoadedType functionType =
            new ForLoadedType(
                createCollectionTransformFunction(
                    componentType.getRawType(),
                    convertedComponentType,
                    (s) -> getFactory().createGetterConversions(s).convert(finalComponentType)));
        StackManipulation stackManipulation = createTransformingContainer(functionType, readValue);
        return new ShortCircuitReturnNull(readValue, stackManipulation);
      } else {
        return readValue;
      }
    }

    @Override
    protected StackManipulation convertMap(TypeDescriptor<?> type) {
      final TypeDescriptor keyType = ReflectUtils.getMapType(type, 0);
      final TypeDescriptor valueType = ReflectUtils.getMapType(type, 1);

      Type convertedKeyType = getFactory().createTypeConversion(true).convert(keyType);
      Type convertedValueType = getFactory().createTypeConversion(true).convert(valueType);

      if (!keyType.hasUnresolvedParameters() && !valueType.hasUnresolvedParameters()) {
        ForLoadedType keyFunctionType =
            new ForLoadedType(
                createCollectionTransformFunction(
                    keyType.getRawType(),
                    convertedKeyType,
                    (s) -> getFactory().createGetterConversions(s).convert(keyType)));
        ForLoadedType valueFunctionType =
            new ForLoadedType(
                createCollectionTransformFunction(
                    valueType.getRawType(),
                    convertedValueType,
                    (s) -> getFactory().createGetterConversions(s).convert(valueType)));
        StackManipulation stackManipulation =
            new Compound(
                readValue,
                TypeCreation.of(keyFunctionType),
                Duplication.SINGLE,
                MethodInvocation.invoke(
                    keyFunctionType
                        .getDeclaredMethods()
                        .filter(
                            ElementMatchers.isConstructor().and(ElementMatchers.takesArguments(0)))
                        .getOnly()),
                TypeCreation.of(valueFunctionType),
                Duplication.SINGLE,
                MethodInvocation.invoke(
                    valueFunctionType
                        .getDeclaredMethods()
                        .filter(
                            ElementMatchers.isConstructor().and(ElementMatchers.takesArguments(0)))
                        .getOnly()),
                MethodInvocation.invoke(
                    BYTE_BUDDY_UTILS_TYPE
                        .getDeclaredMethods()
                        .filter(ElementMatchers.named("getTransformingMap"))
                        .getOnly()));
        return new ShortCircuitReturnNull(readValue, stackManipulation);
      } else {
        return readValue;
      }
    }

    @Override
    protected StackManipulation convertDateTime(TypeDescriptor<?> type) {
      // If the class type is an Instant, then return it.
      if (Instant.class.isAssignableFrom(type.getRawType())) {
        return readValue;
      }

      // Otherwise, generate the following code:
      //
      // for ReadableInstant:
      //   return new Instant(value.getMillis());
      //
      // for ReadablePartial:
      //   return new Instant((value.toDateTime(Instant.EPOCH)).getMillis());

      List<StackManipulation> stackManipulations = new ArrayList<>();

      // Create a new instance of the target type.
      stackManipulations.add(TypeCreation.of(INSTANT_TYPE));
      stackManipulations.add(Duplication.SINGLE);

      // if value is ReadablePartial, convert it to ReadableInstant first
      if (ReadablePartial.class.isAssignableFrom(type.getRawType())) {
        // Generate the following code: .toDateTime(Instant.EPOCH)

        // Load the parameter and cast it to ReadablePartial.
        stackManipulations.add(readValue);
        stackManipulations.add(TypeCasting.to(READABLE_PARTIAL_TYPE));

        // Get Instant.EPOCH
        stackManipulations.add(
            FieldAccess.forField(
                    INSTANT_TYPE
                        .getDeclaredFields()
                        .filter(ElementMatchers.named("EPOCH"))
                        .getOnly())
                .read());

        // Call ReadablePartial.toDateTime
        stackManipulations.add(
            MethodInvocation.invoke(
                READABLE_PARTIAL_TYPE
                    .getDeclaredMethods()
                    .filter(
                        ElementMatchers.named("toDateTime")
                            .and(ElementMatchers.takesArguments(READABLE_INSTANT_TYPE)))
                    .getOnly()));
      } else {
        // Otherwise, parameter is already ReadableInstant.
        // Load the parameter and cast it to ReadableInstant.
        stackManipulations.add(readValue);
        stackManipulations.add(TypeCasting.to(READABLE_INSTANT_TYPE));
      }

      // Call ReadableInstant.getMillis to extract the millis since the epoch.
      stackManipulations.add(
          MethodInvocation.invoke(
              READABLE_INSTANT_TYPE
                  .getDeclaredMethods()
                  .filter(ElementMatchers.named("getMillis"))
                  .getOnly()));

      // Construct a Instant object containing the millis.
      stackManipulations.add(
          MethodInvocation.invoke(
              INSTANT_TYPE
                  .getDeclaredMethods()
                  .filter(
                      ElementMatchers.isConstructor()
                          .and(ElementMatchers.takesArguments(ForLoadedType.of(long.class))))
                  .getOnly()));

      StackManipulation stackManipulation = new StackManipulation.Compound(stackManipulations);
      return new ShortCircuitReturnNull(readValue, stackManipulation);
    }

    @Override
    protected StackManipulation convertByteBuffer(TypeDescriptor<?> type) {
      // Generate the following code:
      // return value.array();

      // We must extract the array from the ByteBuffer before returning.
      // NOTE: we only support array-backed byte buffers in these POJOs. Others (e.g. mmaped
      // files) are not supported.
      StackManipulation stackManipulation =
          new Compound(
              readValue,
              MethodInvocation.invoke(
                  BYTE_BUFFER_TYPE
                      .getDeclaredMethods()
                      .filter(
                          ElementMatchers.named("array")
                              .and(ElementMatchers.returns(BYTE_ARRAY_TYPE)))
                      .getOnly()));
      return new ShortCircuitReturnNull(readValue, stackManipulation);
    }

    @Override
    protected StackManipulation convertCharSequence(TypeDescriptor<?> type) {
      // If the member is a String, then return it.
      if (type.isSubtypeOf(TypeDescriptor.of(String.class))) {
        return readValue;
      }

      // Otherwise, generate the following code:
      // return value.toString();
      StackManipulation stackManipulation =
          new Compound(
              readValue,
              MethodInvocation.invoke(
                  CHAR_SEQUENCE_TYPE
                      .getDeclaredMethods()
                      .filter(
                          ElementMatchers.named("toString").and(ElementMatchers.takesArguments(0)))
                      .getOnly()));
      return new ShortCircuitReturnNull(readValue, stackManipulation);
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
    protected StackManipulation convertEnum(TypeDescriptor<?> type) {
      StackManipulation stackManipulation =
          new Compound(
              readValue,
              MethodInvocation.invoke(
                  ENUM_TYPE
                      .getDeclaredMethods()
                      .filter(
                          ElementMatchers.named("ordinal").and(ElementMatchers.takesArguments(0)))
                      .getOnly()),
              Assigner.DEFAULT.assign(
                  INTEGER_TYPE.asUnboxed().asGenericType(),
                  INTEGER_TYPE.asGenericType(),
                  Typing.STATIC));
      return new ShortCircuitReturnNull(readValue, stackManipulation);
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
  public static class ConvertValueForSetter extends TypeConversion<StackManipulation> {
    protected StackManipulation readValue;

    protected ConvertValueForSetter(StackManipulation readValue) {
      this.readValue = readValue;
    }

    protected TypeConversionsFactory getFactory() {
      return new DefaultTypeConversionsFactory();
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

      Type rowElementType =
          getFactory().createTypeConversion(false).convert(type.getComponentType());
      final TypeDescriptor arrayElementType = ReflectUtils.boxIfPrimitive(type.getComponentType());
      StackManipulation readTransformedValue = readValue;
      if (!arrayElementType.hasUnresolvedParameters()) {
        ForLoadedType conversionFunction =
            new ForLoadedType(
                createCollectionTransformFunction(
                    TypeDescriptor.of(rowElementType).getRawType(),
                    Primitives.wrap(arrayElementType.getRawType()),
                    (s) -> getFactory().createSetterConversions(s).convert(arrayElementType)));
        readTransformedValue = createTransformingContainer(conversionFunction, readValue);
      }

      // Extract an array from the collection.
      StackManipulation stackManipulation =
          new Compound(
              readTransformedValue,
              TypeCasting.to(COLLECTION_TYPE),
              // Call Collection.toArray(T[[]) to extract the array. Push new T[0] on the stack
              // before
              // calling toArray.
              ArrayFactory.forType(loadedType.getComponentType().asBoxed().asGenericType())
                  .withValues(Collections.emptyList()),
              MethodInvocation.invoke(
                  COLLECTION_TYPE
                      .getDeclaredMethods()
                      .filter(
                          ElementMatchers.named("toArray")
                              .and(
                                  ElementMatchers.takesArguments(
                                      TypeDescription.Generic.Builder.rawType(Object.class)
                                          .asArray()
                                          .build()
                                          .asErasure())))
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
      return new ShortCircuitReturnNull(readValue, stackManipulation);
    }

    @Override
    protected StackManipulation convertIterable(TypeDescriptor<?> type) {
      Type rowElementType =
          getFactory()
              .createTypeConversion(false)
              .convert(ReflectUtils.getIterableComponentType(type));
      final TypeDescriptor iterableElementType = ReflectUtils.getIterableComponentType(type);
      if (!iterableElementType.hasUnresolvedParameters()) {
        ForLoadedType conversionFunction =
            new ForLoadedType(
                createCollectionTransformFunction(
                    TypeDescriptor.of(rowElementType).getRawType(),
                    iterableElementType.getRawType(),
                    (s) -> getFactory().createSetterConversions(s).convert(iterableElementType)));
        StackManipulation transformedContainer =
            createTransformingContainer(conversionFunction, readValue);
        return new ShortCircuitReturnNull(readValue, transformedContainer);
      } else {
        return readValue;
      }
    }

    @Override
    protected StackManipulation convertCollection(TypeDescriptor<?> type) {
      Type rowElementType =
          getFactory()
              .createTypeConversion(false)
              .convert(ReflectUtils.getIterableComponentType(type));
      final TypeDescriptor collectionElementType = ReflectUtils.getIterableComponentType(type);

      if (!collectionElementType.hasUnresolvedParameters()) {
        ForLoadedType conversionFunction =
            new ForLoadedType(
                createCollectionTransformFunction(
                    TypeDescriptor.of(rowElementType).getRawType(),
                    collectionElementType.getRawType(),
                    (s) -> getFactory().createSetterConversions(s).convert(collectionElementType)));
        StackManipulation transformedContainer =
            createTransformingContainer(conversionFunction, readValue);
        return new ShortCircuitReturnNull(readValue, transformedContainer);
      } else {
        return readValue;
      }
    }

    @Override
    protected StackManipulation convertList(TypeDescriptor<?> type) {
      Type rowElementType =
          getFactory()
              .createTypeConversion(false)
              .convert(ReflectUtils.getIterableComponentType(type));
      final TypeDescriptor collectionElementType = ReflectUtils.getIterableComponentType(type);

      StackManipulation readTrasformedValue = readValue;
      if (!collectionElementType.hasUnresolvedParameters()) {
        ForLoadedType conversionFunction =
            new ForLoadedType(
                createCollectionTransformFunction(
                    TypeDescriptor.of(rowElementType).getRawType(),
                    collectionElementType.getRawType(),
                    (s) -> getFactory().createSetterConversions(s).convert(collectionElementType)));
        readTrasformedValue = createTransformingContainer(conversionFunction, readValue);
      }
      // TODO: Don't copy if already a list!
      StackManipulation transformedList =
          new Compound(
              readTrasformedValue,
              MethodInvocation.invoke(
                  new ForLoadedType(Lists.class)
                      .getDeclaredMethods()
                      .filter(
                          ElementMatchers.named("newArrayList")
                              .and(ElementMatchers.takesArguments(Iterable.class)))
                      .getOnly()));
      return new ShortCircuitReturnNull(readValue, transformedList);
    }

    @Override
    protected StackManipulation convertMap(TypeDescriptor<?> type) {
      Type rowKeyType =
          getFactory().createTypeConversion(false).convert(ReflectUtils.getMapType(type, 0));
      final TypeDescriptor keyElementType = ReflectUtils.getMapType(type, 0);
      Type rowValueType =
          getFactory().createTypeConversion(false).convert(ReflectUtils.getMapType(type, 1));
      final TypeDescriptor valueElementType = ReflectUtils.getMapType(type, 1);

      StackManipulation readTrasformedValue = readValue;
      if (!keyElementType.hasUnresolvedParameters()
          && !valueElementType.hasUnresolvedParameters()) {
        ForLoadedType keyConversionFunction =
            new ForLoadedType(
                createCollectionTransformFunction(
                    TypeDescriptor.of(rowKeyType).getRawType(),
                    keyElementType.getRawType(),
                    (s) -> getFactory().createSetterConversions(s).convert(keyElementType)));
        ForLoadedType valueConversionFunction =
            new ForLoadedType(
                createCollectionTransformFunction(
                    TypeDescriptor.of(rowValueType).getRawType(),
                    valueElementType.getRawType(),
                    (s) -> getFactory().createSetterConversions(s).convert(valueElementType)));
        readTrasformedValue =
            new Compound(
                readValue,
                TypeCreation.of(keyConversionFunction),
                Duplication.SINGLE,
                MethodInvocation.invoke(
                    keyConversionFunction
                        .getDeclaredMethods()
                        .filter(
                            ElementMatchers.isConstructor().and(ElementMatchers.takesArguments(0)))
                        .getOnly()),
                TypeCreation.of(valueConversionFunction),
                Duplication.SINGLE,
                MethodInvocation.invoke(
                    valueConversionFunction
                        .getDeclaredMethods()
                        .filter(
                            ElementMatchers.isConstructor().and(ElementMatchers.takesArguments(0)))
                        .getOnly()),
                MethodInvocation.invoke(
                    BYTE_BUDDY_UTILS_TYPE
                        .getDeclaredMethods()
                        .filter(ElementMatchers.named("getTransformingMap"))
                        .getOnly()));
      }
      return new ShortCircuitReturnNull(readValue, readTrasformedValue);
    }

    @Override
    protected StackManipulation convertDateTime(TypeDescriptor<?> type) {
      // The setter might be called with a different subclass of ReadableInstant than the one stored
      // in this POJO. We must extract the value passed into the setter and copy it into an instance
      // that the POJO can accept.

      // Generate the following code:
      //   return new T(value.getMillis());
      // Unless T is a sub-class of BaseLocal. Then generate:
      //   return new T(value.getMillis(), DateTimeZone.UTC);

      ForLoadedType loadedType = new ForLoadedType(type.getRawType());
      List<StackManipulation> stackManipulations = new ArrayList<>();

      // Create a new instance of the target type.
      stackManipulations.add(TypeCreation.of(loadedType));
      stackManipulations.add(Duplication.SINGLE);
      // Load the parameter and cast it to a ReadableInstant.
      stackManipulations.add(readValue);
      stackManipulations.add(TypeCasting.to(READABLE_INSTANT_TYPE));
      // Call ReadableInstant.getMillis to extract the millis since the epoch.
      stackManipulations.add(
          MethodInvocation.invoke(
              READABLE_INSTANT_TYPE
                  .getDeclaredMethods()
                  .filter(ElementMatchers.named("getMillis"))
                  .getOnly()));

      if (type.isSubtypeOf(TypeDescriptor.of(BaseLocal.class))) {
        // Access DateTimeZone.UTC
        stackManipulations.add(
            FieldAccess.forField(
                    DATE_TIME_ZONE_TYPE
                        .getDeclaredFields()
                        .filter(ElementMatchers.named("UTC"))
                        .getOnly())
                .read());
        // All subclasses of BaseLocal contain a ()(long, DateTimeZone) constructor
        // that takes in a millis and time zone argument. Call that constructor of the field to
        // initialize it.
        stackManipulations.add(
            MethodInvocation.invoke(
                loadedType
                    .getDeclaredMethods()
                    .filter(
                        ElementMatchers.isConstructor()
                            .and(
                                ElementMatchers.takesArguments(
                                    ForLoadedType.of(long.class), DATE_TIME_ZONE_TYPE)))
                    .getOnly()));
      } else {
        // All subclasses of ReadableInstant and ReadablePartial contain a ()(long) constructor
        // that takes in a millis argument. Call that constructor of the field to initialize it.
        stackManipulations.add(
            MethodInvocation.invoke(
                loadedType
                    .getDeclaredMethods()
                    .filter(
                        ElementMatchers.isConstructor()
                            .and(ElementMatchers.takesArguments(ForLoadedType.of(long.class))))
                    .getOnly()));
      }

      StackManipulation stackManipulation = new Compound(stackManipulations);
      return new ShortCircuitReturnNull(readValue, stackManipulation);
    }

    @Override
    protected StackManipulation convertByteBuffer(TypeDescriptor<?> type) {
      // Generate the following code:
      // return ByteBuffer.wrap((byte[]) value);

      // We currently assume that a byte[] setter will always accept a parameter of type byte[].
      StackManipulation stackManipulation =
          new Compound(
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
      return new ShortCircuitReturnNull(readValue, stackManipulation);
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
      StackManipulation stackManipulation =
          new StackManipulation.Compound(
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
      return new ShortCircuitReturnNull(readValue, stackManipulation);
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
    protected StackManipulation convertEnum(TypeDescriptor<?> type) {
      ForLoadedType loadedType = new ForLoadedType(type.getRawType());

      // Convert the stored ordinal back to the Java enum constant.
      StackManipulation stackManipulation =
          new Compound(
              // Call EnumType::values() to get an array of all enum constants.
              MethodInvocation.invoke(
                  loadedType
                      .getDeclaredMethods()
                      .filter(
                          ElementMatchers.named("values")
                              .and(
                                  ElementMatchers.isStatic()
                                      .and(ElementMatchers.takesArguments(0))))
                      .getOnly()),
              // Read the integer enum value.
              readValue,
              // Unbox Integer -> int before accessing the array.
              Assigner.DEFAULT.assign(
                  INTEGER_TYPE.asBoxed().asGenericType(),
                  INTEGER_TYPE.asUnboxed().asGenericType(),
                  Typing.STATIC),
              // Access the array to return the Java enum type.
              ArrayAccess.REFERENCE.load());
      return new ShortCircuitReturnNull(readValue, stackManipulation);
    }

    @Override
    protected StackManipulation convertDefault(TypeDescriptor<?> type) {
      return readValue;
    }
  }

  /**
   * Invokes a constructor registered using SchemaCreate. As constructor parameters might not be in
   * the same order as the schema fields, reorders the parameters as necessary before calling the
   * constructor.
   */
  static class ConstructorCreateInstruction extends InvokeUserCreateInstruction {
    private final Constructor constructor;

    ConstructorCreateInstruction(
        List<FieldValueTypeInformation> fields,
        Class targetClass,
        Constructor constructor,
        TypeConversionsFactory typeConversionsFactory) {
      super(
          fields,
          targetClass,
          Lists.newArrayList(constructor.getParameters()),
          typeConversionsFactory);
      this.constructor = constructor;
    }

    @Override
    public InstrumentedType prepare(InstrumentedType instrumentedType) {
      return instrumentedType;
    }

    @Override
    protected StackManipulation beforePushingParameters() {
      // Create the target class.
      ForLoadedType loadedType = new ForLoadedType(targetClass);
      return new StackManipulation.Compound(TypeCreation.of(loadedType), Duplication.SINGLE);
    }

    @Override
    protected StackManipulation afterPushingParameters() {
      return MethodInvocation.invoke(new ForLoadedConstructor(constructor));
    }
  }

  /**
   * Invokes a static factory method registered using SchemaCreate. As the method parameters might
   * not be in the same order as the schema fields, reorders the parameters as necessary before
   * calling the constructor.
   */
  static class StaticFactoryMethodInstruction extends InvokeUserCreateInstruction {
    private final Method creator;

    StaticFactoryMethodInstruction(
        List<FieldValueTypeInformation> fields,
        Class targetClass,
        Method creator,
        TypeConversionsFactory typeConversionsFactory) {
      super(
          fields, targetClass, Lists.newArrayList(creator.getParameters()), typeConversionsFactory);
      if (!Modifier.isStatic(creator.getModifiers())) {
        throw new IllegalArgumentException("Method " + creator + " is not static");
      }
      this.creator = creator;
    }

    @Override
    public InstrumentedType prepare(InstrumentedType instrumentedType) {
      return instrumentedType;
    }

    @Override
    protected StackManipulation afterPushingParameters() {
      return MethodInvocation.invoke(new ForLoadedMethod(creator));
    }
  }

  static class InvokeUserCreateInstruction implements Implementation {
    protected final List<FieldValueTypeInformation> fields;
    protected final Class targetClass;
    protected final List<Parameter> parameters;
    protected final Map<Integer, Integer> fieldMapping;
    private final TypeConversionsFactory typeConversionsFactory;

    protected InvokeUserCreateInstruction(
        List<FieldValueTypeInformation> fields,
        Class targetClass,
        List<Parameter> parameters,
        TypeConversionsFactory typeConversionsFactory) {
      this.fields = fields;
      this.targetClass = targetClass;
      this.parameters = parameters;
      this.typeConversionsFactory = typeConversionsFactory;

      // Method parameters might not be in the same order as the schema fields, and the input
      // array to SchemaUserTypeCreator.create is in schema order. Examine the parameter names
      // and compare against field names to calculate the mapping between the two lists.
      Map<String, Integer> fieldsByLogicalName = Maps.newHashMap();
      Map<String, Integer> fieldsByJavaClassMember = Maps.newHashMap();
      for (int i = 0; i < fields.size(); ++i) {
        // Method parameters are allowed to either correspond to the schema field names or to the
        // actual Java field or method names.
        FieldValueTypeInformation fieldValue = checkNotNull(fields.get(i));
        fieldsByLogicalName.put(fieldValue.getName(), i);
        if (fieldValue.getField() != null) {
          fieldsByJavaClassMember.put(fieldValue.getField().getName(), i);
        } else if (fieldValue.getMethod() != null) {
          String name = ReflectUtils.stripGetterPrefix(fieldValue.getMethod().getName());
          fieldsByJavaClassMember.put(name, i);
        }
      }

      fieldMapping = Maps.newHashMap();
      for (int i = 0; i < parameters.size(); ++i) {
        Parameter parameter = parameters.get(i);
        String paramName = parameter.getName();
        Integer index = fieldsByLogicalName.get(paramName);
        if (index == null) {
          index = fieldsByJavaClassMember.get(paramName);
        }
        if (index == null && paramName.endsWith("$")) {
          // AutoValue appends "$" in some cases
          index = fieldsByJavaClassMember.get(paramName.substring(0, paramName.length() - 1));
        }
        if (index == null) {
          throw new RuntimeException(
              "Creator parameter "
                  + paramName
                  + " Doesn't correspond to a schema field."
                  + " Make sure that you are compiling with -parameters parameter to include"
                  + " constructor parameter information in class files.");
        }
        fieldMapping.put(i, index);
      }
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

        StackManipulation stackManipulation = beforePushingParameters();

        // Push all creator parameters on the stack.
        TypeConversion<Type> convertType = typeConversionsFactory.createTypeConversion(true);
        for (int i = 0; i < parameters.size(); i++) {
          Parameter parameter = parameters.get(i);
          ForLoadedType convertedType =
              new ForLoadedType(
                  (Class) convertType.convert(TypeDescriptor.of(parameter.getType())));

          // The instruction to read the parameter. Use the fieldMapping to reorder parameters as
          // necessary.
          StackManipulation readParameter =
              new StackManipulation.Compound(
                  MethodVariableAccess.REFERENCE.loadFrom(1),
                  IntegerConstant.forValue(fieldMapping.get(i)),
                  ArrayAccess.REFERENCE.load(),
                  TypeCasting.to(convertedType));
          stackManipulation =
              new StackManipulation.Compound(
                  stackManipulation,
                  typeConversionsFactory
                      .createSetterConversions(readParameter)
                      .convert(TypeDescriptor.of(parameter.getParameterizedType())));
        }
        stackManipulation =
            new StackManipulation.Compound(
                stackManipulation, afterPushingParameters(), MethodReturn.REFERENCE);

        StackManipulation.Size size = stackManipulation.apply(methodVisitor, implementationContext);
        return new ByteCodeAppender.Size(size.getMaximalSize(), numLocals);
      };
    }

    protected StackManipulation beforePushingParameters() {
      return new StackManipulation.Compound();
    }

    protected StackManipulation afterPushingParameters() {
      return new StackManipulation.Compound();
    }
  }
}
