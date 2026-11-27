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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.asm.AsmVisitorWrapper;
import net.bytebuddy.description.method.MethodDescription.ForLoadedMethod;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.ClassFileLocator;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.dynamic.scaffold.InstrumentedType;
import net.bytebuddy.implementation.FixedValue;
import net.bytebuddy.implementation.Implementation;
import net.bytebuddy.implementation.bytecode.ByteCodeAppender;
import net.bytebuddy.implementation.bytecode.ByteCodeAppender.Size;
import net.bytebuddy.implementation.bytecode.Duplication;
import net.bytebuddy.implementation.bytecode.Removal;
import net.bytebuddy.implementation.bytecode.StackManipulation;
import net.bytebuddy.implementation.bytecode.TypeCreation;
import net.bytebuddy.implementation.bytecode.assign.TypeCasting;
import net.bytebuddy.implementation.bytecode.collection.ArrayAccess;
import net.bytebuddy.implementation.bytecode.constant.IntegerConstant;
import net.bytebuddy.implementation.bytecode.member.MethodInvocation;
import net.bytebuddy.implementation.bytecode.member.MethodReturn;
import net.bytebuddy.implementation.bytecode.member.MethodVariableAccess;
import net.bytebuddy.jar.asm.ClassWriter;
import net.bytebuddy.matcher.ElementMatchers;
import net.bytebuddy.pool.TypePool;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.schemas.FieldValueGetter;
import org.apache.beam.sdk.schemas.FieldValueHaver;
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
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

/** A set of utilities to generate getter and setter classes for JavaBean objects. */
@SuppressWarnings({"rawtypes"})
public class JavaBeanUtils {

  private static final String X_WITH_NULL_METHOD_ERROR_FMT =
      "a %s FieldValueTypeInformation object has a null method field";
  public static final String GETTER_WITH_NULL_METHOD_ERROR =
      String.format(X_WITH_NULL_METHOD_ERROR_FMT, "getter");
  public static final String SETTER_WITH_NULL_METHOD_ERROR =
      String.format(X_WITH_NULL_METHOD_ERROR_FMT, "setter");

  /** Create a {@link Schema} for a Java Bean class. */
  public static Schema schemaFromJavaBeanClass(
      TypeDescriptor<?> typeDescriptor, FieldValueTypeSupplier fieldValueTypeSupplier) {
    return StaticSchemaInference.schemaFromClass(typeDescriptor, fieldValueTypeSupplier);
  }

  private static final String CONSTRUCTOR_HELP_STRING =
      "In order to infer a Schema from a Java Bean, it must have a constructor annotated with"
          + " @SchemaCreate, or it must have a compatible setter for every getter used as a Schema"
          + " field.";

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

    for (FieldValueTypeInformation type : getters) {
      FieldValueTypeInformation setterType = setterMap.get(type.getName());
      Method m =
          Preconditions.checkArgumentNotNull(type.getMethod(), GETTER_WITH_NULL_METHOD_ERROR);
      if (setterType == null) {
        throw new RuntimeException(
            String.format(
                "Java Bean '%s' contains a getter for field '%s', but does not contain a matching"
                    + " setter. %s",
                m.getDeclaringClass(), type.getName(), CONSTRUCTOR_HELP_STRING));
      }
      if (!type.getType().equals(setterType.getType())) {
        throw new RuntimeException(
            String.format(
                "Java Bean '%s' contains a setter for field '%s' that has a mismatching type. %s",
                m.getDeclaringClass(), type.getName(), CONSTRUCTOR_HELP_STRING));
      }
      if (!type.isNullable() == setterType.isNullable()) {
        throw new RuntimeException(
            String.format(
                "Java Bean '%s' contains a setter for field '%s' that has a mismatching nullable"
                    + " attribute. %s",
                m.getDeclaringClass(), type.getName(), CONSTRUCTOR_HELP_STRING));
      }
    }
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
  private static final Map<TypeDescriptorWithSchema<?>, List<FieldValueGetter<@NonNull ?, ?>>>
      CACHED_GETTERS = Maps.newConcurrentMap();

  /**
   * Return the list of {@link FieldValueGetter}s for a Java Bean class
   *
   * <p>The returned list is ordered by the order of fields in the schema.
   */
  public static <T> List<FieldValueGetter<@NonNull T, Object>> getGetters(
      TypeDescriptor<T> typeDescriptor,
      Schema schema,
      FieldValueTypeSupplier fieldValueTypeSupplier,
      TypeConversionsFactory typeConversionsFactory) {
    return (List)
        CACHED_GETTERS.computeIfAbsent(
            TypeDescriptorWithSchema.create(typeDescriptor, schema),
            c -> {
              List<FieldValueTypeInformation> types =
                  fieldValueTypeSupplier.get(typeDescriptor, schema);
              return types.stream()
                  .map(t -> JavaBeanUtils.createGetter(t, typeConversionsFactory))
                  .collect(Collectors.toList());
            });
  }

  public static <ObjectT extends @NonNull Object, ValueT>
      FieldValueGetter<ObjectT, ValueT> createGetter(
          FieldValueTypeInformation typeInformation,
          TypeConversionsFactory typeConversionsFactory) {
    final Method m =
        Preconditions.checkArgumentNotNull(
            typeInformation.getMethod(), GETTER_WITH_NULL_METHOD_ERROR);
    DynamicType.Builder<FieldValueGetter<ObjectT, ValueT>> builder =
        ByteBuddyUtils.subclassGetterInterface(
            BYTE_BUDDY,
            m.getDeclaringClass(),
            typeConversionsFactory.createTypeConversion(false).convert(typeInformation.getType()));
    builder = implementGetterMethods(builder, typeInformation, typeConversionsFactory);
    try {
      return builder
          .visit(new AsmVisitorWrapper.ForDeclaredMethods().writerFlags(ClassWriter.COMPUTE_FRAMES))
          .make()
          .load(
              ReflectHelpers.findClassLoader(m.getDeclaringClass().getClassLoader()),
              getClassLoadingStrategy(m.getDeclaringClass()))
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

  private static <ObjectT extends @NonNull Object, ValueT>
      DynamicType.Builder<FieldValueGetter<ObjectT, ValueT>> implementGetterMethods(
          DynamicType.Builder<FieldValueGetter<ObjectT, ValueT>> builder,
          FieldValueTypeInformation typeInformation,
          TypeConversionsFactory typeConversionsFactory) {
    builder =
        builder
            .method(ElementMatchers.named("name"))
            .intercept(FixedValue.reference(typeInformation.getName()))
            .method(ElementMatchers.named("get"))
            .intercept(new InvokeGetterInstruction(typeInformation, typeConversionsFactory));
    if (typeConversionsFactory.getClass() == ByteBuddyUtils.DefaultTypeConversionsFactory.class
        && ByteBuddyUtils.inferredLogicalTypeFor(typeInformation.getRawType()) != null) {
      builder =
          builder
              .implement(ByteBuddyUtils.DirectLogicalTypeGetter.class)
              .method(ElementMatchers.named("getRaw"))
              .intercept(new InvokeRawGetterInstruction(typeInformation));
    }
    return builder;
  }

  // Invokes the JavaBean getter directly without logical-type toBaseType conversion.
  private static class InvokeRawGetterInstruction implements Implementation {
    private final FieldValueTypeInformation typeInformation;

    InvokeRawGetterInstruction(FieldValueTypeInformation typeInformation) {
      this.typeInformation = typeInformation;
    }

    @Override
    public InstrumentedType prepare(InstrumentedType instrumentedType) {
      return instrumentedType;
    }

    @Override
    public ByteCodeAppender appender(final Target implementationTarget) {
      return (methodVisitor, implementationContext, instrumentedMethod) -> {
        int numLocals = 1 + instrumentedMethod.getParameters().size();
        Method method =
            Preconditions.checkStateNotNull(
                typeInformation.getMethod(), GETTER_WITH_NULL_METHOD_ERROR);
        StackManipulation cast =
            typeInformation.getRawType().isAssignableFrom(method.getReturnType())
                ? StackManipulation.Trivial.INSTANCE
                : TypeCasting.to(TypeDescription.ForLoadedType.of(typeInformation.getRawType()));
        StackManipulation stackManipulation =
            new StackManipulation.Compound(
                MethodVariableAccess.REFERENCE.loadFrom(1),
                MethodInvocation.invoke(new ForLoadedMethod(method)),
                cast,
                MethodReturn.REFERENCE);
        StackManipulation.Size size = stackManipulation.apply(methodVisitor, implementationContext);
        return new Size(size.getMaximalSize(), numLocals);
      };
    }
  }

  // The list of setters for a class is cached, so we only create the classes the first time
  // getSetters is called.
  private static final Map<TypeDescriptorWithSchema<?>, List<FieldValueSetter>> CACHED_SETTERS =
      Maps.newConcurrentMap();

  /**
   * Return the list of {@link FieldValueSetter}s for a Java Bean class
   *
   * <p>The returned list is ordered by the order of fields in the schema.
   */
  public static List<FieldValueSetter> getSetters(
      TypeDescriptor<?> typeDescriptor,
      Schema schema,
      FieldValueTypeSupplier fieldValueTypeSupplier,
      TypeConversionsFactory typeConversionsFactory) {
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

  public static <ObjectT, ValueT> FieldValueSetter<ObjectT, ValueT> createSetter(
      FieldValueTypeInformation typeInformation, TypeConversionsFactory typeConversionsFactory) {
    final Method m =
        Preconditions.checkArgumentNotNull(
            typeInformation.getMethod(), SETTER_WITH_NULL_METHOD_ERROR);
    DynamicType.Builder<FieldValueSetter<ObjectT, ValueT>> builder =
        ByteBuddyUtils.subclassSetterInterface(
            BYTE_BUDDY,
            m.getDeclaringClass(),
            typeConversionsFactory.createTypeConversion(false).convert(typeInformation.getType()));
    builder = implementSetterMethods(builder, typeInformation, typeConversionsFactory);
    try {
      return builder
          .visit(new AsmVisitorWrapper.ForDeclaredMethods().writerFlags(ClassWriter.COMPUTE_FRAMES))
          .make()
          .load(
              ReflectHelpers.findClassLoader(m.getDeclaringClass().getClassLoader()),
              getClassLoadingStrategy(m.getDeclaringClass()))
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

  private static <ObjectT, ValueT>
      DynamicType.Builder<FieldValueSetter<ObjectT, ValueT>> implementSetterMethods(
          DynamicType.Builder<FieldValueSetter<ObjectT, ValueT>> builder,
          FieldValueTypeInformation fieldValueTypeInformation,
          TypeConversionsFactory typeConversionsFactory) {
    return builder
        .method(ElementMatchers.named("name"))
        .intercept(FixedValue.reference(fieldValueTypeInformation.getName()))
        .method(ElementMatchers.named("set"))
        .intercept(new InvokeSetterInstruction(fieldValueTypeInformation, typeConversionsFactory));
  }

  public static <ObjectT> FieldValueHaver<ObjectT> createHaver(
      Class<ObjectT> clazz, Method hasMethod) {
    DynamicType.Builder<FieldValueHaver<ObjectT>> builder =
        ByteBuddyUtils.subclassHaverInterface(BYTE_BUDDY, clazz);
    builder = implementHaverMethods(builder, hasMethod);
    try {
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
        | InvocationTargetException
        | NoSuchMethodException e) {
      throw new RuntimeException("Unable to generate a have for hasMethod '" + hasMethod + "'", e);
    }
  }

  private static <ObjectT> DynamicType.Builder<FieldValueHaver<ObjectT>> implementHaverMethods(
      DynamicType.Builder<FieldValueHaver<ObjectT>> builder, Method hasMethod) {
    return builder
        .method(ElementMatchers.named("name"))
        .intercept(FixedValue.reference(hasMethod.getName()))
        .method(ElementMatchers.named("has"))
        .intercept(new InvokeHaverInstruction(hasMethod));
  }

  // The list of constructors for a class is cached, so we only create the classes the first time
  // getConstructor is called.
  public static final Map<TypeDescriptorWithSchema<?>, SchemaUserTypeCreator> CACHED_CREATORS =
      Maps.newConcurrentMap();

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
          "Unable to generate a creator for class " + clazz + " with schema " + schema);
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

  // Above this field count, setter-based creator generation falls back to SetterBasedCreatorFactory
  // to stay well below HotSpot C2's 8KB HugeMethodLimit even when fields require type conversions.
  private static final int MAX_INLINED_SETTER_CREATOR_FIELDS = 128;
  private static final int MAX_SETTER_CREATORS_PER_CLASS = 32;

  private static final class SetterCreatorKey {
    final TypeDescriptorWithSchema<?> typeWithSchema;
    final Class<?> supplierClass;
    final Class<?> factoryClass;
    final int hash;

    SetterCreatorKey(
        TypeDescriptorWithSchema<?> typeWithSchema, Class<?> supplierClass, Class<?> factoryClass) {
      this.typeWithSchema = typeWithSchema;
      this.supplierClass = supplierClass;
      this.factoryClass = factoryClass;
      int h = typeWithSchema.hashCode();
      h = 31 * h + supplierClass.hashCode();
      h = 31 * h + factoryClass.hashCode();
      this.hash = h;
    }

    @Override
    public int hashCode() {
      return hash;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof SetterCreatorKey)) {
        return false;
      }
      SetterCreatorKey that = (SetterCreatorKey) o;
      return hash == that.hash
          && supplierClass == that.supplierClass
          && factoryClass == that.factoryClass
          && typeWithSchema.equals(that.typeWithSchema);
    }
  }

  // Dedicated ClassValue-anchored cache for generated setter creators (separate from
  // CACHED_CREATORS used by constructor/static creators) so user ClassLoaders are never pinned.
  private static final ClassValue<ConcurrentHashMap<SetterCreatorKey, SchemaUserTypeCreator>>
      SETTER_CREATOR_CACHE =
          new ClassValue<ConcurrentHashMap<SetterCreatorKey, SchemaUserTypeCreator>>() {
            @Override
            protected ConcurrentHashMap<SetterCreatorKey, SchemaUserTypeCreator> computeValue(
                Class<?> type) {
              return new ConcurrentHashMap<>();
            }
          };

  private static boolean isClassAccessibleFromPackage(Class<?> clazz, @Nullable Package fromPkg) {
    if (clazz.isAnonymousClass() || clazz.isLocalClass() || clazz.isSynthetic()) {
      return false;
    }
    if (clazz.getCanonicalName() == null) {
      return false;
    }
    if (clazz.getEnclosingClass() != null
        && !clazz.isInterface()
        && !clazz.isEnum()
        && !Modifier.isStatic(clazz.getModifiers())) {
      return false;
    }
    for (Class<?> c = clazz; c != null; c = c.getEnclosingClass()) {
      int m = c.getModifiers();
      if (Modifier.isPrivate(m) || Modifier.isProtected(m)) {
        return false;
      }
      if (!Modifier.isPublic(m)) {
        Package cPkg = c.getPackage();
        if (fromPkg == null || cPkg == null || !fromPkg.getName().equals(cPkg.getName())) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Returns a cached ByteBuddy-generated {@link SchemaUserTypeCreator} that constructs {@code
   * typeDescriptor} via its accessible no-arg constructor and invokes each setter directly, or
   * {@code null} if a direct bytecode creator cannot be generated (e.g., inaccessible
   * class/constructor/setter, default package, or excessive field count).
   */
  @Internal
  public static @Nullable SchemaUserTypeCreator getSetterCreator(
      TypeDescriptor<?> typeDescriptor,
      Schema schema,
      FieldValueTypeSupplier setterTypeSupplier,
      TypeConversionsFactory typeConversionsFactory) {
    Class<?> clazz = typeDescriptor.getRawType();
    if (schema.getFieldCount() > MAX_INLINED_SETTER_CREATOR_FIELDS) {
      return null;
    }
    Package pkg = clazz.getPackage();
    if (pkg == null) {
      return null;
    }
    String pkgName = pkg.getName();
    if (pkgName.startsWith("java.")
        || pkgName.startsWith("javax.")
        || pkgName.startsWith("sun.")
        || pkgName.startsWith("jdk.")) {
      return null;
    }
    if (Modifier.isAbstract(clazz.getModifiers())
        || clazz.isInterface()
        || !isClassAccessibleFromPackage(clazz, pkg)) {
      return null;
    }
    Constructor<?> defaultCtor;
    try {
      defaultCtor = clazz.getDeclaredConstructor();
    } catch (NoSuchMethodException e) {
      return null;
    }
    int ctorMods = defaultCtor.getModifiers();
    if (Modifier.isPrivate(ctorMods) || Modifier.isProtected(ctorMods)) {
      return null;
    }
    List<FieldValueTypeInformation> setterTypes = setterTypeSupplier.get(typeDescriptor, schema);
    if (setterTypes.size() != schema.getFieldCount()) {
      return null;
    }
    for (FieldValueTypeInformation info : setterTypes) {
      Method m = info.getMethod();
      if (m == null) {
        return null;
      }
      int mMods = m.getModifiers();
      if (Modifier.isPrivate(mMods) || Modifier.isProtected(mMods) || Modifier.isStatic(mMods)) {
        return null;
      }
      Class<?> declClass = m.getDeclaringClass();
      if (!isClassAccessibleFromPackage(declClass, pkg)) {
        return null;
      }
      if (!Modifier.isPublic(mMods)) {
        Package declPkg = declClass.getPackage();
        if (declPkg == null || !pkgName.equals(declPkg.getName())) {
          return null;
        }
      }
      Class<?>[] paramTypes = m.getParameterTypes();
      if (paramTypes.length != 1) {
        return null;
      }
      Class<?> pType = paramTypes[0];
      while (pType.isArray()) {
        pType = pType.getComponentType();
      }
      if (!pType.isPrimitive() && !isClassAccessibleFromPackage(pType, pkg)) {
        return null;
      }
    }
    boolean canCache =
        typeConversionsFactory.getClass() == ByteBuddyUtils.DefaultTypeConversionsFactory.class;
    if (!canCache) {
      return tryCreateSetterCreator(clazz, setterTypes, typeConversionsFactory);
    }
    ConcurrentHashMap<SetterCreatorKey, SchemaUserTypeCreator> perClassCache =
        SETTER_CREATOR_CACHE.get(clazz);
    SetterCreatorKey key =
        new SetterCreatorKey(
            TypeDescriptorWithSchema.create(typeDescriptor, schema),
            setterTypeSupplier.getClass(),
            typeConversionsFactory.getClass());
    SchemaUserTypeCreator existing = perClassCache.get(key);
    if (existing != null) {
      return existing;
    }
    if (perClassCache.size() >= MAX_SETTER_CREATORS_PER_CLASS) {
      return null;
    }
    SchemaUserTypeCreator created =
        tryCreateSetterCreator(clazz, setterTypes, typeConversionsFactory);
    if (created == null) {
      return null;
    }
    SchemaUserTypeCreator prev = perClassCache.putIfAbsent(key, created);
    return prev != null ? prev : created;
  }

  private static <T> @Nullable SchemaUserTypeCreator tryCreateSetterCreator(
      Class<T> clazz,
      List<FieldValueTypeInformation> setterTypes,
      TypeConversionsFactory typeConversionsFactory) {
    try {
      DynamicType.Builder<SchemaUserTypeCreator> builder =
          BYTE_BUDDY
              .with(new InjectPackageStrategy(clazz))
              .subclass(SchemaUserTypeCreator.class)
              .method(ElementMatchers.named("create"))
              .intercept(
                  new SetMethodCreateInstruction(setterTypes, clazz, typeConversionsFactory));
      ClassLoader targetLoader = ReflectHelpers.findClassLoader(clazz.getClassLoader());
      TypePool typePool =
          TypePool.Default.of(
              new ClassFileLocator.Compound(
                  ClassFileLocator.ForClassLoader.of(targetLoader),
                  ClassFileLocator.ForClassLoader.of(JavaBeanUtils.class.getClassLoader())));
      return builder
          .visit(new AsmVisitorWrapper.ForDeclaredMethods().writerFlags(ClassWriter.COMPUTE_FRAMES))
          .make(typePool)
          .load(targetLoader, getClassLoadingStrategy(clazz))
          .getLoaded()
          .getDeclaredConstructor()
          .newInstance();
    } catch (Exception | LinkageError e) {
      // Fall back cleanly to SetterBasedCreatorFactory on sealed packages or classloader limits.
      return null;
    }
  }

  private static class SetMethodCreateInstruction implements Implementation {
    private final List<FieldValueTypeInformation> setterTypes;
    private final Class<?> beanClass;
    private final TypeConversionsFactory typeConversionsFactory;

    SetMethodCreateInstruction(
        List<FieldValueTypeInformation> setterTypes,
        Class<?> beanClass,
        TypeConversionsFactory typeConversionsFactory) {
      this.setterTypes = setterTypes;
      this.beanClass = beanClass;
      this.typeConversionsFactory = typeConversionsFactory;
    }

    @Override
    public InstrumentedType prepare(InstrumentedType instrumentedType) {
      return instrumentedType;
    }

    @Override
    public ByteCodeAppender appender(final Target implementationTarget) {
      return (methodVisitor, implementationContext, instrumentedMethod) -> {
        int baseLocals = 1 + instrumentedMethod.getParameters().size();
        int tempLocal = baseLocals;
        boolean usedTemp = false;

        TypeDescription.ForLoadedType loadedType = new TypeDescription.ForLoadedType(beanClass);
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

        TypeConversion<Type> convertType = typeConversionsFactory.createTypeConversion(true);
        for (int i = 0; i < setterTypes.size(); i++) {
          FieldValueTypeInformation typeInfo = setterTypes.get(i);
          Method setter =
              Preconditions.checkStateNotNull(typeInfo.getMethod(), SETTER_WITH_NULL_METHOD_ERROR);
          TypeDescription.ForLoadedType convertedType =
              new TypeDescription.ForLoadedType((Class<?>) convertType.convert(typeInfo.getType()));
          StackManipulation readParam =
              new StackManipulation.Compound(
                  MethodVariableAccess.REFERENCE.loadFrom(1),
                  IntegerConstant.forValue(i),
                  ArrayAccess.REFERENCE.load(),
                  TypeCasting.to(convertedType));
          StackManipulation convertedValue;
          if (typeInfo.getRawType().isPrimitive()
              || ByteBuddyUtils.isIdentityConversion(
                  s ->
                      typeConversionsFactory
                          .createSetterConversions(s)
                          .convert(typeInfo.getType()))) {
            convertedValue =
                typeConversionsFactory
                    .createSetterConversions(readParam)
                    .convert(typeInfo.getType());
          } else {
            // Spill param to reusable local once to avoid double aaload/checkcast in null check.
            usedTemp = true;
            StackManipulation storeParam =
                new StackManipulation.Compound(
                    readParam, MethodVariableAccess.REFERENCE.storeAt(tempLocal));
            StackManipulation loadParam = MethodVariableAccess.REFERENCE.loadFrom(tempLocal);
            convertedValue =
                new StackManipulation.Compound(
                    storeParam,
                    typeConversionsFactory
                        .createSetterConversions(loadParam)
                        .convert(typeInfo.getType()));
          }
          StackManipulation invokeSetter = MethodInvocation.invoke(new ForLoadedMethod(setter));
          Class<?> returnType = setter.getReturnType();
          StackManipulation discardReturn =
              returnType.equals(Void.TYPE)
                  ? StackManipulation.Trivial.INSTANCE
                  : Removal.of(new TypeDescription.ForLoadedType(returnType));
          stackManipulation =
              new StackManipulation.Compound(
                  stackManipulation,
                  Duplication.SINGLE,
                  convertedValue,
                  invokeSetter,
                  discardReturn);
        }
        stackManipulation =
            new StackManipulation.Compound(stackManipulation, MethodReturn.REFERENCE);
        StackManipulation.Size size = stackManipulation.apply(methodVisitor, implementationContext);
        return new Size(size.getMaximalSize(), usedTemp ? baseLocals + 1 : baseLocals);
      };
    }
  }

  public static <T, K extends Comparable<? super K>> Comparator<T> comparingNullFirst(
      Function<? super T, ? extends @Nullable K> keyExtractor) {
    return Comparator.comparing(keyExtractor, Comparator.nullsFirst(Comparator.naturalOrder()));
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
        StackManipulation cast =
            typeInformation
                    .getRawType()
                    .isAssignableFrom(
                        Preconditions.checkStateNotNull(typeInformation.getMethod())
                            .getReturnType())
                ? StackManipulation.Trivial.INSTANCE
                : TypeCasting.to(TypeDescription.ForLoadedType.of(typeInformation.getRawType()));

        // StackManipulation that reads the value from the getter.
        Method method =
            Preconditions.checkStateNotNull(
                typeInformation.getMethod(), GETTER_WITH_NULL_METHOD_ERROR);
        StackManipulation readValue =
            new StackManipulation.Compound(
                // Method param is offset 1 (offset 0 is the this parameter).
                MethodVariableAccess.REFERENCE.loadFrom(1),
                // Invoke the getter
                MethodInvocation.invoke(new ForLoadedMethod(method)),
                cast);

        StackManipulation stackManipulation;
        if (!method.getReturnType().isPrimitive()
            && !ByteBuddyUtils.isIdentityConversion(
                s ->
                    typeConversionsFactory
                        .createGetterConversions(s)
                        .convert(typeInformation.getType()))) {
          // Spill non-primitive getter result once so null checks don't invoke the getter twice.
          int tempLocal = numLocals;
          numLocals++;
          StackManipulation loadTemp = MethodVariableAccess.REFERENCE.loadFrom(tempLocal);
          stackManipulation =
              new StackManipulation.Compound(
                  readValue,
                  MethodVariableAccess.REFERENCE.storeAt(tempLocal),
                  typeConversionsFactory
                      .createGetterConversions(loadTemp)
                      .convert(typeInformation.getType()),
                  MethodReturn.REFERENCE);
        } else {
          stackManipulation =
              new StackManipulation.Compound(
                  typeConversionsFactory
                      .createGetterConversions(readValue)
                      .convert(typeInformation.getType()),
                  MethodReturn.REFERENCE);
        }

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

        Method method =
            Preconditions.checkStateNotNull(
                fieldValueTypeInformation.getMethod(), SETTER_WITH_NULL_METHOD_ERROR);
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
          // Discard return value (handles both 1-slot and 2-slot long/double fluent setters).
          stackManipulation =
              new StackManipulation.Compound(
                  stackManipulation,
                  Removal.of(new TypeDescription.ForLoadedType(method.getReturnType())));
        }
        stackManipulation = new StackManipulation.Compound(stackManipulation, MethodReturn.VOID);

        StackManipulation.Size size = stackManipulation.apply(methodVisitor, implementationContext);
        return new Size(size.getMaximalSize(), numLocals);
      };
    }
  }

  // Implements a method to check a presence on an object.
  private static class InvokeHaverInstruction implements Implementation {
    private final Method hasMethod;

    public InvokeHaverInstruction(Method hasMethod) {
      this.hasMethod = hasMethod;
    }

    @Override
    public ByteCodeAppender appender(Target implementationTarget) {
      return (methodVisitor, implementationContext, instrumentedMethod) -> {
        // this + method parameters.
        int numLocals = 1 + instrumentedMethod.getParameters().size();
        StackManipulation.Size size =
            new StackManipulation.Compound(
                    // Read the first argument
                    MethodVariableAccess.REFERENCE.loadFrom(1),
                    // Call hasMethod
                    MethodInvocation.invoke(new ForLoadedMethod(hasMethod)),
                    MethodReturn.INTEGER)
                .apply(methodVisitor, implementationContext);
        return new Size(size.getMaximalSize(), numLocals);
      };
    }

    @Override
    public InstrumentedType prepare(InstrumentedType instrumentedType) {
      return instrumentedType;
    }
  }
}
