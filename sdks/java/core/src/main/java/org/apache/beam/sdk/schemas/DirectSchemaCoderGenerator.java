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
package org.apache.beam.sdk.schemas;

import static org.apache.beam.sdk.util.ByteBuddyUtils.getClassLoadingStrategy;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.asm.AsmVisitorWrapper;
import net.bytebuddy.description.modifier.Visibility;
import net.bytebuddy.dynamic.ClassFileLocator;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.dynamic.scaffold.InstrumentedType;
import net.bytebuddy.implementation.Implementation;
import net.bytebuddy.implementation.bytecode.ByteCodeAppender;
import net.bytebuddy.jar.asm.ClassWriter;
import net.bytebuddy.jar.asm.Label;
import net.bytebuddy.jar.asm.MethodVisitor;
import net.bytebuddy.jar.asm.Opcodes;
import net.bytebuddy.jar.asm.Type;
import net.bytebuddy.matcher.ElementMatchers;
import net.bytebuddy.pool.TypePool;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.DoubleCoder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.RowCoderGenerator;
import org.apache.beam.sdk.coders.RowWireCodec;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.LogicalType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.schemas.logicaltypes.Date;
import org.apache.beam.sdk.schemas.logicaltypes.DateTime;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.apache.beam.sdk.schemas.logicaltypes.MicrosInstant;
import org.apache.beam.sdk.schemas.logicaltypes.NanosInstant;
import org.apache.beam.sdk.schemas.logicaltypes.Time;
import org.apache.beam.sdk.schemas.logicaltypes.UuidLogicalType;
import org.apache.beam.sdk.schemas.utils.ByteBuddyUtils;
import org.apache.beam.sdk.schemas.utils.ByteBuddyUtils.InjectPackageStrategy;
import org.apache.beam.sdk.schemas.utils.ReflectUtils;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.util.VarInt;
import org.apache.beam.sdk.util.common.ElementByteSizeObservableIterable;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generates fused ByteBuddy {@link Coder} implementations for {@link JavaFieldSchema} POJOs and
 * {@link JavaBeanSchema} JavaBeans that bypass intermediate {@link Row} allocation, primitive
 * boxing, and logical-type round-trips while emitting the exact {@code beam:coder:row:v1} wire
 * format.
 *
 * <p>For schemas with {@code 1 <= fieldCount <= 64}, emits straight-line single-method encode,
 * decode, and byte-size implementations. For wider schemas ({@code 65 <= fieldCount <= 512}), emits
 * chunked 32-field private helper methods on the same generated coder class so every method stays
 * well below HotSpot C2's 8KB {@code HugeMethodLimit} while still bypassing intermediate {@link
 * Row} allocations. On any unsupported type, annotation, access restriction, or schema count above
 * {@code 512}, {@link #tryGenerate} returns {@code null} and {@link SchemaCoder} falls back cleanly
 * to {@code RowCoderGenerator}. On schema evolution ({@code streamFieldCount != fieldCount}, or an
 * oversized null bitmap on the {@code <= 64} single-method path), the generated coder delegates the
 * remainder of the stream to {@link RowCoderGenerator#decodeAfterFieldCount} or {@link
 * RowCoderGenerator#decodeAfterNullByteLen}; chunked wide decoders handle arbitrary {@code
 * nullByteLen} directly via {@link RowWireCodec#readNullBytes}.
 */
@Internal
@SuppressWarnings({"nullness", "rawtypes"})
public final class DirectSchemaCoderGenerator {
  private static final Logger LOG = LoggerFactory.getLogger(DirectSchemaCoderGenerator.class);
  private static final int MAX_DIRECT_FIELDS = 64;
  // Chunked wide encode patches a 1-byte nullByteLen prefix directly in the 512-byte ScratchPool
  // buffer, which requires ceil(MAX_CHUNKED_DIRECT_FIELDS / 8) <= 127 (i.e. <= 1016 fields).
  private static final int MAX_CHUNKED_DIRECT_FIELDS = 512;
  private static final int DIRECT_CHUNK_SIZE = 32;
  private static final int MAX_CLASSES_PER_TYPE = 32;

  static {
    if (((MAX_CHUNKED_DIRECT_FIELDS + 7) >>> 3) > 127) {
      throw new AssertionError(
          "MAX_CHUNKED_DIRECT_FIELDS exceeds 1-byte nullByteLen in-place header limit");
    }
  }

  private static final String WIRE_CODEC = "org/apache/beam/sdk/coders/RowWireCodec";
  private static final String SCRATCH_POOL = "org/apache/beam/sdk/coders/RowWireCodec$ScratchPool";
  private static final String SCRATCH_POOL_DESC =
      "Lorg/apache/beam/sdk/coders/RowWireCodec$ScratchPool;";
  private static final String REF_SCRATCH_POOL =
      "org/apache/beam/sdk/coders/RowWireCodec$RefScratchPool";
  private static final String REF_SCRATCH_POOL_DESC =
      "Lorg/apache/beam/sdk/coders/RowWireCodec$RefScratchPool;";
  private static final String BASE_TYPE = Type.getInternalName(DirectSchemaCoderBase.class);
  private static final String GEN_TYPE = Type.getInternalName(DirectSchemaCoderGenerator.class);
  private static final String CODER_TYPE = "org/apache/beam/sdk/coders/Coder";
  private static final String OBSERVER_TYPE =
      "org/apache/beam/sdk/util/common/ElementByteSizeObserver";
  private static final String OBSERVABLE_ITERABLE_TYPE =
      "org/apache/beam/sdk/util/common/ElementByteSizeObservableIterable";
  private static final String VARINT_TYPE = "org/apache/beam/sdk/util/VarInt";
  private static final String OUT_DESC = "Ljava/io/OutputStream;";
  private static final String IN_DESC = "Ljava/io/InputStream;";
  private static final String OBSERVER_DESC =
      "Lorg/apache/beam/sdk/util/common/ElementByteSizeObserver;";

  private static final ByteBuddy BYTE_BUDDY = new ByteBuddy();

  // Anchor generated coder classes to targetClass via ClassValue so user ClassLoaders (e.g.
  // Spark/Flink job classloaders) are never pinned by static maps and can unload cleanly.
  private static final ClassValue<
          ConcurrentHashMap<CoderClassKey, Class<? extends DirectSchemaCoderBase>>>
      CLASS_CACHE =
          new ClassValue<
              ConcurrentHashMap<CoderClassKey, Class<? extends DirectSchemaCoderBase>>>() {
            @Override
            protected ConcurrentHashMap<CoderClassKey, Class<? extends DirectSchemaCoderBase>>
                computeValue(Class<?> type) {
              return new ConcurrentHashMap<>();
            }
          };

  private static final IterableCoder<String> LAZY_STRING_ITERABLE_CODER =
      IterableCoder.of(StringUtf8Coder.of());
  private static final IterableCoder<Integer> LAZY_INT32_ITERABLE_CODER =
      IterableCoder.of(VarIntCoder.of());
  private static final IterableCoder<Long> LAZY_INT64_ITERABLE_CODER =
      IterableCoder.of(VarLongCoder.of());
  private static final IterableCoder<Double> LAZY_DOUBLE_ITERABLE_CODER =
      IterableCoder.of(DoubleCoder.of());

  private DirectSchemaCoderGenerator() {}

  /** Shared base class for generated fused POJO/JavaBean coders. */
  @Internal
  public abstract static class DirectSchemaCoderBase<T> extends CustomCoder<T> {
    protected final Schema schema;
    protected final SerializableFunction<Row, T> fromRowFunction;
    protected final Coder<?>[] subCoders;
    // Lazily generated only if schema-evolution decode or verifyDeterministic is actually invoked.
    private transient volatile @Nullable Coder<Row> fallbackRowCoder;

    protected DirectSchemaCoderBase(
        Schema schema, SerializableFunction<Row, T> fromRowFunction, Coder<?>[] subCoders) {
      this.schema = schema;
      this.fromRowFunction = fromRowFunction;
      this.subCoders = subCoders;
    }

    protected final Coder<Row> getFallbackRowCoder() {
      Coder<Row> coder = fallbackRowCoder;
      if (coder == null) {
        coder = RowCoderGenerator.generate(schema);
        fallbackRowCoder = coder;
      }
      return coder;
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      getFallbackRowCoder().verifyDeterministic();
    }

    protected final T decodeEvolved(int streamFieldCount, InputStream in) throws IOException {
      Row row =
          RowCoderGenerator.decodeAfterFieldCount(getFallbackRowCoder(), streamFieldCount, in);
      return fromRowFunction.apply(row);
    }

    protected final T decodeEvolvedAfterNullLen(
        int streamFieldCount, int nullByteLen, InputStream in) throws IOException {
      Row row =
          RowCoderGenerator.decodeAfterNullByteLen(
              getFallbackRowCoder(), streamFieldCount, nullByteLen, in);
      return fromRowFunction.apply(row);
    }

    protected static IOException invalidNullBitmapLength(int nullByteLen) {
      return new CoderException("Invalid null bitset length: " + nullByteLen);
    }

    protected static IOException cannotEncodeNull(String className) {
      return new CoderException("cannot encode a null " + className);
    }

    protected static NullPointerException nullPrimitive(String fieldName) {
      return new NullPointerException("Cannot decode null into primitive field " + fieldName);
    }
  }

  /** Bounds-checked enum lookup by ordinal backed by cached {@link ClassValue}. */
  @Internal
  public static Object enumConstantForOrdinal(Class<?> enumClass, int ordinal) {
    return ByteBuddyUtils.enumConstantForOrdinal(enumClass, ordinal);
  }

  /** Observes byte size for {@code Collection<String>}, delegating lazily if observable. */
  @Internal
  public static long observeStringCollectionByteSize(
      Collection<String> col, ElementByteSizeObserver observer) throws Exception {
    if (col instanceof ElementByteSizeObservableIterable) {
      LAZY_STRING_ITERABLE_CODER.registerByteSizeObserver(col, observer);
      return 0L;
    }
    return RowWireCodec.sizeOfStringCollection(col);
  }

  /** Observes byte size for {@code Collection<Integer>}, delegating lazily if observable. */
  @Internal
  public static long observeInt32CollectionByteSize(
      Collection<Integer> col, ElementByteSizeObserver observer) throws Exception {
    if (col instanceof ElementByteSizeObservableIterable) {
      LAZY_INT32_ITERABLE_CODER.registerByteSizeObserver(col, observer);
      return 0L;
    }
    return RowWireCodec.sizeOfInt32Collection(col);
  }

  /** Observes byte size for {@code Collection<Long>}, delegating lazily if observable. */
  @Internal
  public static long observeInt64CollectionByteSize(
      Collection<Long> col, ElementByteSizeObserver observer) throws Exception {
    if (col instanceof ElementByteSizeObservableIterable) {
      LAZY_INT64_ITERABLE_CODER.registerByteSizeObserver(col, observer);
      return 0L;
    }
    return RowWireCodec.sizeOfInt64Collection(col);
  }

  /** Observes byte size for {@code Collection<Double>}, delegating lazily if observable. */
  @Internal
  public static long observeDoubleCollectionByteSize(
      Collection<Double> col, ElementByteSizeObserver observer) throws Exception {
    if (col instanceof ElementByteSizeObservableIterable) {
      LAZY_DOUBLE_ITERABLE_CODER.registerByteSizeObserver(col, observer);
      return 0L;
    }
    return RowWireCodec.sizeOfDoubleCollection(col);
  }

  /**
   * Observes byte size for {@code Collection<SubT>} with a nested direct coder, delegating lazily
   * via {@link IterableCoder} if the collection implements {@link
   * ElementByteSizeObservableIterable}.
   */
  @Internal
  @SuppressWarnings("unchecked")
  public static void observeElementCollectionByteSize(
      Collection<?> col, Coder elemCoder, ElementByteSizeObserver observer) throws Exception {
    if (col instanceof ElementByteSizeObservableIterable) {
      IterableCoder.of(elemCoder).registerByteSizeObserver(col, observer);
      return;
    }
    observer.update(4L);
    if (col instanceof List && col instanceof java.util.RandomAccess) {
      List<?> list = (List<?>) col;
      for (int i = 0, n = list.size(); i < n; i++) {
        elemCoder.registerByteSizeObserver(list.get(i), observer);
      }
    } else {
      for (Object elem : col) {
        elemCoder.registerByteSizeObserver(elem, observer);
      }
    }
  }

  /**
   * Reads a Joda {@link org.joda.time.DateTime} using the exact {@code beam:coder:row:v1} wire
   * format.
   */
  @Internal
  public static org.joda.time.DateTime readJodaDateTime(InputStream in, byte[] buf)
      throws IOException {
    return new org.joda.time.DateTime(RowWireCodec.readBigEndianLong(in, buf) + Long.MIN_VALUE);
  }

  private enum FieldKind {
    PRIM_BYTE,
    PRIM_SHORT,
    PRIM_INT,
    PRIM_LONG,
    PRIM_FLOAT,
    PRIM_DOUBLE,
    PRIM_BOOL,
    BOX_BYTE,
    BOX_SHORT,
    BOX_INT,
    BOX_LONG,
    BOX_FLOAT,
    BOX_DOUBLE,
    BOX_BOOL,
    STRING,
    BYTES,
    DECIMAL,
    JODA_INSTANT,
    JODA_DATETIME,
    LOCAL_DATE,
    LOCAL_TIME,
    LOCAL_DATETIME,
    UUID_TYPE,
    NANOS_INSTANT,
    MICROS_INSTANT,
    ENUM_ORDINAL,
    LIST_STRING,
    LIST_INT,
    LIST_LONG,
    LIST_DOUBLE,
    LIST_ROW,
    MAP_STRING_LONG,
    MAP_STRING_STRING,
    MAP_STRING_INT,
    MAP_STRING_DOUBLE,
    SUB_ROW;

    boolean isPrimitive() {
      return this == PRIM_BYTE
          || this == PRIM_SHORT
          || this == PRIM_INT
          || this == PRIM_LONG
          || this == PRIM_FLOAT
          || this == PRIM_DOUBLE
          || this == PRIM_BOOL;
    }
  }

  private static final class FieldSpec {
    final int rowIndex;
    final int encPos;
    final String schemaFieldName;
    final boolean nullable;
    final FieldKind kind;
    final Class<?> javaType;
    final @Nullable Field pojoField;
    final @Nullable Method getter;
    final @Nullable Method setter;
    final @Nullable Coder<?> subCoder;

    FieldSpec(
        int rowIndex,
        int encPos,
        String schemaFieldName,
        boolean nullable,
        FieldKind kind,
        Class<?> javaType,
        @Nullable Field pojoField,
        @Nullable Method getter,
        @Nullable Method setter,
        @Nullable Coder<?> subCoder) {
      this.rowIndex = rowIndex;
      this.encPos = encPos;
      this.schemaFieldName = schemaFieldName;
      this.nullable = nullable;
      this.kind = kind;
      this.javaType = javaType;
      this.pojoField = pojoField;
      this.getter = getter;
      this.setter = setter;
      this.subCoder = subCoder;
    }

    boolean isPojo() {
      return pojoField != null;
    }
  }

  private static final class CoderClassKey {
    final Class<?> targetClass;
    final Class<?> providerClass;
    final Schema schema;
    final int[] encodingPosToIndex;
    final boolean staticEncoding;
    final int hash;

    CoderClassKey(
        Class<?> targetClass,
        Class<?> providerClass,
        Schema schema,
        int[] encodingPosToIndex,
        boolean staticEncoding) {
      this.targetClass = targetClass;
      this.providerClass = providerClass;
      this.schema = schema;
      this.encodingPosToIndex = encodingPosToIndex;
      this.staticEncoding = staticEncoding;
      int h = targetClass.hashCode();
      h = 31 * h + providerClass.hashCode();
      h = 31 * h + schema.hashCode();
      h = 31 * h + Arrays.hashCode(encodingPosToIndex);
      h = 31 * h + (staticEncoding ? 1 : 0);
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
      if (!(o instanceof CoderClassKey)) {
        return false;
      }
      CoderClassKey that = (CoderClassKey) o;
      return hash == that.hash
          && targetClass == that.targetClass
          && providerClass == that.providerClass
          && staticEncoding == that.staticEncoding
          && Arrays.equals(encodingPosToIndex, that.encodingPosToIndex)
          && schema.equals(that.schema);
    }
  }

  /**
   * Attempts to generate a direct fused {@link Coder} for {@code typeDescriptor}. Returns {@code
   * null} if the type or schema is not eligible for direct bytecode generation.
   */
  @SuppressWarnings("unchecked")
  static <T> @Nullable Coder<T> tryGenerate(
      Schema schema,
      TypeDescriptor<T> typeDescriptor,
      SerializableFunction<T, Row> toRowFunction,
      SerializableFunction<Row, T> fromRowFunction) {
    if (!(toRowFunction instanceof GetterBasedSchemaProvider.ToRowWithValueGetters)
        || !(fromRowFunction instanceof FromRowUsingCreator)) {
      return null;
    }
    GetterBasedSchemaProvider.ToRowWithValueGetters<?> toRow =
        (GetterBasedSchemaProvider.ToRowWithValueGetters<?>) toRowFunction;
    FromRowUsingCreator<?> fromRow = (FromRowUsingCreator<?>) fromRowFunction;
    GetterBasedSchemaProvider toProvider = toRow.getOuter();
    GetterBasedSchemaProvider fromProvider = fromRow.getSchemaProvider();
    if (toProvider.getClass() != fromProvider.getClass()) {
      return null;
    }
    Class<?> providerClass = toProvider.getClass();
    if (providerClass != JavaFieldSchema.class && providerClass != JavaBeanSchema.class) {
      return null;
    }
    if (!schema.equals(toRow.getSchema())
        || !typeDescriptor.equals(toRow.getTargetType())
        || !typeDescriptor.equals(fromRow.getTypeDescriptor())) {
      return null;
    }
    Set<Class<?>> visited = new HashSet<>();
    return (Coder<T>)
        tryGenerateForProvider(
            schema,
            typeDescriptor,
            toProvider,
            (SerializableFunction<Row, T>) fromRowFunction,
            visited);
  }

  @SuppressWarnings("unchecked")
  private static @Nullable Coder<?> tryGenerateForProvider(
      Schema schema,
      TypeDescriptor<?> typeDescriptor,
      GetterBasedSchemaProvider provider,
      @Nullable SerializableFunction<Row, ?> fromRowFn,
      Set<Class<?>> visited) {
    int fieldCount = schema.getFieldCount();
    if (fieldCount < 1 || fieldCount > MAX_CHUNKED_DIRECT_FIELDS) {
      return null;
    }
    if (schema.getUUID() == null) {
      return null;
    }
    boolean staticEncoding = schema.isStaticEncoding();
    if (staticEncoding && schema.hasNullableFields()) {
      return null;
    }
    Class<?> targetClass = typeDescriptor.getRawType();
    if (!isAccessibleTargetClass(targetClass)) {
      return null;
    }
    // Defer custom @SchemaCreate classes to standard RowCoder delegation.
    if (ReflectUtils.getAnnotatedCreateMethod(targetClass) != null
        || ReflectUtils.getAnnotatedConstructor(targetClass) != null) {
      return null;
    }
    Constructor<?> noArgCtor;
    try {
      noArgCtor = targetClass.getDeclaredConstructor();
    } catch (NoSuchMethodException e) {
      return null;
    }
    int ctorMods = noArgCtor.getModifiers();
    if (Modifier.isPrivate(ctorMods) || Modifier.isProtected(ctorMods)) {
      return null;
    }
    if (!visited.add(targetClass)) {
      return null;
    }
    try {
      int[] encodingPosToRowIndex = RowCoderGenerator.resolveEncodingPosToRowIndex(schema);
      if (encodingPosToRowIndex.length != fieldCount) {
        return null;
      }
      boolean isPojo = provider.getClass() == JavaFieldSchema.class;
      FieldSpec[] specsByEncPos =
          buildFieldSpecs(schema, typeDescriptor, provider, isPojo, encodingPosToRowIndex, visited);
      if (specsByEncPos == null) {
        return null;
      }
      Coder<?>[] subCoders = new Coder<?>[fieldCount];
      for (int pos = 0; pos < fieldCount; pos++) {
        subCoders[pos] = specsByEncPos[pos].subCoder;
      }
      SerializableFunction<Row, ?> actualFromRow =
          fromRowFn != null ? fromRowFn : new FromRowUsingCreator(typeDescriptor, provider);

      CoderClassKey key =
          new CoderClassKey(
              targetClass,
              provider.getClass(),
              schema,
              encodingPosToRowIndex.clone(),
              staticEncoding);
      ConcurrentHashMap<CoderClassKey, Class<? extends DirectSchemaCoderBase>> perTypeCache =
          CLASS_CACHE.get(targetClass);
      Class<? extends DirectSchemaCoderBase> coderClass = perTypeCache.get(key);
      if (coderClass == null) {
        if (perTypeCache.size() >= MAX_CLASSES_PER_TYPE) {
          return null;
        }
        boolean hasNullable = schema.hasNullableFields();
        coderClass =
            perTypeCache.computeIfAbsent(
                key,
                k -> generateCoderClass(targetClass, specsByEncPos, staticEncoding, hasNullable));
      }
      return coderClass
          .getConstructor(Schema.class, SerializableFunction.class, Coder[].class)
          .newInstance(schema, actualFromRow, subCoders);
    } catch (Exception | LinkageError e) {
      LOG.warn("Skipping direct SchemaCoder generation for {}", targetClass.getName(), e);
      return null;
    } finally {
      visited.remove(targetClass);
    }
  }

  /**
   * Verifies that {@code clazz} (and every enclosing class) is accessible from a top-level class
   * generated in {@code fromPkg} (not a nestmate).
   */
  private static boolean isClassAccessibleFrom(Class<?> clazz, @Nullable Package fromPkg) {
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

  private static boolean isAccessibleTargetClass(Class<?> clazz) {
    if (clazz.isPrimitive()
        || clazz.isArray()
        || clazz.isInterface()
        || clazz.isEnum()
        || Modifier.isAbstract(clazz.getModifiers())) {
      return false;
    }
    Package pkg = clazz.getPackage();
    if (pkg == null) {
      return false;
    }
    String pkgName = pkg.getName();
    if (pkgName.startsWith("java.")
        || pkgName.startsWith("javax.")
        || pkgName.startsWith("sun.")
        || pkgName.startsWith("jdk.")) {
      return false;
    }
    return isClassAccessibleFrom(clazz, pkg);
  }

  private static boolean isSamePackageOrPublic(
      Class<?> targetClass, Class<?> declaringClass, int mods) {
    if (Modifier.isPrivate(mods) || Modifier.isStatic(mods) || Modifier.isProtected(mods)) {
      return false;
    }
    Package targetPkg = targetClass.getPackage();
    if (!isClassAccessibleFrom(declaringClass, targetPkg)) {
      return false;
    }
    if (Modifier.isPublic(mods)) {
      return true;
    }
    Package declPkg = declaringClass.getPackage();
    return targetPkg != null && declPkg != null && targetPkg.getName().equals(declPkg.getName());
  }

  private static boolean canUseDirectRowEncoding(@Nullable Schema rowSchema) {
    return rowSchema != null
        && !rowSchema.isEncodingPositionsOverridden()
        && !rowSchema.isStaticEncoding()
        && !RowCoderGenerator.hasEncodingPositionOverride(rowSchema.getUUID());
  }

  private static @Nullable FieldSpec[] buildFieldSpecs(
      Schema schema,
      TypeDescriptor<?> targetType,
      GetterBasedSchemaProvider provider,
      boolean isPojo,
      int[] encodingPosToRowIndex,
      Set<Class<?>> visited) {
    Class<?> targetClass = targetType.getRawType();
    int fieldCount = schema.getFieldCount();
    List<FieldValueTypeInformation> typeInfos =
        isPojo
            ? JavaFieldSchema.JavaFieldTypeSupplier.INSTANCE.get(targetType, schema)
            : JavaBeanSchema.GetterTypeSupplier.INSTANCE.get(targetType, schema);
    if (typeInfos.size() != fieldCount) {
      return null;
    }
    List<FieldValueTypeInformation> setterInfos = null;
    if (!isPojo) {
      setterInfos = JavaBeanSchema.SetterTypeSupplier.INSTANCE.get(targetType, schema);
      if (setterInfos.size() != fieldCount) {
        return null;
      }
    }

    FieldSpec[] specs = new FieldSpec[fieldCount];
    for (int pos = 0; pos < fieldCount; pos++) {
      int rowIndex = encodingPosToRowIndex[pos];
      Schema.Field schemaField = schema.getField(rowIndex);
      FieldType fieldType = schemaField.getType();
      FieldValueTypeInformation info = typeInfos.get(rowIndex);
      if (info == null || !schemaField.getName().equals(info.getName())) {
        return null;
      }
      if (info.getType().hasUnresolvedParameters()) {
        return null;
      }
      Field pojoField = null;
      Method getter = null;
      Method setter = null;
      Class<?> memberClass;
      if (isPojo) {
        pojoField = info.getField();
        if (pojoField == null) {
          return null;
        }
        int mods = pojoField.getModifiers();
        if (Modifier.isFinal(mods)
            || !isSamePackageOrPublic(targetClass, pojoField.getDeclaringClass(), mods)) {
          return null;
        }
        memberClass = pojoField.getType();
      } else {
        getter = info.getMethod();
        FieldValueTypeInformation sInfo = setterInfos.get(rowIndex);
        if (getter == null || sInfo == null || !schemaField.getName().equals(sInfo.getName())) {
          return null;
        }
        setter = sInfo.getMethod();
        if (setter == null || setter.getParameterCount() != 1) {
          return null;
        }
        if (!isSamePackageOrPublic(targetClass, getter.getDeclaringClass(), getter.getModifiers())
            || !isSamePackageOrPublic(
                targetClass, setter.getDeclaringClass(), setter.getModifiers())) {
          return null;
        }
        memberClass = getter.getReturnType();
        if (!memberClass.equals(setter.getParameterTypes()[0])) {
          return null;
        }
      }
      // Also guard generic erasure mismatches (e.g., class Holder<T> where getField returns
      // Object).
      if (!memberClass.equals(info.getRawType())) {
        return null;
      }

      Coder<?> subCoder = null;
      FieldKind kind =
          classifyField(targetClass, fieldType, info.getType(), memberClass, provider, visited);
      if (kind == null) {
        return null;
      }
      if (kind == FieldKind.SUB_ROW) {
        Schema subSchema = fieldType.getRowSchema();
        if (subSchema == null) {
          return null;
        }
        subCoder = tryGenerateForProvider(subSchema, info.getType(), provider, null, visited);
        if (subCoder == null) {
          return null;
        }
      } else if (kind == FieldKind.LIST_ROW) {
        FieldType elemType = fieldType.getCollectionElementType();
        TypeDescriptor<?> elemTypeDesc = ReflectUtils.getIterableComponentType(info.getType());
        if (elemType == null
            || elemType.getNullable()
            || elemType.getRowSchema() == null
            || elemTypeDesc == null) {
          return null;
        }
        subCoder =
            tryGenerateForProvider(elemType.getRowSchema(), elemTypeDesc, provider, null, visited);
        if (subCoder == null) {
          return null;
        }
      }

      specs[pos] =
          new FieldSpec(
              rowIndex,
              pos,
              schemaField.getName(),
              fieldType.getNullable(),
              kind,
              memberClass,
              pojoField,
              getter,
              setter,
              subCoder);
    }
    return specs;
  }

  private static @Nullable FieldKind classifyField(
      Class<?> targetClass,
      FieldType fieldType,
      TypeDescriptor<?> javaTypeDesc,
      Class<?> rawClass,
      GetterBasedSchemaProvider provider,
      Set<Class<?>> visited) {
    TypeName tn = fieldType.getTypeName();
    switch (tn) {
      case BYTE:
        if (rawClass == byte.class) {
          return fieldType.getNullable() ? null : FieldKind.PRIM_BYTE;
        }
        return rawClass == Byte.class ? FieldKind.BOX_BYTE : null;
      case INT16:
        if (rawClass == short.class) {
          return fieldType.getNullable() ? null : FieldKind.PRIM_SHORT;
        }
        return rawClass == Short.class ? FieldKind.BOX_SHORT : null;
      case INT32:
        if (rawClass == int.class) {
          return fieldType.getNullable() ? null : FieldKind.PRIM_INT;
        }
        return rawClass == Integer.class ? FieldKind.BOX_INT : null;
      case INT64:
        if (rawClass == long.class) {
          return fieldType.getNullable() ? null : FieldKind.PRIM_LONG;
        }
        return rawClass == Long.class ? FieldKind.BOX_LONG : null;
      case FLOAT:
        if (rawClass == float.class) {
          return fieldType.getNullable() ? null : FieldKind.PRIM_FLOAT;
        }
        return rawClass == Float.class ? FieldKind.BOX_FLOAT : null;
      case DOUBLE:
        if (rawClass == double.class) {
          return fieldType.getNullable() ? null : FieldKind.PRIM_DOUBLE;
        }
        return rawClass == Double.class ? FieldKind.BOX_DOUBLE : null;
      case BOOLEAN:
        if (rawClass == boolean.class) {
          return fieldType.getNullable() ? null : FieldKind.PRIM_BOOL;
        }
        return rawClass == Boolean.class ? FieldKind.BOX_BOOL : null;
      case STRING:
        return rawClass == String.class ? FieldKind.STRING : null;
      case BYTES:
        return rawClass == byte[].class ? FieldKind.BYTES : null;
      case DECIMAL:
        return rawClass == BigDecimal.class ? FieldKind.DECIMAL : null;
      case DATETIME:
        if (rawClass == Instant.class) {
          return FieldKind.JODA_INSTANT;
        }
        if (rawClass == org.joda.time.DateTime.class) {
          return FieldKind.JODA_DATETIME;
        }
        return null;
      case LOGICAL_TYPE:
        LogicalType<?, ?> logical = fieldType.getLogicalType();
        if (logical == null) {
          return null;
        }
        Class<?> lc = logical.getClass();
        if (lc == Date.class && rawClass == LocalDate.class) {
          return FieldKind.LOCAL_DATE;
        }
        if (lc == Time.class && rawClass == LocalTime.class) {
          return FieldKind.LOCAL_TIME;
        }
        if (lc == DateTime.class
            && rawClass == LocalDateTime.class
            && canUseDirectRowEncoding(DateTime.DATETIME_SCHEMA)) {
          return FieldKind.LOCAL_DATETIME;
        }
        if (lc == UuidLogicalType.class
            && rawClass == UUID.class
            && canUseDirectRowEncoding(UuidLogicalType.UUID_SCHEMA)) {
          return FieldKind.UUID_TYPE;
        }
        if (lc == NanosInstant.class
            && rawClass == java.time.Instant.class
            && canUseDirectRowEncoding(logical.getBaseType().getRowSchema())) {
          return FieldKind.NANOS_INSTANT;
        }
        if (lc == MicrosInstant.class
            && rawClass == java.time.Instant.class
            && canUseDirectRowEncoding(logical.getBaseType().getRowSchema())) {
          return FieldKind.MICROS_INSTANT;
        }
        if (lc == EnumerationType.class
            && rawClass.isEnum()
            && isClassAccessibleFrom(rawClass, targetClass.getPackage())) {
          EnumerationType enumType = (EnumerationType) logical;
          Object[] constants = rawClass.getEnumConstants();
          if (constants == null || enumType.getValuesMap().size() != constants.length) {
            return null;
          }
          Map<String, Integer> valuesMap = enumType.getValuesMap();
          for (Object c : constants) {
            Enum<?> e = (Enum<?>) c;
            Integer mapped = valuesMap.get(e.toString());
            if (mapped == null || mapped.intValue() != e.ordinal()) {
              return null;
            }
          }
          return FieldKind.ENUM_ORDINAL;
        }
        return null;
      case ARRAY:
        if (rawClass != List.class && rawClass != ArrayList.class && rawClass != Collection.class) {
          return null;
        }
        FieldType elemType = fieldType.getCollectionElementType();
        if (elemType == null || elemType.getNullable()) {
          return null;
        }
        TypeDescriptor<?> elemJava = ReflectUtils.getIterableComponentType(javaTypeDesc);
        if (elemJava == null) {
          return null;
        }
        Class<?> elemRaw = elemJava.getRawType();
        if (elemType.getTypeName() == TypeName.STRING && elemRaw == String.class) {
          return FieldKind.LIST_STRING;
        }
        if (elemType.getTypeName() == TypeName.INT32 && elemRaw == Integer.class) {
          return FieldKind.LIST_INT;
        }
        if (elemType.getTypeName() == TypeName.INT64 && elemRaw == Long.class) {
          return FieldKind.LIST_LONG;
        }
        if (elemType.getTypeName() == TypeName.DOUBLE && elemRaw == Double.class) {
          return FieldKind.LIST_DOUBLE;
        }
        if (elemType.getTypeName() == TypeName.ROW
            && isAccessibleTargetClass(elemRaw)
            && isClassAccessibleFrom(elemRaw, targetClass.getPackage())) {
          return FieldKind.LIST_ROW;
        }
        return null;
      case MAP:
        if (rawClass != Map.class) {
          return null;
        }
        FieldType kt = fieldType.getMapKeyType();
        FieldType vt = fieldType.getMapValueType();
        if (kt == null || vt == null || kt.getNullable() || vt.getNullable()) {
          return null;
        }
        TypeDescriptor<?> keyJava = ReflectUtils.getMapType(javaTypeDesc, 0);
        TypeDescriptor<?> valJava = ReflectUtils.getMapType(javaTypeDesc, 1);
        if (keyJava == null || valJava == null || keyJava.getRawType() != String.class) {
          return null;
        }
        if (kt.getTypeName() != TypeName.STRING) {
          return null;
        }
        Class<?> vr = valJava.getRawType();
        if (vt.getTypeName() == TypeName.INT64 && vr == Long.class) {
          return FieldKind.MAP_STRING_LONG;
        }
        if (vt.getTypeName() == TypeName.STRING && vr == String.class) {
          return FieldKind.MAP_STRING_STRING;
        }
        if (vt.getTypeName() == TypeName.INT32 && vr == Integer.class) {
          return FieldKind.MAP_STRING_INT;
        }
        if (vt.getTypeName() == TypeName.DOUBLE && vr == Double.class) {
          return FieldKind.MAP_STRING_DOUBLE;
        }
        return null;
      case ROW:
        if (provider == null
            || !isAccessibleTargetClass(rawClass)
            || !isClassAccessibleFrom(rawClass, targetClass.getPackage())
            || visited.contains(rawClass)) {
          return null;
        }
        return FieldKind.SUB_ROW;
      default:
        return null;
    }
  }

  @SuppressWarnings("unchecked")
  private static Class<? extends DirectSchemaCoderBase> generateCoderClass(
      Class<?> targetClass,
      FieldSpec[] specsByEncPos,
      boolean staticEncoding,
      boolean hasNullableFields) {
    if (specsByEncPos.length > MAX_DIRECT_FIELDS) {
      return generateChunkedCoderClass(
          targetClass, specsByEncPos, staticEncoding, hasNullableFields);
    }
    DynamicType.Builder<DirectSchemaCoderBase> builder =
        BYTE_BUDDY
            .with(new InjectPackageStrategy(targetClass))
            .subclass(DirectSchemaCoderBase.class)
            .method(ElementMatchers.named("encode").and(ElementMatchers.takesArguments(2)))
            .intercept(
                new EncodeImpl(targetClass, specsByEncPos, staticEncoding, hasNullableFields))
            .method(ElementMatchers.named("decode").and(ElementMatchers.takesArguments(1)))
            .intercept(
                new DecodeImpl(targetClass, specsByEncPos, staticEncoding, hasNullableFields))
            .method(
                ElementMatchers.named("isRegisterByteSizeObserverCheap")
                    .and(ElementMatchers.takesArguments(1)))
            .intercept(new IsCheapImpl(targetClass, specsByEncPos))
            .method(
                ElementMatchers.named("registerByteSizeObserver")
                    .and(ElementMatchers.takesArguments(2)))
            .intercept(
                new ByteSizeImpl(targetClass, specsByEncPos, staticEncoding, hasNullableFields));

    ClassLoader targetLoader = ReflectHelpers.findClassLoader(targetClass.getClassLoader());
    TypePool typePool =
        TypePool.Default.of(
            new ClassFileLocator.Compound(
                ClassFileLocator.ForClassLoader.of(targetLoader),
                ClassFileLocator.ForClassLoader.of(
                    DirectSchemaCoderGenerator.class.getClassLoader())));
    return builder
        .visit(new AsmVisitorWrapper.ForDeclaredMethods().writerFlags(ClassWriter.COMPUTE_FRAMES))
        .make(typePool)
        .load(targetLoader, getClassLoadingStrategy(targetClass))
        .getLoaded();
  }

  private static boolean kindNeedsIsCheapCheck(FieldKind kind) {
    switch (kind) {
      case STRING:
      case LIST_STRING:
      case LIST_INT:
      case LIST_LONG:
      case LIST_DOUBLE:
      case LIST_ROW:
      case MAP_STRING_LONG:
      case MAP_STRING_STRING:
      case MAP_STRING_INT:
      case MAP_STRING_DOUBLE:
      case SUB_ROW:
        return true;
      default:
        return false;
    }
  }

  private static boolean kindHasDynamicByteSize(FieldKind kind) {
    switch (kind) {
      case PRIM_BYTE:
      case PRIM_BOOL:
      case PRIM_SHORT:
      case PRIM_FLOAT:
      case PRIM_DOUBLE:
        return false;
      default:
        return true;
    }
  }

  private static Class<? extends DirectSchemaCoderBase> generateChunkedCoderClass(
      Class<?> targetClass,
      FieldSpec[] specsByEncPos,
      boolean staticEncoding,
      boolean hasNullableFields) {
    int count = specsByEncPos.length;
    int chunkCount = (count + DIRECT_CHUNK_SIZE - 1) / DIRECT_CHUNK_SIZE;
    boolean isPojo = specsByEncPos[0].isPojo();
    boolean nullableHeader = hasNullableFields && !staticEncoding;
    boolean needsScratch = needsScratchForDecode(specsByEncPos);

    int[] refSlotByPos = new int[count];
    int refCount = 0;
    boolean[] chunkHasRefs = new boolean[chunkCount];
    boolean[] chunkNeedsIsCheap = new boolean[chunkCount];
    boolean[] chunkHasNullable = new boolean[chunkCount];
    boolean[] chunkNeedsByteSize = new boolean[chunkCount];
    boolean anyNeedsIsCheap = false;
    for (int c = 0; c < chunkCount; c++) {
      int start = c * DIRECT_CHUNK_SIZE;
      int end = Math.min(start + DIRECT_CHUNK_SIZE, count);
      for (int pos = start; pos < end; pos++) {
        FieldSpec spec = specsByEncPos[pos];
        if (!spec.kind.isPrimitive()) {
          refSlotByPos[pos] = refCount++;
          chunkHasRefs[c] = true;
        } else {
          refSlotByPos[pos] = -1;
        }
        if (kindNeedsIsCheapCheck(spec.kind)) {
          chunkNeedsIsCheap[c] = true;
          anyNeedsIsCheap = true;
        }
        if (spec.nullable) {
          chunkHasNullable[c] = true;
        }
        if (kindHasDynamicByteSize(spec.kind)) {
          chunkNeedsByteSize[c] = true;
        }
      }
    }

    DynamicType.Builder<DirectSchemaCoderBase> builder =
        BYTE_BUDDY
            .with(new InjectPackageStrategy(targetClass))
            .subclass(DirectSchemaCoderBase.class);

    for (int c = 0; c < chunkCount; c++) {
      int start = c * DIRECT_CHUNK_SIZE;
      int end = Math.min(start + DIRECT_CHUNK_SIZE, count);
      if (nullableHeader && !isPojo) {
        if (chunkHasRefs[c]) {
          builder =
              builder
                  .defineMethod("nullScanBeanC" + c, int.class, Visibility.PRIVATE)
                  .withParameters(targetClass, Object[].class)
                  .intercept(new NullScanChunkImpl(specsByEncPos, start, end, true, refSlotByPos))
                  .defineMethod("encBeanC" + c, int.class, Visibility.PRIVATE)
                  .withParameters(
                      targetClass, Object[].class, OutputStream.class, byte[].class, int.class)
                  .throwing(IOException.class)
                  .intercept(new EncodeChunkImpl(specsByEncPos, start, end, true, refSlotByPos));
        } else {
          builder =
              builder
                  .defineMethod("encC" + c, int.class, Visibility.PRIVATE)
                  .withParameters(targetClass, OutputStream.class, byte[].class, int.class)
                  .throwing(IOException.class)
                  .intercept(new EncodeChunkImpl(specsByEncPos, start, end, false, refSlotByPos));
        }
      } else {
        if (nullableHeader && chunkHasRefs[c]) {
          builder =
              builder
                  .defineMethod("nullScanC" + c, int.class, Visibility.PRIVATE)
                  .withParameters(targetClass)
                  .intercept(new NullScanChunkImpl(specsByEncPos, start, end, false, refSlotByPos));
        }
        builder =
            builder
                .defineMethod("encC" + c, int.class, Visibility.PRIVATE)
                .withParameters(targetClass, OutputStream.class, byte[].class, int.class)
                .throwing(IOException.class)
                .intercept(new EncodeChunkImpl(specsByEncPos, start, end, false, refSlotByPos));
      }

      builder =
          builder
              .defineMethod("decNoNullC" + c, void.class, Visibility.PRIVATE)
              .withParameters(targetClass, InputStream.class, byte[].class)
              .throwing(IOException.class)
              .intercept(new DecodeChunkImpl(specsByEncPos, start, end, false));
      if (nullableHeader) {
        builder =
            builder
                .defineMethod("decNullC" + c, void.class, Visibility.PRIVATE)
                .withParameters(targetClass, InputStream.class, byte[].class, int.class)
                .throwing(IOException.class)
                .intercept(new DecodeChunkImpl(specsByEncPos, start, end, true));
      }

      if (chunkNeedsIsCheap[c]) {
        builder =
            builder
                .defineMethod("isCheapC" + c, boolean.class, Visibility.PRIVATE)
                .withParameters(targetClass)
                .intercept(new IsCheapChunkImpl(specsByEncPos, start, end));
      }
      if (chunkNeedsByteSize[c]) {
        builder =
            builder
                .defineMethod("sizeC" + c, long.class, Visibility.PRIVATE)
                .withParameters(targetClass, ElementByteSizeObserver.class)
                .throwing(Exception.class)
                .intercept(
                    new ByteSizeChunkImpl(
                        specsByEncPos, start, end, !staticEncoding && chunkHasNullable[c]));
      }
    }

    builder =
        builder
            .method(ElementMatchers.named("encode").and(ElementMatchers.takesArguments(2)))
            .intercept(
                new ChunkedEncodeImpl(
                    targetClass,
                    count,
                    chunkCount,
                    staticEncoding,
                    hasNullableFields,
                    isPojo,
                    refCount,
                    chunkHasRefs))
            .method(ElementMatchers.named("decode").and(ElementMatchers.takesArguments(1)))
            .intercept(
                new ChunkedDecodeImpl(
                    targetClass,
                    count,
                    chunkCount,
                    staticEncoding,
                    hasNullableFields,
                    needsScratch))
            .method(
                ElementMatchers.named("isRegisterByteSizeObserverCheap")
                    .and(ElementMatchers.takesArguments(1)))
            .intercept(new ChunkedIsCheapImpl(targetClass, chunkNeedsIsCheap, anyNeedsIsCheap))
            .method(
                ElementMatchers.named("registerByteSizeObserver")
                    .and(ElementMatchers.takesArguments(2)))
            .intercept(
                new ChunkedByteSizeImpl(
                    targetClass,
                    specsByEncPos,
                    chunkCount,
                    staticEncoding,
                    hasNullableFields,
                    chunkNeedsByteSize,
                    chunkHasNullable));

    ClassLoader targetLoader = ReflectHelpers.findClassLoader(targetClass.getClassLoader());
    TypePool typePool =
        TypePool.Default.of(
            new ClassFileLocator.Compound(
                ClassFileLocator.ForClassLoader.of(targetLoader),
                ClassFileLocator.ForClassLoader.of(
                    DirectSchemaCoderGenerator.class.getClassLoader())));
    return builder
        .visit(new AsmVisitorWrapper.ForDeclaredMethods().writerFlags(ClassWriter.COMPUTE_FRAMES))
        .make(typePool)
        .load(targetLoader, getClassLoadingStrategy(targetClass))
        .getLoaded();
  }

  private static void pushInt(MethodVisitor mv, int value) {
    if (value >= -1 && value <= 5) {
      mv.visitInsn(Opcodes.ICONST_0 + value);
    } else if (value >= Byte.MIN_VALUE && value <= Byte.MAX_VALUE) {
      mv.visitIntInsn(Opcodes.BIPUSH, value);
    } else if (value >= Short.MIN_VALUE && value <= Short.MAX_VALUE) {
      mv.visitIntInsn(Opcodes.SIPUSH, value);
    } else {
      mv.visitLdcInsn(value);
    }
  }

  private static void emitReadMember(MethodVisitor mv, int objLocal, FieldSpec spec) {
    mv.visitVarInsn(Opcodes.ALOAD, objLocal);
    if (spec.isPojo()) {
      Field f = spec.pojoField;
      mv.visitFieldInsn(
          Opcodes.GETFIELD,
          Type.getInternalName(f.getDeclaringClass()),
          f.getName(),
          Type.getDescriptor(f.getType()));
    } else {
      Method g = spec.getter;
      boolean isInterface = g.getDeclaringClass().isInterface();
      mv.visitMethodInsn(
          isInterface ? Opcodes.INVOKEINTERFACE : Opcodes.INVOKEVIRTUAL,
          Type.getInternalName(g.getDeclaringClass()),
          g.getName(),
          Type.getMethodDescriptor(g),
          isInterface);
    }
  }

  private static void emitWriteMember(MethodVisitor mv, FieldSpec spec) {
    if (spec.isPojo()) {
      Field f = spec.pojoField;
      mv.visitFieldInsn(
          Opcodes.PUTFIELD,
          Type.getInternalName(f.getDeclaringClass()),
          f.getName(),
          Type.getDescriptor(f.getType()));
    } else {
      Method s = spec.setter;
      boolean isInterface = s.getDeclaringClass().isInterface();
      mv.visitMethodInsn(
          isInterface ? Opcodes.INVOKEINTERFACE : Opcodes.INVOKEVIRTUAL,
          Type.getInternalName(s.getDeclaringClass()),
          s.getName(),
          Type.getMethodDescriptor(s),
          isInterface);
      Class<?> ret = s.getReturnType();
      if (ret == long.class || ret == double.class) {
        mv.visitInsn(Opcodes.POP2);
      } else if (ret != void.class) {
        mv.visitInsn(Opcodes.POP);
      }
    }
  }

  private static void emitThrowNullNonNullable(MethodVisitor mv, int rowIndex) {
    mv.visitVarInsn(Opcodes.ALOAD, 0);
    mv.visitFieldInsn(
        Opcodes.GETFIELD, BASE_TYPE, "schema", "Lorg/apache/beam/sdk/schemas/Schema;");
    pushInt(mv, rowIndex);
    mv.visitMethodInsn(
        Opcodes.INVOKESTATIC,
        WIRE_CODEC,
        "nullNonNullableField",
        "(Lorg/apache/beam/sdk/schemas/Schema;I)Ljava/lang/RuntimeException;",
        false);
    mv.visitInsn(Opcodes.ATHROW);
  }

  private static final String PACK_SUFFIX = "[BI" + OUT_DESC + ")I";

  private static void emitPackCall(
      MethodVisitor mv, String method, String desc, int bufLocal, int posLocal, int outLocal) {
    mv.visitVarInsn(Opcodes.ALOAD, bufLocal);
    mv.visitVarInsn(Opcodes.ILOAD, posLocal);
    mv.visitVarInsn(Opcodes.ALOAD, outLocal);
    mv.visitMethodInsn(Opcodes.INVOKESTATIC, WIRE_CODEC, method, desc, false);
    mv.visitVarInsn(Opcodes.ISTORE, posLocal);
  }

  private static void emitFlush(MethodVisitor mv, int bufLocal, int posLocal, int outLocal) {
    mv.visitVarInsn(Opcodes.ALOAD, bufLocal);
    mv.visitVarInsn(Opcodes.ILOAD, posLocal);
    mv.visitVarInsn(Opcodes.ALOAD, outLocal);
    mv.visitMethodInsn(Opcodes.INVOKESTATIC, WIRE_CODEC, "flush", "([BI" + OUT_DESC + ")I", false);
    mv.visitVarInsn(Opcodes.ISTORE, posLocal);
  }

  private static void emitPackNonNullValue(
      MethodVisitor mv, FieldSpec spec, int valLocal, int outLocal, int bufLocal, int posLocal) {
    switch (spec.kind) {
      case BOX_BYTE:
        mv.visitVarInsn(Opcodes.ALOAD, valLocal);
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Byte", "byteValue", "()B", false);
        emitPackCall(mv, "packByte", "(B" + PACK_SUFFIX, bufLocal, posLocal, outLocal);
        break;
      case BOX_SHORT:
        mv.visitVarInsn(Opcodes.ALOAD, valLocal);
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Short", "shortValue", "()S", false);
        emitPackCall(mv, "packInt16", "(S" + PACK_SUFFIX, bufLocal, posLocal, outLocal);
        break;
      case BOX_INT:
        mv.visitVarInsn(Opcodes.ALOAD, valLocal);
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Integer", "intValue", "()I", false);
        emitPackCall(mv, "packVarInt", "(I" + PACK_SUFFIX, bufLocal, posLocal, outLocal);
        break;
      case BOX_LONG:
        mv.visitVarInsn(Opcodes.ALOAD, valLocal);
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Long", "longValue", "()J", false);
        emitPackCall(mv, "packVarLong", "(J" + PACK_SUFFIX, bufLocal, posLocal, outLocal);
        break;
      case BOX_FLOAT:
        mv.visitVarInsn(Opcodes.ALOAD, valLocal);
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Float", "floatValue", "()F", false);
        emitPackCall(mv, "packFloat", "(F" + PACK_SUFFIX, bufLocal, posLocal, outLocal);
        break;
      case BOX_DOUBLE:
        mv.visitVarInsn(Opcodes.ALOAD, valLocal);
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Double", "doubleValue", "()D", false);
        emitPackCall(mv, "packDouble", "(D" + PACK_SUFFIX, bufLocal, posLocal, outLocal);
        break;
      case BOX_BOOL:
        mv.visitVarInsn(Opcodes.ALOAD, valLocal);
        mv.visitMethodInsn(
            Opcodes.INVOKEVIRTUAL, "java/lang/Boolean", "booleanValue", "()Z", false);
        emitPackCall(mv, "packBoolean", "(Z" + PACK_SUFFIX, bufLocal, posLocal, outLocal);
        break;
      case STRING:
        mv.visitVarInsn(Opcodes.ALOAD, valLocal);
        emitPackCall(
            mv, "packString", "(Ljava/lang/String;" + PACK_SUFFIX, bufLocal, posLocal, outLocal);
        break;
      case BYTES:
        mv.visitVarInsn(Opcodes.ALOAD, valLocal);
        emitPackCall(mv, "packBytes", "([B" + PACK_SUFFIX, bufLocal, posLocal, outLocal);
        break;
      case DECIMAL:
        mv.visitVarInsn(Opcodes.ALOAD, valLocal);
        emitPackCall(
            mv,
            "packDecimal",
            "(Ljava/math/BigDecimal;" + PACK_SUFFIX,
            bufLocal,
            posLocal,
            outLocal);
        break;
      case JODA_INSTANT:
      case JODA_DATETIME:
        mv.visitVarInsn(Opcodes.ALOAD, valLocal);
        emitPackCall(
            mv,
            "packDateTime",
            "(Lorg/joda/time/ReadableInstant;" + PACK_SUFFIX,
            bufLocal,
            posLocal,
            outLocal);
        break;
      case LOCAL_DATE:
        mv.visitVarInsn(Opcodes.ALOAD, valLocal);
        emitPackCall(
            mv,
            "packLocalDate",
            "(Ljava/time/LocalDate;" + PACK_SUFFIX,
            bufLocal,
            posLocal,
            outLocal);
        break;
      case LOCAL_TIME:
        mv.visitVarInsn(Opcodes.ALOAD, valLocal);
        emitPackCall(
            mv,
            "packLocalTime",
            "(Ljava/time/LocalTime;" + PACK_SUFFIX,
            bufLocal,
            posLocal,
            outLocal);
        break;
      case LOCAL_DATETIME:
        mv.visitVarInsn(Opcodes.ALOAD, valLocal);
        emitPackCall(
            mv,
            "packLocalDateTime",
            "(Ljava/time/LocalDateTime;" + PACK_SUFFIX,
            bufLocal,
            posLocal,
            outLocal);
        break;
      case UUID_TYPE:
        mv.visitVarInsn(Opcodes.ALOAD, valLocal);
        emitPackCall(
            mv, "packUuid", "(Ljava/util/UUID;" + PACK_SUFFIX, bufLocal, posLocal, outLocal);
        break;
      case NANOS_INSTANT:
        mv.visitVarInsn(Opcodes.ALOAD, valLocal);
        emitPackCall(
            mv,
            "packNanosInstant",
            "(Ljava/time/Instant;" + PACK_SUFFIX,
            bufLocal,
            posLocal,
            outLocal);
        break;
      case MICROS_INSTANT:
        mv.visitVarInsn(Opcodes.ALOAD, valLocal);
        emitPackCall(
            mv,
            "packMicrosInstant",
            "(Ljava/time/Instant;" + PACK_SUFFIX,
            bufLocal,
            posLocal,
            outLocal);
        break;
      case ENUM_ORDINAL:
        mv.visitVarInsn(Opcodes.ALOAD, valLocal);
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Enum", "ordinal", "()I", false);
        emitPackCall(mv, "packVarInt", "(I" + PACK_SUFFIX, bufLocal, posLocal, outLocal);
        break;
      case LIST_STRING:
        mv.visitVarInsn(Opcodes.ALOAD, valLocal);
        emitPackCall(
            mv,
            "packStringCollection",
            "(Ljava/util/Collection;" + PACK_SUFFIX,
            bufLocal,
            posLocal,
            outLocal);
        break;
      case LIST_INT:
        mv.visitVarInsn(Opcodes.ALOAD, valLocal);
        emitPackCall(
            mv,
            "packInt32Collection",
            "(Ljava/util/Collection;" + PACK_SUFFIX,
            bufLocal,
            posLocal,
            outLocal);
        break;
      case LIST_LONG:
        mv.visitVarInsn(Opcodes.ALOAD, valLocal);
        emitPackCall(
            mv,
            "packInt64Collection",
            "(Ljava/util/Collection;" + PACK_SUFFIX,
            bufLocal,
            posLocal,
            outLocal);
        break;
      case LIST_DOUBLE:
        mv.visitVarInsn(Opcodes.ALOAD, valLocal);
        emitPackCall(
            mv,
            "packDoubleCollection",
            "(Ljava/util/Collection;" + PACK_SUFFIX,
            bufLocal,
            posLocal,
            outLocal);
        break;
      case LIST_ROW:
        // Flush buffered prefix bytes to out before invoking nested element coder so wire order is
        // strictly preserved (ScratchPool already isolates nested buffers).
        emitFlush(mv, bufLocal, posLocal, outLocal);
        mv.visitVarInsn(Opcodes.ALOAD, valLocal);
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitFieldInsn(
            Opcodes.GETFIELD, BASE_TYPE, "subCoders", "[Lorg/apache/beam/sdk/coders/Coder;");
        pushInt(mv, spec.encPos);
        mv.visitInsn(Opcodes.AALOAD);
        mv.visitVarInsn(Opcodes.ALOAD, outLocal);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC,
            WIRE_CODEC,
            "writeElementCollection",
            "(Ljava/util/Collection;Lorg/apache/beam/sdk/coders/Coder;" + OUT_DESC + ")V",
            false);
        break;
      case MAP_STRING_LONG:
        mv.visitVarInsn(Opcodes.ALOAD, valLocal);
        emitPackCall(
            mv,
            "packStringLongMap",
            "(Ljava/util/Map;" + PACK_SUFFIX,
            bufLocal,
            posLocal,
            outLocal);
        break;
      case MAP_STRING_STRING:
        mv.visitVarInsn(Opcodes.ALOAD, valLocal);
        emitPackCall(
            mv,
            "packStringStringMap",
            "(Ljava/util/Map;" + PACK_SUFFIX,
            bufLocal,
            posLocal,
            outLocal);
        break;
      case MAP_STRING_INT:
        mv.visitVarInsn(Opcodes.ALOAD, valLocal);
        emitPackCall(
            mv, "packStringIntMap", "(Ljava/util/Map;" + PACK_SUFFIX, bufLocal, posLocal, outLocal);
        break;
      case MAP_STRING_DOUBLE:
        mv.visitVarInsn(Opcodes.ALOAD, valLocal);
        emitPackCall(
            mv,
            "packStringDoubleMap",
            "(Ljava/util/Map;" + PACK_SUFFIX,
            bufLocal,
            posLocal,
            outLocal);
        break;
      case SUB_ROW:
        // Flush buffered prefix bytes to out before invoking nested sub-coder so wire order is
        // strictly preserved (ScratchPool already isolates nested buffers).
        emitFlush(mv, bufLocal, posLocal, outLocal);
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitFieldInsn(
            Opcodes.GETFIELD, BASE_TYPE, "subCoders", "[Lorg/apache/beam/sdk/coders/Coder;");
        pushInt(mv, spec.encPos);
        mv.visitInsn(Opcodes.AALOAD);
        mv.visitVarInsn(Opcodes.ALOAD, valLocal);
        mv.visitVarInsn(Opcodes.ALOAD, outLocal);
        mv.visitMethodInsn(
            Opcodes.INVOKEVIRTUAL,
            CODER_TYPE,
            "encode",
            "(Ljava/lang/Object;" + OUT_DESC + ")V",
            false);
        break;
      default:
        throw new AssertionError("Unexpected reference kind: " + spec.kind);
    }
  }

  private static void emitPackPrimitive(
      MethodVisitor mv, FieldSpec spec, int objLocal, int outLocal, int bufLocal, int posLocal) {
    emitReadMember(mv, objLocal, spec);
    switch (spec.kind) {
      case PRIM_BYTE:
        emitPackCall(mv, "packByte", "(B" + PACK_SUFFIX, bufLocal, posLocal, outLocal);
        break;
      case PRIM_SHORT:
        emitPackCall(mv, "packInt16", "(S" + PACK_SUFFIX, bufLocal, posLocal, outLocal);
        break;
      case PRIM_INT:
        emitPackCall(mv, "packVarInt", "(I" + PACK_SUFFIX, bufLocal, posLocal, outLocal);
        break;
      case PRIM_LONG:
        emitPackCall(mv, "packVarLong", "(J" + PACK_SUFFIX, bufLocal, posLocal, outLocal);
        break;
      case PRIM_FLOAT:
        emitPackCall(mv, "packFloat", "(F" + PACK_SUFFIX, bufLocal, posLocal, outLocal);
        break;
      case PRIM_DOUBLE:
        emitPackCall(mv, "packDouble", "(D" + PACK_SUFFIX, bufLocal, posLocal, outLocal);
        break;
      case PRIM_BOOL:
        emitPackCall(mv, "packBoolean", "(Z" + PACK_SUFFIX, bufLocal, posLocal, outLocal);
        break;
      default:
        throw new AssertionError("Unexpected primitive kind: " + spec.kind);
    }
  }

  private static void emitReadValueForDecode(
      MethodVisitor mv, FieldSpec spec, int inLocal, int bufLocal) {
    switch (spec.kind) {
      case PRIM_BYTE:
        mv.visitVarInsn(Opcodes.ALOAD, inLocal);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC, WIRE_CODEC, "readByte", "(" + IN_DESC + ")B", false);
        break;
      case PRIM_SHORT:
        mv.visitVarInsn(Opcodes.ALOAD, inLocal);
        mv.visitVarInsn(Opcodes.ALOAD, bufLocal);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC, WIRE_CODEC, "readInt16", "(" + IN_DESC + "[B)S", false);
        break;
      case PRIM_INT:
        mv.visitVarInsn(Opcodes.ALOAD, inLocal);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC, WIRE_CODEC, "readVarInt", "(" + IN_DESC + ")I", false);
        break;
      case PRIM_LONG:
        mv.visitVarInsn(Opcodes.ALOAD, inLocal);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC, WIRE_CODEC, "readVarLong", "(" + IN_DESC + ")J", false);
        break;
      case PRIM_FLOAT:
        mv.visitVarInsn(Opcodes.ALOAD, inLocal);
        mv.visitVarInsn(Opcodes.ALOAD, bufLocal);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC, WIRE_CODEC, "readFloat", "(" + IN_DESC + "[B)F", false);
        break;
      case PRIM_DOUBLE:
        mv.visitVarInsn(Opcodes.ALOAD, inLocal);
        mv.visitVarInsn(Opcodes.ALOAD, bufLocal);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC, WIRE_CODEC, "readDouble", "(" + IN_DESC + "[B)D", false);
        break;
      case PRIM_BOOL:
        mv.visitVarInsn(Opcodes.ALOAD, inLocal);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC, WIRE_CODEC, "readBoolean", "(" + IN_DESC + ")Z", false);
        break;
      case BOX_BYTE:
        mv.visitVarInsn(Opcodes.ALOAD, inLocal);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC, WIRE_CODEC, "readByte", "(" + IN_DESC + ")B", false);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC, "java/lang/Byte", "valueOf", "(B)Ljava/lang/Byte;", false);
        break;
      case BOX_SHORT:
        mv.visitVarInsn(Opcodes.ALOAD, inLocal);
        mv.visitVarInsn(Opcodes.ALOAD, bufLocal);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC, WIRE_CODEC, "readInt16", "(" + IN_DESC + "[B)S", false);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC, "java/lang/Short", "valueOf", "(S)Ljava/lang/Short;", false);
        break;
      case BOX_INT:
        mv.visitVarInsn(Opcodes.ALOAD, inLocal);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC, WIRE_CODEC, "readVarInt", "(" + IN_DESC + ")I", false);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC, "java/lang/Integer", "valueOf", "(I)Ljava/lang/Integer;", false);
        break;
      case BOX_LONG:
        mv.visitVarInsn(Opcodes.ALOAD, inLocal);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC, WIRE_CODEC, "readVarLong", "(" + IN_DESC + ")J", false);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC, "java/lang/Long", "valueOf", "(J)Ljava/lang/Long;", false);
        break;
      case BOX_FLOAT:
        mv.visitVarInsn(Opcodes.ALOAD, inLocal);
        mv.visitVarInsn(Opcodes.ALOAD, bufLocal);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC, WIRE_CODEC, "readFloat", "(" + IN_DESC + "[B)F", false);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC, "java/lang/Float", "valueOf", "(F)Ljava/lang/Float;", false);
        break;
      case BOX_DOUBLE:
        mv.visitVarInsn(Opcodes.ALOAD, inLocal);
        mv.visitVarInsn(Opcodes.ALOAD, bufLocal);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC, WIRE_CODEC, "readDouble", "(" + IN_DESC + "[B)D", false);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC, "java/lang/Double", "valueOf", "(D)Ljava/lang/Double;", false);
        break;
      case BOX_BOOL:
        mv.visitVarInsn(Opcodes.ALOAD, inLocal);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC, WIRE_CODEC, "readBoolean", "(" + IN_DESC + ")Z", false);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC, "java/lang/Boolean", "valueOf", "(Z)Ljava/lang/Boolean;", false);
        break;
      case STRING:
        mv.visitVarInsn(Opcodes.ALOAD, inLocal);
        mv.visitVarInsn(Opcodes.ALOAD, bufLocal);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC,
            WIRE_CODEC,
            "readString",
            "(" + IN_DESC + "[B)Ljava/lang/String;",
            false);
        break;
      case BYTES:
        mv.visitVarInsn(Opcodes.ALOAD, inLocal);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC, WIRE_CODEC, "readBytes", "(" + IN_DESC + ")[B", false);
        break;
      case DECIMAL:
        mv.visitVarInsn(Opcodes.ALOAD, inLocal);
        mv.visitVarInsn(Opcodes.ALOAD, bufLocal);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC,
            WIRE_CODEC,
            "readDecimal",
            "(" + IN_DESC + "[B)Ljava/math/BigDecimal;",
            false);
        break;
      case JODA_INSTANT:
        mv.visitVarInsn(Opcodes.ALOAD, inLocal);
        mv.visitVarInsn(Opcodes.ALOAD, bufLocal);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC,
            WIRE_CODEC,
            "readDateTime",
            "(" + IN_DESC + "[B)Lorg/joda/time/Instant;",
            false);
        break;
      case JODA_DATETIME:
        mv.visitVarInsn(Opcodes.ALOAD, inLocal);
        mv.visitVarInsn(Opcodes.ALOAD, bufLocal);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC,
            GEN_TYPE,
            "readJodaDateTime",
            "(" + IN_DESC + "[B)Lorg/joda/time/DateTime;",
            false);
        break;
      case LOCAL_DATE:
        mv.visitVarInsn(Opcodes.ALOAD, inLocal);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC,
            WIRE_CODEC,
            "readLocalDate",
            "(" + IN_DESC + ")Ljava/time/LocalDate;",
            false);
        break;
      case LOCAL_TIME:
        mv.visitVarInsn(Opcodes.ALOAD, inLocal);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC,
            WIRE_CODEC,
            "readLocalTime",
            "(" + IN_DESC + ")Ljava/time/LocalTime;",
            false);
        break;
      case LOCAL_DATETIME:
        mv.visitVarInsn(Opcodes.ALOAD, inLocal);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC,
            WIRE_CODEC,
            "readLocalDateTime",
            "(" + IN_DESC + ")Ljava/time/LocalDateTime;",
            false);
        break;
      case UUID_TYPE:
        mv.visitVarInsn(Opcodes.ALOAD, inLocal);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC,
            WIRE_CODEC,
            "readUuid",
            "(" + IN_DESC + ")Ljava/util/UUID;",
            false);
        break;
      case NANOS_INSTANT:
        mv.visitVarInsn(Opcodes.ALOAD, inLocal);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC,
            WIRE_CODEC,
            "readNanosInstant",
            "(" + IN_DESC + ")Ljava/time/Instant;",
            false);
        break;
      case MICROS_INSTANT:
        mv.visitVarInsn(Opcodes.ALOAD, inLocal);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC,
            WIRE_CODEC,
            "readMicrosInstant",
            "(" + IN_DESC + ")Ljava/time/Instant;",
            false);
        break;
      case ENUM_ORDINAL:
        mv.visitLdcInsn(Type.getType(spec.javaType));
        mv.visitVarInsn(Opcodes.ALOAD, inLocal);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC, WIRE_CODEC, "readVarInt", "(" + IN_DESC + ")I", false);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC,
            GEN_TYPE,
            "enumConstantForOrdinal",
            "(Ljava/lang/Class;I)Ljava/lang/Object;",
            false);
        mv.visitTypeInsn(Opcodes.CHECKCAST, Type.getInternalName(spec.javaType));
        break;
      case LIST_STRING:
        mv.visitVarInsn(Opcodes.ALOAD, inLocal);
        mv.visitVarInsn(Opcodes.ALOAD, bufLocal);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC,
            WIRE_CODEC,
            "readStringList",
            "(" + IN_DESC + "[B)Ljava/util/List;",
            false);
        if (spec.javaType == ArrayList.class) {
          mv.visitTypeInsn(Opcodes.CHECKCAST, "java/util/ArrayList");
        }
        break;
      case LIST_INT:
        mv.visitVarInsn(Opcodes.ALOAD, inLocal);
        mv.visitVarInsn(Opcodes.ALOAD, bufLocal);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC,
            WIRE_CODEC,
            "readInt32List",
            "(" + IN_DESC + "[B)Ljava/util/List;",
            false);
        if (spec.javaType == ArrayList.class) {
          mv.visitTypeInsn(Opcodes.CHECKCAST, "java/util/ArrayList");
        }
        break;
      case LIST_LONG:
        mv.visitVarInsn(Opcodes.ALOAD, inLocal);
        mv.visitVarInsn(Opcodes.ALOAD, bufLocal);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC,
            WIRE_CODEC,
            "readInt64List",
            "(" + IN_DESC + "[B)Ljava/util/List;",
            false);
        if (spec.javaType == ArrayList.class) {
          mv.visitTypeInsn(Opcodes.CHECKCAST, "java/util/ArrayList");
        }
        break;
      case LIST_DOUBLE:
        mv.visitVarInsn(Opcodes.ALOAD, inLocal);
        mv.visitVarInsn(Opcodes.ALOAD, bufLocal);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC,
            WIRE_CODEC,
            "readDoubleList",
            "(" + IN_DESC + "[B)Ljava/util/List;",
            false);
        if (spec.javaType == ArrayList.class) {
          mv.visitTypeInsn(Opcodes.CHECKCAST, "java/util/ArrayList");
        }
        break;
      case LIST_ROW:
        mv.visitVarInsn(Opcodes.ALOAD, inLocal);
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitFieldInsn(
            Opcodes.GETFIELD, BASE_TYPE, "subCoders", "[Lorg/apache/beam/sdk/coders/Coder;");
        pushInt(mv, spec.encPos);
        mv.visitInsn(Opcodes.AALOAD);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC,
            WIRE_CODEC,
            "readElementList",
            "(" + IN_DESC + "Lorg/apache/beam/sdk/coders/Coder;)Ljava/util/List;",
            false);
        if (spec.javaType == ArrayList.class) {
          mv.visitTypeInsn(Opcodes.CHECKCAST, "java/util/ArrayList");
        }
        break;
      case MAP_STRING_LONG:
        mv.visitVarInsn(Opcodes.ALOAD, inLocal);
        mv.visitVarInsn(Opcodes.ALOAD, bufLocal);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC,
            WIRE_CODEC,
            "readStringLongMap",
            "(" + IN_DESC + "[B)Ljava/util/Map;",
            false);
        break;
      case MAP_STRING_STRING:
        mv.visitVarInsn(Opcodes.ALOAD, inLocal);
        mv.visitVarInsn(Opcodes.ALOAD, bufLocal);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC,
            WIRE_CODEC,
            "readStringStringMap",
            "(" + IN_DESC + "[B)Ljava/util/Map;",
            false);
        break;
      case MAP_STRING_INT:
        mv.visitVarInsn(Opcodes.ALOAD, inLocal);
        mv.visitVarInsn(Opcodes.ALOAD, bufLocal);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC,
            WIRE_CODEC,
            "readStringIntMap",
            "(" + IN_DESC + "[B)Ljava/util/Map;",
            false);
        break;
      case MAP_STRING_DOUBLE:
        mv.visitVarInsn(Opcodes.ALOAD, inLocal);
        mv.visitVarInsn(Opcodes.ALOAD, bufLocal);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC,
            WIRE_CODEC,
            "readStringDoubleMap",
            "(" + IN_DESC + "[B)Ljava/util/Map;",
            false);
        break;
      case SUB_ROW:
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitFieldInsn(
            Opcodes.GETFIELD, BASE_TYPE, "subCoders", "[Lorg/apache/beam/sdk/coders/Coder;");
        pushInt(mv, spec.encPos);
        mv.visitInsn(Opcodes.AALOAD);
        mv.visitVarInsn(Opcodes.ALOAD, inLocal);
        mv.visitMethodInsn(
            Opcodes.INVOKEVIRTUAL,
            CODER_TYPE,
            "decode",
            "(" + IN_DESC + ")Ljava/lang/Object;",
            false);
        mv.visitTypeInsn(Opcodes.CHECKCAST, Type.getInternalName(spec.javaType));
        break;
    }
  }

  private static final class EncodeImpl implements Implementation {
    private final Class<?> targetClass;
    private final FieldSpec[] specs;
    private final boolean staticEncoding;
    private final boolean hasNullableFields;

    EncodeImpl(
        Class<?> targetClass,
        FieldSpec[] specs,
        boolean staticEncoding,
        boolean hasNullableFields) {
      this.targetClass = targetClass;
      this.specs = specs;
      this.staticEncoding = staticEncoding;
      this.hasNullableFields = hasNullableFields;
    }

    @Override
    public InstrumentedType prepare(InstrumentedType instrumentedType) {
      return instrumentedType;
    }

    @Override
    public ByteCodeAppender appender(Target target) {
      return (mv, context, method) -> {
        int count = specs.length;
        String targetInternal = Type.getInternalName(targetClass);
        // Slots: 0=this, 1=rawValue, 2=out, 3=typedObj
        Label nonNullArg = new Label();
        mv.visitVarInsn(Opcodes.ALOAD, 1);
        mv.visitJumpInsn(Opcodes.IFNONNULL, nonNullArg);
        mv.visitLdcInsn(targetClass.getSimpleName());
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC,
            BASE_TYPE,
            "cannotEncodeNull",
            "(Ljava/lang/String;)Ljava/io/IOException;",
            false);
        mv.visitInsn(Opcodes.ATHROW);
        mv.visitLabel(nonNullArg);

        mv.visitVarInsn(Opcodes.ALOAD, 1);
        mv.visitTypeInsn(Opcodes.CHECKCAST, targetInternal);
        mv.visitVarInsn(Opcodes.ASTORE, 3);

        if (!hasNullableFields) {
          // Slots: 4=tempRef, 5=pool, 6=buf, 7=pos, 8=ex
          Label tryStart = new Label();
          Label tryEnd = new Label();
          Label catchHandler = new Label();
          mv.visitTryCatchBlock(tryStart, tryEnd, catchHandler, null);

          mv.visitMethodInsn(
              Opcodes.INVOKESTATIC, WIRE_CODEC, "scratchPool", "()" + SCRATCH_POOL_DESC, false);
          mv.visitVarInsn(Opcodes.ASTORE, 5);
          mv.visitVarInsn(Opcodes.ALOAD, 5);
          mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, SCRATCH_POOL, "acquire", "()[B", false);
          mv.visitVarInsn(Opcodes.ASTORE, 6);

          mv.visitLabel(tryStart);
          mv.visitInsn(Opcodes.ICONST_0);
          mv.visitVarInsn(Opcodes.ISTORE, 7);
          if (!staticEncoding) {
            pushInt(mv, count);
            mv.visitInsn(Opcodes.LCONST_0);
            emitPackCall(mv, "packHeader", "(IJ" + PACK_SUFFIX, 6, 7, 2);
          }
          for (int pos = 0; pos < count; pos++) {
            FieldSpec spec = specs[pos];
            if (spec.kind.isPrimitive()) {
              emitPackPrimitive(mv, spec, 3, 2, 6, 7);
            } else {
              emitReadMember(mv, 3, spec);
              mv.visitVarInsn(Opcodes.ASTORE, 4);
              Label ok = new Label();
              mv.visitVarInsn(Opcodes.ALOAD, 4);
              mv.visitJumpInsn(Opcodes.IFNONNULL, ok);
              emitThrowNullNonNullable(mv, spec.rowIndex);
              mv.visitLabel(ok);
              emitPackNonNullValue(mv, spec, 4, 2, 6, 7);
            }
          }
          emitFlush(mv, 6, 7, 2);
          mv.visitLabel(tryEnd);

          mv.visitVarInsn(Opcodes.ALOAD, 5);
          mv.visitVarInsn(Opcodes.ALOAD, 6);
          mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, SCRATCH_POOL, "release", "([B)V", false);
          mv.visitInsn(Opcodes.RETURN);

          mv.visitLabel(catchHandler);
          mv.visitVarInsn(Opcodes.ASTORE, 8);
          mv.visitVarInsn(Opcodes.ALOAD, 5);
          mv.visitVarInsn(Opcodes.ALOAD, 6);
          mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, SCRATCH_POOL, "release", "([B)V", false);
          mv.visitVarInsn(Opcodes.ALOAD, 8);
          mv.visitInsn(Opcodes.ATHROW);
          return new ByteCodeAppender.Size(8, 9);
        }

        // Nullable schema: evaluate each reference member ONCE into its own local, build long mask.
        // Slots: 4..5=nullMask, 6=pool, 7=buf, 8=pos, 9=ex, ref locals start at 10.
        int[] refSlots = new int[count];
        int nextSlot = 10;
        mv.visitInsn(Opcodes.LCONST_0);
        mv.visitVarInsn(Opcodes.LSTORE, 4);
        for (int pos = 0; pos < count; pos++) {
          FieldSpec spec = specs[pos];
          if (spec.kind.isPrimitive()) {
            continue;
          }
          int slot = nextSlot++;
          refSlots[pos] = slot;
          emitReadMember(mv, 3, spec);
          mv.visitVarInsn(Opcodes.ASTORE, slot);
          Label notNull = new Label();
          mv.visitVarInsn(Opcodes.ALOAD, slot);
          mv.visitJumpInsn(Opcodes.IFNONNULL, notNull);
          if (!spec.nullable) {
            emitThrowNullNonNullable(mv, spec.rowIndex);
          } else {
            mv.visitVarInsn(Opcodes.LLOAD, 4);
            mv.visitLdcInsn(1L << pos);
            mv.visitInsn(Opcodes.LOR);
            mv.visitVarInsn(Opcodes.LSTORE, 4);
          }
          mv.visitLabel(notNull);
        }

        Label tryStart = new Label();
        Label tryEnd = new Label();
        Label catchHandler = new Label();
        mv.visitTryCatchBlock(tryStart, tryEnd, catchHandler, null);

        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC, WIRE_CODEC, "scratchPool", "()" + SCRATCH_POOL_DESC, false);
        mv.visitVarInsn(Opcodes.ASTORE, 6);
        mv.visitVarInsn(Opcodes.ALOAD, 6);
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, SCRATCH_POOL, "acquire", "()[B", false);
        mv.visitVarInsn(Opcodes.ASTORE, 7);

        mv.visitLabel(tryStart);
        mv.visitInsn(Opcodes.ICONST_0);
        mv.visitVarInsn(Opcodes.ISTORE, 8);
        pushInt(mv, count);
        mv.visitVarInsn(Opcodes.LLOAD, 4);
        emitPackCall(mv, "packHeader", "(IJ" + PACK_SUFFIX, 7, 8, 2);

        for (int pos = 0; pos < count; pos++) {
          FieldSpec spec = specs[pos];
          if (spec.kind.isPrimitive()) {
            emitPackPrimitive(mv, spec, 3, 2, 7, 8);
          } else if (!spec.nullable) {
            emitPackNonNullValue(mv, spec, refSlots[pos], 2, 7, 8);
          } else {
            Label skip = new Label();
            mv.visitVarInsn(Opcodes.ALOAD, refSlots[pos]);
            mv.visitJumpInsn(Opcodes.IFNULL, skip);
            emitPackNonNullValue(mv, spec, refSlots[pos], 2, 7, 8);
            mv.visitLabel(skip);
          }
        }
        emitFlush(mv, 7, 8, 2);
        mv.visitLabel(tryEnd);

        mv.visitVarInsn(Opcodes.ALOAD, 6);
        mv.visitVarInsn(Opcodes.ALOAD, 7);
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, SCRATCH_POOL, "release", "([B)V", false);
        mv.visitInsn(Opcodes.RETURN);

        mv.visitLabel(catchHandler);
        mv.visitVarInsn(Opcodes.ASTORE, 9);
        mv.visitVarInsn(Opcodes.ALOAD, 6);
        mv.visitVarInsn(Opcodes.ALOAD, 7);
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, SCRATCH_POOL, "release", "([B)V", false);
        mv.visitVarInsn(Opcodes.ALOAD, 9);
        mv.visitInsn(Opcodes.ATHROW);
        return new ByteCodeAppender.Size(8, nextSlot);
      };
    }
  }

  private static boolean needsScratchForDecode(FieldSpec[] specs) {
    for (FieldSpec spec : specs) {
      switch (spec.kind) {
        case PRIM_SHORT:
        case PRIM_FLOAT:
        case PRIM_DOUBLE:
        case BOX_SHORT:
        case BOX_FLOAT:
        case BOX_DOUBLE:
        case STRING:
        case DECIMAL:
        case JODA_INSTANT:
        case JODA_DATETIME:
        case LIST_STRING:
        case LIST_INT:
        case LIST_LONG:
        case LIST_DOUBLE:
        case MAP_STRING_LONG:
        case MAP_STRING_STRING:
        case MAP_STRING_INT:
        case MAP_STRING_DOUBLE:
          return true;
        default:
          break;
      }
    }
    return false;
  }

  private static final class DecodeImpl implements Implementation {
    private final Class<?> targetClass;
    private final FieldSpec[] specs;
    private final boolean staticEncoding;
    private final boolean hasNullableFields;

    DecodeImpl(
        Class<?> targetClass,
        FieldSpec[] specs,
        boolean staticEncoding,
        boolean hasNullableFields) {
      this.targetClass = targetClass;
      this.specs = specs;
      this.staticEncoding = staticEncoding;
      this.hasNullableFields = hasNullableFields;
    }

    @Override
    public InstrumentedType prepare(InstrumentedType instrumentedType) {
      return instrumentedType;
    }

    @Override
    public ByteCodeAppender appender(Target target) {
      return (mv, context, method) -> {
        int count = specs.length;
        String targetInternal = Type.getInternalName(targetClass);
        boolean needsScratch = needsScratchForDecode(specs);
        // Decode-side scratch buffer holds no bytes live across field boundaries and is checked out
        // exclusively from ScratchPool, so nested subCoder.decode calls can never corrupt outer
        // decode state.
        if (staticEncoding) {
          // Slots: 0=this, 1=in, 2=res, [3=pool, 4=buf, 5=ex]
          if (!needsScratch) {
            emitNewTarget(mv, targetInternal, 2);
            for (int pos = 0; pos < count; pos++) {
              mv.visitVarInsn(Opcodes.ALOAD, 2);
              emitReadValueForDecode(mv, specs[pos], 1, -1);
              emitWriteMember(mv, specs[pos]);
            }
            mv.visitVarInsn(Opcodes.ALOAD, 2);
            mv.visitInsn(Opcodes.ARETURN);
            return new ByteCodeAppender.Size(6, 3);
          }
          Label tryStart = new Label();
          Label tryEnd = new Label();
          Label catchHandler = new Label();
          mv.visitTryCatchBlock(tryStart, tryEnd, catchHandler, null);

          mv.visitMethodInsn(
              Opcodes.INVOKESTATIC, WIRE_CODEC, "scratchPool", "()" + SCRATCH_POOL_DESC, false);
          mv.visitVarInsn(Opcodes.ASTORE, 3);
          mv.visitVarInsn(Opcodes.ALOAD, 3);
          mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, SCRATCH_POOL, "acquire", "()[B", false);
          mv.visitVarInsn(Opcodes.ASTORE, 4);

          mv.visitLabel(tryStart);
          emitNewTarget(mv, targetInternal, 2);
          for (int pos = 0; pos < count; pos++) {
            mv.visitVarInsn(Opcodes.ALOAD, 2);
            emitReadValueForDecode(mv, specs[pos], 1, 4);
            emitWriteMember(mv, specs[pos]);
          }
          mv.visitLabel(tryEnd);

          mv.visitVarInsn(Opcodes.ALOAD, 3);
          mv.visitVarInsn(Opcodes.ALOAD, 4);
          mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, SCRATCH_POOL, "release", "([B)V", false);
          mv.visitVarInsn(Opcodes.ALOAD, 2);
          mv.visitInsn(Opcodes.ARETURN);

          mv.visitLabel(catchHandler);
          mv.visitVarInsn(Opcodes.ASTORE, 5);
          mv.visitVarInsn(Opcodes.ALOAD, 3);
          mv.visitVarInsn(Opcodes.ALOAD, 4);
          mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, SCRATCH_POOL, "release", "([B)V", false);
          mv.visitVarInsn(Opcodes.ALOAD, 5);
          mv.visitInsn(Opcodes.ATHROW);
          return new ByteCodeAppender.Size(6, 6);
        }

        // Slots: 0=this, 1=in, 2=res, 3=streamFieldCount, 4=nullByteLen, 5..6=nullMask,
        //        7=pool, 8=buf, 9=ex
        // streamFieldCount = readVarInt(in)
        mv.visitVarInsn(Opcodes.ALOAD, 1);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC, WIRE_CODEC, "readVarInt", "(" + IN_DESC + ")I", false);
        mv.visitVarInsn(Opcodes.ISTORE, 3);

        Label matchingCount = new Label();
        mv.visitVarInsn(Opcodes.ILOAD, 3);
        pushInt(mv, count);
        mv.visitJumpInsn(Opcodes.IF_ICMPEQ, matchingCount);
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitVarInsn(Opcodes.ILOAD, 3);
        mv.visitVarInsn(Opcodes.ALOAD, 1);
        mv.visitMethodInsn(
            Opcodes.INVOKEVIRTUAL,
            BASE_TYPE,
            "decodeEvolved",
            "(I" + IN_DESC + ")Ljava/lang/Object;",
            false);
        mv.visitInsn(Opcodes.ARETURN);
        mv.visitLabel(matchingCount);

        // nullByteLen = readVarInt(in)
        mv.visitVarInsn(Opcodes.ALOAD, 1);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC, WIRE_CODEC, "readVarInt", "(" + IN_DESC + ")I", false);
        mv.visitVarInsn(Opcodes.ISTORE, 4);

        Label badNullLen = new Label();
        mv.visitVarInsn(Opcodes.ILOAD, 4);
        mv.visitJumpInsn(Opcodes.IFLT, badNullLen);

        if (!hasNullableFields) {
          Label fallbackNulls = new Label();
          mv.visitVarInsn(Opcodes.ILOAD, 4);
          mv.visitJumpInsn(Opcodes.IFNE, fallbackNulls);
          if (!needsScratch) {
            emitNewTarget(mv, targetInternal, 2);
            for (int pos = 0; pos < count; pos++) {
              mv.visitVarInsn(Opcodes.ALOAD, 2);
              emitReadValueForDecode(mv, specs[pos], 1, -1);
              emitWriteMember(mv, specs[pos]);
            }
            mv.visitVarInsn(Opcodes.ALOAD, 2);
            mv.visitInsn(Opcodes.ARETURN);
          } else {
            // Slots on fast path: 5=pool, 6=buf, 7=ex
            Label tryStart = new Label();
            Label tryEnd = new Label();
            Label catchHandler = new Label();
            mv.visitTryCatchBlock(tryStart, tryEnd, catchHandler, null);

            mv.visitMethodInsn(
                Opcodes.INVOKESTATIC, WIRE_CODEC, "scratchPool", "()" + SCRATCH_POOL_DESC, false);
            mv.visitVarInsn(Opcodes.ASTORE, 5);
            mv.visitVarInsn(Opcodes.ALOAD, 5);
            mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, SCRATCH_POOL, "acquire", "()[B", false);
            mv.visitVarInsn(Opcodes.ASTORE, 6);

            mv.visitLabel(tryStart);
            emitNewTarget(mv, targetInternal, 2);
            for (int pos = 0; pos < count; pos++) {
              mv.visitVarInsn(Opcodes.ALOAD, 2);
              emitReadValueForDecode(mv, specs[pos], 1, 6);
              emitWriteMember(mv, specs[pos]);
            }
            mv.visitLabel(tryEnd);

            mv.visitVarInsn(Opcodes.ALOAD, 5);
            mv.visitVarInsn(Opcodes.ALOAD, 6);
            mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, SCRATCH_POOL, "release", "([B)V", false);
            mv.visitVarInsn(Opcodes.ALOAD, 2);
            mv.visitInsn(Opcodes.ARETURN);

            mv.visitLabel(catchHandler);
            mv.visitVarInsn(Opcodes.ASTORE, 7);
            mv.visitVarInsn(Opcodes.ALOAD, 5);
            mv.visitVarInsn(Opcodes.ALOAD, 6);
            mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, SCRATCH_POOL, "release", "([B)V", false);
            mv.visitVarInsn(Opcodes.ALOAD, 7);
            mv.visitInsn(Opcodes.ATHROW);
          }

          mv.visitLabel(fallbackNulls);
          mv.visitVarInsn(Opcodes.ALOAD, 0);
          mv.visitVarInsn(Opcodes.ILOAD, 3);
          mv.visitVarInsn(Opcodes.ILOAD, 4);
          mv.visitVarInsn(Opcodes.ALOAD, 1);
          mv.visitMethodInsn(
              Opcodes.INVOKEVIRTUAL,
              BASE_TYPE,
              "decodeEvolvedAfterNullLen",
              "(II" + IN_DESC + ")Ljava/lang/Object;",
              false);
          mv.visitInsn(Opcodes.ARETURN);

          mv.visitLabel(badNullLen);
          mv.visitVarInsn(Opcodes.ILOAD, 4);
          mv.visitMethodInsn(
              Opcodes.INVOKESTATIC,
              BASE_TYPE,
              "invalidNullBitmapLength",
              "(I)Ljava/io/IOException;",
              false);
          mv.visitInsn(Opcodes.ATHROW);
          return new ByteCodeAppender.Size(6, needsScratch ? 8 : 5);
        }

        // Nullable schema: handle nullByteLen == 0 (or 1..8) directly; > 8 falls back cleanly.
        Label hasNulls = new Label();
        Label wideNulls = new Label();
        mv.visitVarInsn(Opcodes.ILOAD, 4);
        pushInt(mv, 8);
        mv.visitJumpInsn(Opcodes.IF_ICMPGT, wideNulls);

        if (needsScratch) {
          // Acquire one scratch buffer covering null mask + all field decodes.
          // Slots: 5..6=nullMask, 7=pool, 8=buf, 9=ex
          Label tryStart = new Label();
          Label tryEnd = new Label();
          Label retLabel = new Label();
          Label catchHandler = new Label();
          mv.visitTryCatchBlock(tryStart, tryEnd, catchHandler, null);

          mv.visitMethodInsn(
              Opcodes.INVOKESTATIC, WIRE_CODEC, "scratchPool", "()" + SCRATCH_POOL_DESC, false);
          mv.visitVarInsn(Opcodes.ASTORE, 7);
          mv.visitVarInsn(Opcodes.ALOAD, 7);
          mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, SCRATCH_POOL, "acquire", "()[B", false);
          mv.visitVarInsn(Opcodes.ASTORE, 8);

          mv.visitLabel(tryStart);
          if (count <= 32) {
            mv.visitVarInsn(Opcodes.ILOAD, 4);
            mv.visitJumpInsn(Opcodes.IFNE, hasNulls);
            emitNewTarget(mv, targetInternal, 2);
            for (int pos = 0; pos < count; pos++) {
              mv.visitVarInsn(Opcodes.ALOAD, 2);
              emitReadValueForDecode(mv, specs[pos], 1, 8);
              emitWriteMember(mv, specs[pos]);
            }
            mv.visitJumpInsn(Opcodes.GOTO, retLabel);

            mv.visitLabel(hasNulls);
            mv.visitVarInsn(Opcodes.ALOAD, 1);
            mv.visitVarInsn(Opcodes.ILOAD, 4);
            mv.visitVarInsn(Opcodes.ALOAD, 8);
            mv.visitMethodInsn(
                Opcodes.INVOKESTATIC, WIRE_CODEC, "readNullMask", "(" + IN_DESC + "I[B)J", false);
            mv.visitVarInsn(Opcodes.LSTORE, 5);
          } else {
            Label afterMask = new Label();
            mv.visitInsn(Opcodes.LCONST_0);
            mv.visitVarInsn(Opcodes.LSTORE, 5);
            mv.visitVarInsn(Opcodes.ILOAD, 4);
            mv.visitJumpInsn(Opcodes.IFEQ, afterMask);
            mv.visitVarInsn(Opcodes.ALOAD, 1);
            mv.visitVarInsn(Opcodes.ILOAD, 4);
            mv.visitVarInsn(Opcodes.ALOAD, 8);
            mv.visitMethodInsn(
                Opcodes.INVOKESTATIC, WIRE_CODEC, "readNullMask", "(" + IN_DESC + "I[B)J", false);
            mv.visitVarInsn(Opcodes.LSTORE, 5);
            mv.visitLabel(afterMask);
          }

          emitMaskedDecodeFields(mv, targetInternal, count, 8);
          mv.visitLabel(retLabel);
          mv.visitLabel(tryEnd);

          mv.visitVarInsn(Opcodes.ALOAD, 7);
          mv.visitVarInsn(Opcodes.ALOAD, 8);
          mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, SCRATCH_POOL, "release", "([B)V", false);
          mv.visitVarInsn(Opcodes.ALOAD, 2);
          mv.visitInsn(Opcodes.ARETURN);

          mv.visitLabel(catchHandler);
          mv.visitVarInsn(Opcodes.ASTORE, 9);
          mv.visitVarInsn(Opcodes.ALOAD, 7);
          mv.visitVarInsn(Opcodes.ALOAD, 8);
          mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, SCRATCH_POOL, "release", "([B)V", false);
          mv.visitVarInsn(Opcodes.ALOAD, 9);
          mv.visitInsn(Opcodes.ATHROW);
        } else {
          // No field uses buf: avoid ThreadLocal lookup completely when nullByteLen == 0, and
          // borrow/release buf solely around readNullMask when nullByteLen > 0.
          if (count <= 32) {
            mv.visitVarInsn(Opcodes.ILOAD, 4);
            mv.visitJumpInsn(Opcodes.IFNE, hasNulls);
            emitNewTarget(mv, targetInternal, 2);
            for (int pos = 0; pos < count; pos++) {
              mv.visitVarInsn(Opcodes.ALOAD, 2);
              emitReadValueForDecode(mv, specs[pos], 1, -1);
              emitWriteMember(mv, specs[pos]);
            }
            mv.visitVarInsn(Opcodes.ALOAD, 2);
            mv.visitInsn(Opcodes.ARETURN);

            mv.visitLabel(hasNulls);
            emitReadNullMaskWithPool(mv, 1, 4, 5, 7, 8, 9);
          } else {
            Label afterMask = new Label();
            mv.visitInsn(Opcodes.LCONST_0);
            mv.visitVarInsn(Opcodes.LSTORE, 5);
            mv.visitVarInsn(Opcodes.ILOAD, 4);
            mv.visitJumpInsn(Opcodes.IFEQ, afterMask);
            emitReadNullMaskWithPool(mv, 1, 4, 5, 7, 8, 9);
            mv.visitLabel(afterMask);
          }

          emitMaskedDecodeFields(mv, targetInternal, count, -1);
          mv.visitVarInsn(Opcodes.ALOAD, 2);
          mv.visitInsn(Opcodes.ARETURN);
        }

        mv.visitLabel(wideNulls);
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitVarInsn(Opcodes.ILOAD, 3);
        mv.visitVarInsn(Opcodes.ILOAD, 4);
        mv.visitVarInsn(Opcodes.ALOAD, 1);
        mv.visitMethodInsn(
            Opcodes.INVOKEVIRTUAL,
            BASE_TYPE,
            "decodeEvolvedAfterNullLen",
            "(II" + IN_DESC + ")Ljava/lang/Object;",
            false);
        mv.visitInsn(Opcodes.ARETURN);

        mv.visitLabel(badNullLen);
        mv.visitVarInsn(Opcodes.ILOAD, 4);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC,
            BASE_TYPE,
            "invalidNullBitmapLength",
            "(I)Ljava/io/IOException;",
            false);
        mv.visitInsn(Opcodes.ATHROW);
        return new ByteCodeAppender.Size(6, 10);
      };
    }

    private void emitReadNullMaskWithPool(
        MethodVisitor mv,
        int inLocal,
        int lenLocal,
        int maskLocal,
        int poolLocal,
        int bufLocal,
        int exLocal) {
      Label tryStart = new Label();
      Label tryEnd = new Label();
      Label catchHandler = new Label();
      Label after = new Label();
      mv.visitTryCatchBlock(tryStart, tryEnd, catchHandler, null);

      mv.visitMethodInsn(
          Opcodes.INVOKESTATIC, WIRE_CODEC, "scratchPool", "()" + SCRATCH_POOL_DESC, false);
      mv.visitVarInsn(Opcodes.ASTORE, poolLocal);
      mv.visitVarInsn(Opcodes.ALOAD, poolLocal);
      mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, SCRATCH_POOL, "acquire", "()[B", false);
      mv.visitVarInsn(Opcodes.ASTORE, bufLocal);

      mv.visitLabel(tryStart);
      mv.visitVarInsn(Opcodes.ALOAD, inLocal);
      mv.visitVarInsn(Opcodes.ILOAD, lenLocal);
      mv.visitVarInsn(Opcodes.ALOAD, bufLocal);
      mv.visitMethodInsn(
          Opcodes.INVOKESTATIC, WIRE_CODEC, "readNullMask", "(" + IN_DESC + "I[B)J", false);
      mv.visitVarInsn(Opcodes.LSTORE, maskLocal);
      mv.visitLabel(tryEnd);

      mv.visitVarInsn(Opcodes.ALOAD, poolLocal);
      mv.visitVarInsn(Opcodes.ALOAD, bufLocal);
      mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, SCRATCH_POOL, "release", "([B)V", false);
      mv.visitJumpInsn(Opcodes.GOTO, after);

      mv.visitLabel(catchHandler);
      mv.visitVarInsn(Opcodes.ASTORE, exLocal);
      mv.visitVarInsn(Opcodes.ALOAD, poolLocal);
      mv.visitVarInsn(Opcodes.ALOAD, bufLocal);
      mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, SCRATCH_POOL, "release", "([B)V", false);
      mv.visitVarInsn(Opcodes.ALOAD, exLocal);
      mv.visitInsn(Opcodes.ATHROW);

      mv.visitLabel(after);
    }

    private void emitMaskedDecodeFields(
        MethodVisitor mv, String targetInternal, int count, int bufLocal) {
      emitNewTarget(mv, targetInternal, 2);
      for (int pos = 0; pos < count; pos++) {
        FieldSpec spec = specs[pos];
        Label isNull = new Label();
        Label done = new Label();
        mv.visitVarInsn(Opcodes.LLOAD, 5);
        mv.visitLdcInsn(1L << pos);
        mv.visitInsn(Opcodes.LAND);
        mv.visitInsn(Opcodes.LCONST_0);
        mv.visitInsn(Opcodes.LCMP);
        mv.visitJumpInsn(Opcodes.IFNE, isNull);
        mv.visitVarInsn(Opcodes.ALOAD, 2);
        emitReadValueForDecode(mv, spec, 1, bufLocal);
        emitWriteMember(mv, spec);
        mv.visitJumpInsn(Opcodes.GOTO, done);

        mv.visitLabel(isNull);
        if (spec.kind.isPrimitive()) {
          mv.visitLdcInsn(spec.schemaFieldName);
          mv.visitMethodInsn(
              Opcodes.INVOKESTATIC,
              BASE_TYPE,
              "nullPrimitive",
              "(Ljava/lang/String;)Ljava/lang/NullPointerException;",
              false);
          mv.visitInsn(Opcodes.ATHROW);
        } else {
          // Explicitly write null to overwrite any non-null default field initializer in ctor.
          mv.visitVarInsn(Opcodes.ALOAD, 2);
          mv.visitInsn(Opcodes.ACONST_NULL);
          emitWriteMember(mv, spec);
        }
        mv.visitLabel(done);
      }
    }
  }

  private static void emitNewTarget(MethodVisitor mv, String targetInternal, int localSlot) {
    mv.visitTypeInsn(Opcodes.NEW, targetInternal);
    mv.visitInsn(Opcodes.DUP);
    mv.visitMethodInsn(Opcodes.INVOKESPECIAL, targetInternal, "<init>", "()V", false);
    mv.visitVarInsn(Opcodes.ASTORE, localSlot);
  }

  private static void emitNonNullArgCheck(MethodVisitor mv, Class<?> targetClass) {
    Label nonNullArg = new Label();
    mv.visitVarInsn(Opcodes.ALOAD, 1);
    mv.visitJumpInsn(Opcodes.IFNONNULL, nonNullArg);
    mv.visitLdcInsn(targetClass.getSimpleName());
    mv.visitMethodInsn(
        Opcodes.INVOKESTATIC,
        BASE_TYPE,
        "cannotEncodeNull",
        "(Ljava/lang/String;)Ljava/io/IOException;",
        false);
    mv.visitInsn(Opcodes.ATHROW);
    mv.visitLabel(nonNullArg);
  }

  private static void emitIsCheapRange(
      MethodVisitor mv, FieldSpec[] specs, int start, int end, int objSlot, int tempSlot) {
    for (int pos = start; pos < end; pos++) {
      FieldSpec spec = specs[pos];
      switch (spec.kind) {
        case STRING:
          emitReadMember(mv, objSlot, spec);
          mv.visitVarInsn(Opcodes.ASTORE, tempSlot);
          Label sOk = new Label();
          mv.visitVarInsn(Opcodes.ALOAD, tempSlot);
          mv.visitJumpInsn(Opcodes.IFNULL, sOk);
          mv.visitVarInsn(Opcodes.ALOAD, tempSlot);
          mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/String", "length", "()I", false);
          pushInt(mv, 1024);
          mv.visitJumpInsn(Opcodes.IF_ICMPLE, sOk);
          mv.visitInsn(Opcodes.ICONST_0);
          mv.visitInsn(Opcodes.IRETURN);
          mv.visitLabel(sOk);
          break;
        case LIST_STRING:
        case LIST_INT:
        case LIST_LONG:
        case LIST_DOUBLE:
        case LIST_ROW:
          // Cheap when null or an ElementByteSizeObservableIterable (matches ListCoder).
          emitReadMember(mv, objSlot, spec);
          mv.visitVarInsn(Opcodes.ASTORE, tempSlot);
          Label lOk = new Label();
          mv.visitVarInsn(Opcodes.ALOAD, tempSlot);
          mv.visitJumpInsn(Opcodes.IFNULL, lOk);
          mv.visitVarInsn(Opcodes.ALOAD, tempSlot);
          mv.visitTypeInsn(Opcodes.INSTANCEOF, OBSERVABLE_ITERABLE_TYPE);
          mv.visitJumpInsn(Opcodes.IFNE, lOk);
          mv.visitInsn(Opcodes.ICONST_0);
          mv.visitInsn(Opcodes.IRETURN);
          mv.visitLabel(lOk);
          break;
        case MAP_STRING_LONG:
        case MAP_STRING_STRING:
        case MAP_STRING_INT:
        case MAP_STRING_DOUBLE:
          // Only cheap when null (matches RowCoderGenerator delegating to MapCoder).
          emitReadMember(mv, objSlot, spec);
          Label cOk = new Label();
          mv.visitJumpInsn(Opcodes.IFNULL, cOk);
          mv.visitInsn(Opcodes.ICONST_0);
          mv.visitInsn(Opcodes.IRETURN);
          mv.visitLabel(cOk);
          break;
        case SUB_ROW:
          emitReadMember(mv, objSlot, spec);
          mv.visitVarInsn(Opcodes.ASTORE, tempSlot);
          Label rOk = new Label();
          mv.visitVarInsn(Opcodes.ALOAD, tempSlot);
          mv.visitJumpInsn(Opcodes.IFNULL, rOk);
          mv.visitVarInsn(Opcodes.ALOAD, 0);
          mv.visitFieldInsn(
              Opcodes.GETFIELD, BASE_TYPE, "subCoders", "[Lorg/apache/beam/sdk/coders/Coder;");
          pushInt(mv, spec.encPos);
          mv.visitInsn(Opcodes.AALOAD);
          mv.visitVarInsn(Opcodes.ALOAD, tempSlot);
          mv.visitMethodInsn(
              Opcodes.INVOKEVIRTUAL,
              CODER_TYPE,
              "isRegisterByteSizeObserverCheap",
              "(Ljava/lang/Object;)Z",
              false);
          mv.visitJumpInsn(Opcodes.IFNE, rOk);
          mv.visitInsn(Opcodes.ICONST_0);
          mv.visitInsn(Opcodes.IRETURN);
          mv.visitLabel(rOk);
          break;
        default:
          break;
      }
    }
  }

  private static long computeFixedConstBytes(
      FieldSpec[] specs, boolean staticEncoding, boolean hasNullableFields) {
    long constBytes = 0L;
    if (!staticEncoding) {
      constBytes += VarInt.getLength(specs.length);
      if (!hasNullableFields) {
        constBytes += 1L; // Empty null bitmask byte (0x00).
      }
    }
    for (FieldSpec spec : specs) {
      switch (spec.kind) {
        case PRIM_BYTE:
        case PRIM_BOOL:
          constBytes += 1L;
          break;
        case PRIM_SHORT:
          constBytes += 2L;
          break;
        case PRIM_FLOAT:
          constBytes += 4L;
          break;
        case PRIM_DOUBLE:
          constBytes += 8L;
          break;
        default:
          break;
      }
    }
    return constBytes;
  }

  private static final class IsCheapImpl implements Implementation {
    private final Class<?> targetClass;
    private final FieldSpec[] specs;

    IsCheapImpl(Class<?> targetClass, FieldSpec[] specs) {
      this.targetClass = targetClass;
      this.specs = specs;
    }

    @Override
    public InstrumentedType prepare(InstrumentedType instrumentedType) {
      return instrumentedType;
    }

    @Override
    public ByteCodeAppender appender(Target target) {
      return (mv, context, method) -> {
        // Slots: 0=this, 1=rawValue, 2=typedObj, 3=tempRef
        // Return false on null, strings > 1024 chars, non-null Maps, or non-observable Collections.
        Label nonNull = new Label();
        mv.visitVarInsn(Opcodes.ALOAD, 1);
        mv.visitJumpInsn(Opcodes.IFNONNULL, nonNull);
        mv.visitInsn(Opcodes.ICONST_0);
        mv.visitInsn(Opcodes.IRETURN);
        mv.visitLabel(nonNull);

        mv.visitVarInsn(Opcodes.ALOAD, 1);
        mv.visitTypeInsn(Opcodes.CHECKCAST, Type.getInternalName(targetClass));
        mv.visitVarInsn(Opcodes.ASTORE, 2);

        emitIsCheapRange(mv, specs, 0, specs.length, 2, 3);
        mv.visitInsn(Opcodes.ICONST_1);
        mv.visitInsn(Opcodes.IRETURN);
        return new ByteCodeAppender.Size(4, 4);
      };
    }
  }

  private static final class ByteSizeImpl implements Implementation {
    private final Class<?> targetClass;
    private final FieldSpec[] specs;
    private final boolean staticEncoding;
    private final boolean hasNullableFields;

    ByteSizeImpl(
        Class<?> targetClass,
        FieldSpec[] specs,
        boolean staticEncoding,
        boolean hasNullableFields) {
      this.targetClass = targetClass;
      this.specs = specs;
      this.staticEncoding = staticEncoding;
      this.hasNullableFields = hasNullableFields;
    }

    @Override
    public InstrumentedType prepare(InstrumentedType instrumentedType) {
      return instrumentedType;
    }

    @Override
    public ByteCodeAppender appender(Target target) {
      return (mv, context, method) -> {
        int count = specs.length;
        // Slots: 0=this, 1=rawValue, 2=observer, 3=obj, 4..5=total, 6=highestNull, 7=tempRef
        Label nonNullArg = new Label();
        mv.visitVarInsn(Opcodes.ALOAD, 1);
        mv.visitJumpInsn(Opcodes.IFNONNULL, nonNullArg);
        mv.visitLdcInsn(targetClass.getSimpleName());
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC,
            BASE_TYPE,
            "cannotEncodeNull",
            "(Ljava/lang/String;)Ljava/io/IOException;",
            false);
        mv.visitInsn(Opcodes.ATHROW);
        mv.visitLabel(nonNullArg);

        mv.visitVarInsn(Opcodes.ALOAD, 1);
        mv.visitTypeInsn(Opcodes.CHECKCAST, Type.getInternalName(targetClass));
        mv.visitVarInsn(Opcodes.ASTORE, 3);

        long constBytes = computeFixedConstBytes(specs, staticEncoding, hasNullableFields);
        mv.visitLdcInsn(constBytes);
        mv.visitVarInsn(Opcodes.LSTORE, 4);
        if (hasNullableFields && !staticEncoding) {
          mv.visitInsn(Opcodes.ICONST_M1);
          mv.visitVarInsn(Opcodes.ISTORE, 6);
        }

        for (int pos = 0; pos < count; pos++) {
          FieldSpec spec = specs[pos];
          switch (spec.kind) {
            case PRIM_BYTE:
            case PRIM_BOOL:
            case PRIM_SHORT:
            case PRIM_FLOAT:
            case PRIM_DOUBLE:
              break;
            case PRIM_INT:
              mv.visitVarInsn(Opcodes.LLOAD, 4);
              emitReadMember(mv, 3, spec);
              mv.visitMethodInsn(Opcodes.INVOKESTATIC, VARINT_TYPE, "getLength", "(I)I", false);
              mv.visitInsn(Opcodes.I2L);
              mv.visitInsn(Opcodes.LADD);
              mv.visitVarInsn(Opcodes.LSTORE, 4);
              break;
            case PRIM_LONG:
              mv.visitVarInsn(Opcodes.LLOAD, 4);
              emitReadMember(mv, 3, spec);
              mv.visitMethodInsn(Opcodes.INVOKESTATIC, VARINT_TYPE, "getLength", "(J)I", false);
              mv.visitInsn(Opcodes.I2L);
              mv.visitInsn(Opcodes.LADD);
              mv.visitVarInsn(Opcodes.LSTORE, 4);
              break;
            default:
              emitReadMember(mv, 3, spec);
              mv.visitVarInsn(Opcodes.ASTORE, 7);
              Label notNull = new Label();
              Label after = new Label();
              mv.visitVarInsn(Opcodes.ALOAD, 7);
              mv.visitJumpInsn(Opcodes.IFNONNULL, notNull);
              if (!spec.nullable) {
                emitThrowNullNonNullable(mv, spec.rowIndex);
              } else {
                if (!staticEncoding) {
                  pushInt(mv, pos);
                  mv.visitVarInsn(Opcodes.ISTORE, 6);
                }
                mv.visitJumpInsn(Opcodes.GOTO, after);
              }
              mv.visitLabel(notNull);
              emitAddRefSize(mv, spec, 7, 4, 2);
              mv.visitLabel(after);
              break;
          }
        }

        if (hasNullableFields && !staticEncoding) {
          mv.visitVarInsn(Opcodes.LLOAD, 4);
          mv.visitVarInsn(Opcodes.ILOAD, 6);
          mv.visitMethodInsn(Opcodes.INVOKESTATIC, WIRE_CODEC, "sizeOfNullMask", "(I)J", false);
          mv.visitInsn(Opcodes.LADD);
          mv.visitVarInsn(Opcodes.LSTORE, 4);
        }

        mv.visitVarInsn(Opcodes.ALOAD, 2);
        mv.visitVarInsn(Opcodes.LLOAD, 4);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC, "java/lang/Long", "valueOf", "(J)Ljava/lang/Long;", false);
        mv.visitMethodInsn(
            Opcodes.INVOKEVIRTUAL, OBSERVER_TYPE, "update", "(Ljava/lang/Object;)V", false);
        mv.visitInsn(Opcodes.RETURN);
        return new ByteCodeAppender.Size(6, 8);
      };
    }

    private static void addConstLong(MethodVisitor mv, int totalSlot, long delta) {
      mv.visitVarInsn(Opcodes.LLOAD, totalSlot);
      mv.visitLdcInsn(delta);
      mv.visitInsn(Opcodes.LADD);
      mv.visitVarInsn(Opcodes.LSTORE, totalSlot);
    }

    private static void emitAddRefSize(
        MethodVisitor mv, FieldSpec spec, int valSlot, int totalSlot, int obsSlot) {
      switch (spec.kind) {
        case BOX_BYTE:
        case BOX_BOOL:
          addConstLong(mv, totalSlot, 1L);
          break;
        case BOX_SHORT:
          addConstLong(mv, totalSlot, 2L);
          break;
        case BOX_FLOAT:
          addConstLong(mv, totalSlot, 4L);
          break;
        case BOX_DOUBLE:
        case JODA_INSTANT:
        case JODA_DATETIME:
          addConstLong(mv, totalSlot, 8L);
          break;
        case BOX_INT:
          mv.visitVarInsn(Opcodes.LLOAD, totalSlot);
          mv.visitVarInsn(Opcodes.ALOAD, valSlot);
          mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Integer", "intValue", "()I", false);
          mv.visitMethodInsn(Opcodes.INVOKESTATIC, VARINT_TYPE, "getLength", "(I)I", false);
          mv.visitInsn(Opcodes.I2L);
          mv.visitInsn(Opcodes.LADD);
          mv.visitVarInsn(Opcodes.LSTORE, totalSlot);
          break;
        case BOX_LONG:
          mv.visitVarInsn(Opcodes.LLOAD, totalSlot);
          mv.visitVarInsn(Opcodes.ALOAD, valSlot);
          mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Long", "longValue", "()J", false);
          mv.visitMethodInsn(Opcodes.INVOKESTATIC, VARINT_TYPE, "getLength", "(J)I", false);
          mv.visitInsn(Opcodes.I2L);
          mv.visitInsn(Opcodes.LADD);
          mv.visitVarInsn(Opcodes.LSTORE, totalSlot);
          break;
        case ENUM_ORDINAL:
          mv.visitVarInsn(Opcodes.LLOAD, totalSlot);
          mv.visitVarInsn(Opcodes.ALOAD, valSlot);
          mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Enum", "ordinal", "()I", false);
          mv.visitMethodInsn(Opcodes.INVOKESTATIC, VARINT_TYPE, "getLength", "(I)I", false);
          mv.visitInsn(Opcodes.I2L);
          mv.visitInsn(Opcodes.LADD);
          mv.visitVarInsn(Opcodes.LSTORE, totalSlot);
          break;
        case STRING:
          emitAddStaticSize(mv, totalSlot, valSlot, "sizeOfString", "(Ljava/lang/String;)J");
          break;
        case BYTES:
          emitAddStaticSize(mv, totalSlot, valSlot, "sizeOfBytes", "([B)J");
          break;
        case DECIMAL:
          emitAddStaticSize(mv, totalSlot, valSlot, "sizeOfDecimal", "(Ljava/math/BigDecimal;)J");
          break;
        case LOCAL_DATE:
          emitAddStaticSize(mv, totalSlot, valSlot, "sizeOfLocalDate", "(Ljava/time/LocalDate;)J");
          break;
        case LOCAL_TIME:
          emitAddStaticSize(mv, totalSlot, valSlot, "sizeOfLocalTime", "(Ljava/time/LocalTime;)J");
          break;
        case LOCAL_DATETIME:
          emitAddStaticSize(
              mv, totalSlot, valSlot, "sizeOfLocalDateTime", "(Ljava/time/LocalDateTime;)J");
          break;
        case UUID_TYPE:
          emitAddStaticSize(mv, totalSlot, valSlot, "sizeOfUuid", "(Ljava/util/UUID;)J");
          break;
        case NANOS_INSTANT:
          emitAddStaticSize(mv, totalSlot, valSlot, "sizeOfNanosInstant", "(Ljava/time/Instant;)J");
          break;
        case MICROS_INSTANT:
          emitAddStaticSize(
              mv, totalSlot, valSlot, "sizeOfMicrosInstant", "(Ljava/time/Instant;)J");
          break;
        case LIST_STRING:
          emitAddObservedCollectionSize(
              mv, totalSlot, valSlot, obsSlot, "observeStringCollectionByteSize");
          break;
        case LIST_INT:
          emitAddObservedCollectionSize(
              mv, totalSlot, valSlot, obsSlot, "observeInt32CollectionByteSize");
          break;
        case LIST_LONG:
          emitAddObservedCollectionSize(
              mv, totalSlot, valSlot, obsSlot, "observeInt64CollectionByteSize");
          break;
        case LIST_DOUBLE:
          emitAddObservedCollectionSize(
              mv, totalSlot, valSlot, obsSlot, "observeDoubleCollectionByteSize");
          break;
        case LIST_ROW:
          mv.visitVarInsn(Opcodes.ALOAD, valSlot);
          mv.visitVarInsn(Opcodes.ALOAD, 0);
          mv.visitFieldInsn(
              Opcodes.GETFIELD, BASE_TYPE, "subCoders", "[Lorg/apache/beam/sdk/coders/Coder;");
          pushInt(mv, spec.encPos);
          mv.visitInsn(Opcodes.AALOAD);
          mv.visitVarInsn(Opcodes.ALOAD, obsSlot);
          mv.visitMethodInsn(
              Opcodes.INVOKESTATIC,
              GEN_TYPE,
              "observeElementCollectionByteSize",
              "(Ljava/util/Collection;Lorg/apache/beam/sdk/coders/Coder;" + OBSERVER_DESC + ")V",
              false);
          break;
        case MAP_STRING_LONG:
          emitAddStaticSize(mv, totalSlot, valSlot, "sizeOfStringLongMap", "(Ljava/util/Map;)J");
          break;
        case MAP_STRING_STRING:
          emitAddStaticSize(mv, totalSlot, valSlot, "sizeOfStringStringMap", "(Ljava/util/Map;)J");
          break;
        case MAP_STRING_INT:
          emitAddStaticSize(mv, totalSlot, valSlot, "sizeOfStringIntMap", "(Ljava/util/Map;)J");
          break;
        case MAP_STRING_DOUBLE:
          emitAddStaticSize(mv, totalSlot, valSlot, "sizeOfStringDoubleMap", "(Ljava/util/Map;)J");
          break;
        case SUB_ROW:
          mv.visitVarInsn(Opcodes.ALOAD, 0);
          mv.visitFieldInsn(
              Opcodes.GETFIELD, BASE_TYPE, "subCoders", "[Lorg/apache/beam/sdk/coders/Coder;");
          pushInt(mv, spec.encPos);
          mv.visitInsn(Opcodes.AALOAD);
          mv.visitVarInsn(Opcodes.ALOAD, valSlot);
          mv.visitVarInsn(Opcodes.ALOAD, obsSlot);
          mv.visitMethodInsn(
              Opcodes.INVOKEVIRTUAL,
              CODER_TYPE,
              "registerByteSizeObserver",
              "(Ljava/lang/Object;" + OBSERVER_DESC + ")V",
              false);
          break;
        default:
          throw new AssertionError("Unexpected kind: " + spec.kind);
      }
    }

    private static void emitAddStaticSize(
        MethodVisitor mv, int totalSlot, int valSlot, String method, String desc) {
      mv.visitVarInsn(Opcodes.LLOAD, totalSlot);
      mv.visitVarInsn(Opcodes.ALOAD, valSlot);
      mv.visitMethodInsn(Opcodes.INVOKESTATIC, WIRE_CODEC, method, desc, false);
      mv.visitInsn(Opcodes.LADD);
      mv.visitVarInsn(Opcodes.LSTORE, totalSlot);
    }

    private static void emitAddObservedCollectionSize(
        MethodVisitor mv, int totalSlot, int valSlot, int obsSlot, String method) {
      mv.visitVarInsn(Opcodes.LLOAD, totalSlot);
      mv.visitVarInsn(Opcodes.ALOAD, valSlot);
      mv.visitVarInsn(Opcodes.ALOAD, obsSlot);
      mv.visitMethodInsn(
          Opcodes.INVOKESTATIC,
          GEN_TYPE,
          method,
          "(Ljava/util/Collection;" + OBSERVER_DESC + ")J",
          false);
      mv.visitInsn(Opcodes.LADD);
      mv.visitVarInsn(Opcodes.LSTORE, totalSlot);
    }
  }

  private static final class NullScanChunkImpl implements Implementation {
    private final FieldSpec[] specs;
    private final int start;
    private final int end;
    private final boolean storeBeanRefs;
    private final int[] refSlotByPos;

    NullScanChunkImpl(
        FieldSpec[] specs, int start, int end, boolean storeBeanRefs, int[] refSlotByPos) {
      this.specs = specs;
      this.start = start;
      this.end = end;
      this.storeBeanRefs = storeBeanRefs;
      this.refSlotByPos = refSlotByPos;
    }

    @Override
    public InstrumentedType prepare(InstrumentedType instrumentedType) {
      return instrumentedType;
    }

    @Override
    public ByteCodeAppender appender(Target target) {
      return (mv, context, method) -> {
        // POJO slots: 0=this, 1=obj, 2=chunkMask, 3=tempRef
        // Bean slots: 0=this, 1=obj, 2=refs, 3=chunkMask, 4=tempRef
        int maskSlot = storeBeanRefs ? 3 : 2;
        int tempSlot = storeBeanRefs ? 4 : 3;
        mv.visitInsn(Opcodes.ICONST_0);
        mv.visitVarInsn(Opcodes.ISTORE, maskSlot);
        for (int pos = start; pos < end; pos++) {
          FieldSpec spec = specs[pos];
          if (spec.kind.isPrimitive()) {
            continue;
          }
          emitReadMember(mv, 1, spec);
          mv.visitVarInsn(Opcodes.ASTORE, tempSlot);
          if (storeBeanRefs) {
            mv.visitVarInsn(Opcodes.ALOAD, 2);
            pushInt(mv, refSlotByPos[pos]);
            mv.visitVarInsn(Opcodes.ALOAD, tempSlot);
            mv.visitInsn(Opcodes.AASTORE);
          }
          Label notNull = new Label();
          mv.visitVarInsn(Opcodes.ALOAD, tempSlot);
          mv.visitJumpInsn(Opcodes.IFNONNULL, notNull);
          if (!spec.nullable) {
            emitThrowNullNonNullable(mv, spec.rowIndex);
          } else {
            mv.visitVarInsn(Opcodes.ILOAD, maskSlot);
            pushInt(mv, 1 << (pos - start));
            mv.visitInsn(Opcodes.IOR);
            mv.visitVarInsn(Opcodes.ISTORE, maskSlot);
          }
          mv.visitLabel(notNull);
        }
        mv.visitVarInsn(Opcodes.ILOAD, maskSlot);
        mv.visitInsn(Opcodes.IRETURN);
        return new ByteCodeAppender.Size(4, tempSlot + 1);
      };
    }
  }

  private static final class EncodeChunkImpl implements Implementation {
    private final FieldSpec[] specs;
    private final int start;
    private final int end;
    private final boolean useBeanRefs;
    private final int[] refSlotByPos;

    EncodeChunkImpl(
        FieldSpec[] specs, int start, int end, boolean useBeanRefs, int[] refSlotByPos) {
      this.specs = specs;
      this.start = start;
      this.end = end;
      this.useBeanRefs = useBeanRefs;
      this.refSlotByPos = refSlotByPos;
    }

    @Override
    public InstrumentedType prepare(InstrumentedType instrumentedType) {
      return instrumentedType;
    }

    @Override
    public ByteCodeAppender appender(Target target) {
      return (mv, context, method) -> {
        // Non-bean slots: 0=this, 1=obj, 2=out, 3=buf, 4=pos, 5=tempRef
        // Bean slots:     0=this, 1=obj, 2=refs, 3=out, 4=buf, 5=pos, 6=tempRef
        int outSlot = useBeanRefs ? 3 : 2;
        int bufSlot = useBeanRefs ? 4 : 3;
        int posSlot = useBeanRefs ? 5 : 4;
        int tempSlot = useBeanRefs ? 6 : 5;
        for (int pos = start; pos < end; pos++) {
          FieldSpec spec = specs[pos];
          if (spec.kind.isPrimitive()) {
            emitPackPrimitive(mv, spec, 1, outSlot, bufSlot, posSlot);
          } else if (useBeanRefs) {
            mv.visitVarInsn(Opcodes.ALOAD, 2);
            pushInt(mv, refSlotByPos[pos]);
            mv.visitInsn(Opcodes.AALOAD);
            if (spec.javaType != Object.class) {
              mv.visitTypeInsn(Opcodes.CHECKCAST, Type.getInternalName(spec.javaType));
            }
            mv.visitVarInsn(Opcodes.ASTORE, tempSlot);
            if (!spec.nullable) {
              emitPackNonNullValue(mv, spec, tempSlot, outSlot, bufSlot, posSlot);
            } else {
              Label skip = new Label();
              mv.visitVarInsn(Opcodes.ALOAD, tempSlot);
              mv.visitJumpInsn(Opcodes.IFNULL, skip);
              emitPackNonNullValue(mv, spec, tempSlot, outSlot, bufSlot, posSlot);
              mv.visitLabel(skip);
            }
          } else {
            emitReadMember(mv, 1, spec);
            mv.visitVarInsn(Opcodes.ASTORE, tempSlot);
            if (spec.nullable) {
              Label skip = new Label();
              mv.visitVarInsn(Opcodes.ALOAD, tempSlot);
              mv.visitJumpInsn(Opcodes.IFNULL, skip);
              emitPackNonNullValue(mv, spec, tempSlot, outSlot, bufSlot, posSlot);
              mv.visitLabel(skip);
            } else {
              Label ok = new Label();
              mv.visitVarInsn(Opcodes.ALOAD, tempSlot);
              mv.visitJumpInsn(Opcodes.IFNONNULL, ok);
              emitThrowNullNonNullable(mv, spec.rowIndex);
              mv.visitLabel(ok);
              emitPackNonNullValue(mv, spec, tempSlot, outSlot, bufSlot, posSlot);
            }
          }
        }
        mv.visitVarInsn(Opcodes.ILOAD, posSlot);
        mv.visitInsn(Opcodes.IRETURN);
        return new ByteCodeAppender.Size(8, tempSlot + 1);
      };
    }
  }

  private static final class ChunkedEncodeImpl implements Implementation {
    private final Class<?> targetClass;
    private final int count;
    private final int chunkCount;
    private final boolean staticEncoding;
    private final boolean hasNullableFields;
    private final boolean isPojo;
    private final int refCount;
    private final boolean[] chunkHasRefs;

    ChunkedEncodeImpl(
        Class<?> targetClass,
        int count,
        int chunkCount,
        boolean staticEncoding,
        boolean hasNullableFields,
        boolean isPojo,
        int refCount,
        boolean[] chunkHasRefs) {
      this.targetClass = targetClass;
      this.count = count;
      this.chunkCount = chunkCount;
      this.staticEncoding = staticEncoding;
      this.hasNullableFields = hasNullableFields;
      this.isPojo = isPojo;
      this.refCount = refCount;
      this.chunkHasRefs = chunkHasRefs;
    }

    @Override
    public InstrumentedType prepare(InstrumentedType instrumentedType) {
      return instrumentedType;
    }

    @Override
    public ByteCodeAppender appender(Target target) {
      return (mv, context, method) -> {
        String ownerInternal = target.getInstrumentedType().getInternalName();
        String targetInternal = Type.getInternalName(targetClass);
        String targetDesc = Type.getDescriptor(targetClass);
        // Slots: 0=this, 1=rawValue, 2=out, 3=obj
        emitNonNullArgCheck(mv, targetClass);

        mv.visitVarInsn(Opcodes.ALOAD, 1);
        mv.visitTypeInsn(Opcodes.CHECKCAST, targetInternal);
        mv.visitVarInsn(Opcodes.ASTORE, 3);

        boolean nullableHeader = hasNullableFields && !staticEncoding;
        if (!nullableHeader) {
          // Slots: 4=pool, 5=buf, 6=pos, 7=ex
          String encDesc = "(" + targetDesc + OUT_DESC + "[BI)I";
          Label tryStart = new Label();
          Label tryEnd = new Label();
          Label catchHandler = new Label();
          mv.visitTryCatchBlock(tryStart, tryEnd, catchHandler, null);

          mv.visitMethodInsn(
              Opcodes.INVOKESTATIC, WIRE_CODEC, "scratchPool", "()" + SCRATCH_POOL_DESC, false);
          mv.visitVarInsn(Opcodes.ASTORE, 4);
          mv.visitVarInsn(Opcodes.ALOAD, 4);
          mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, SCRATCH_POOL, "acquire", "()[B", false);
          mv.visitVarInsn(Opcodes.ASTORE, 5);

          mv.visitLabel(tryStart);
          mv.visitInsn(Opcodes.ICONST_0);
          mv.visitVarInsn(Opcodes.ISTORE, 6);
          if (!staticEncoding) {
            pushInt(mv, count);
            mv.visitInsn(Opcodes.LCONST_0);
            emitPackCall(mv, "packHeader", "(IJ" + PACK_SUFFIX, 5, 6, 2);
          }
          for (int c = 0; c < chunkCount; c++) {
            mv.visitVarInsn(Opcodes.ALOAD, 0);
            mv.visitVarInsn(Opcodes.ALOAD, 3);
            mv.visitVarInsn(Opcodes.ALOAD, 2);
            mv.visitVarInsn(Opcodes.ALOAD, 5);
            mv.visitVarInsn(Opcodes.ILOAD, 6);
            mv.visitMethodInsn(Opcodes.INVOKESPECIAL, ownerInternal, "encC" + c, encDesc, false);
            mv.visitVarInsn(Opcodes.ISTORE, 6);
          }
          emitFlush(mv, 5, 6, 2);
          mv.visitLabel(tryEnd);

          mv.visitVarInsn(Opcodes.ALOAD, 4);
          mv.visitVarInsn(Opcodes.ALOAD, 5);
          mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, SCRATCH_POOL, "release", "([B)V", false);
          mv.visitInsn(Opcodes.RETURN);

          mv.visitLabel(catchHandler);
          mv.visitVarInsn(Opcodes.ASTORE, 7);
          mv.visitVarInsn(Opcodes.ALOAD, 4);
          mv.visitVarInsn(Opcodes.ALOAD, 5);
          mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, SCRATCH_POOL, "release", "([B)V", false);
          mv.visitVarInsn(Opcodes.ALOAD, 7);
          mv.visitInsn(Opcodes.ATHROW);
          return new ByteCodeAppender.Size(6, 8);
        }

        // Nullable wide encode (single ScratchPool buffer + optional RefScratchPool for beans):
        // Slots: 4=pool, 5=buf, 6=pos, 7=highestNullByteLen, 8=chunkMask
        //        If useRefs: 9=refPool, 10=refs, 11=ex; else 9=ex
        boolean useRefs = !isPojo && refCount > 0;
        int exSlot = useRefs ? 11 : 9;
        // For MAX_CHUNKED_DIRECT_FIELDS <= 512, nullByteLen <= 64 < 128 -> always 1 byte.
        int hdrLen = VarInt.getLength(count) + 1;

        Label tryStart = new Label();
        Label tryEnd = new Label();
        Label catchHandler = new Label();
        Label tryRefStart = new Label();
        Label tryRefEnd = new Label();
        Label catchBufOnly = new Label();
        if (useRefs) {
          mv.visitTryCatchBlock(tryRefStart, tryRefEnd, catchBufOnly, null);
        }
        mv.visitTryCatchBlock(tryStart, tryEnd, catchHandler, null);

        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC, WIRE_CODEC, "scratchPool", "()" + SCRATCH_POOL_DESC, false);
        mv.visitVarInsn(Opcodes.ASTORE, 4);
        mv.visitVarInsn(Opcodes.ALOAD, 4);
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, SCRATCH_POOL, "acquire", "()[B", false);
        mv.visitVarInsn(Opcodes.ASTORE, 5);

        if (useRefs) {
          mv.visitLabel(tryRefStart);
          mv.visitMethodInsn(
              Opcodes.INVOKESTATIC,
              WIRE_CODEC,
              "refScratchPool",
              "()" + REF_SCRATCH_POOL_DESC,
              false);
          mv.visitVarInsn(Opcodes.ASTORE, 9);
          mv.visitVarInsn(Opcodes.ALOAD, 9);
          pushInt(mv, refCount);
          mv.visitMethodInsn(
              Opcodes.INVOKEVIRTUAL, REF_SCRATCH_POOL, "acquire", "(I)[Ljava/lang/Object;", false);
          mv.visitVarInsn(Opcodes.ASTORE, 10);
          mv.visitLabel(tryRefEnd);
        }

        mv.visitLabel(tryStart);
        mv.visitInsn(Opcodes.ICONST_0);
        mv.visitVarInsn(Opcodes.ISTORE, 7);

        String beanScanDesc = "(" + targetDesc + "[Ljava/lang/Object;)I";
        String pojoScanDesc = "(" + targetDesc + ")I";
        for (int c = 0; c < chunkCount; c++) {
          if (chunkHasRefs[c]) {
            mv.visitVarInsn(Opcodes.ALOAD, 0);
            mv.visitVarInsn(Opcodes.ALOAD, 3);
            if (useRefs) {
              mv.visitVarInsn(Opcodes.ALOAD, 10);
              mv.visitMethodInsn(
                  Opcodes.INVOKESPECIAL, ownerInternal, "nullScanBeanC" + c, beanScanDesc, false);
            } else {
              mv.visitMethodInsn(
                  Opcodes.INVOKESPECIAL, ownerInternal, "nullScanC" + c, pojoScanDesc, false);
            }
            mv.visitVarInsn(Opcodes.ISTORE, 8);
            mv.visitVarInsn(Opcodes.ALOAD, 5);
            pushInt(mv, hdrLen);
            pushInt(mv, c);
            mv.visitVarInsn(Opcodes.ILOAD, 8);
            mv.visitVarInsn(Opcodes.ILOAD, 7);
          } else {
            mv.visitVarInsn(Opcodes.ALOAD, 5);
            pushInt(mv, hdrLen);
            pushInt(mv, c);
            mv.visitInsn(Opcodes.ICONST_0);
            mv.visitVarInsn(Opcodes.ILOAD, 7);
          }
          mv.visitMethodInsn(
              Opcodes.INVOKESTATIC, WIRE_CODEC, "putChunkNullMask32", "([BIIII)I", false);
          mv.visitVarInsn(Opcodes.ISTORE, 7);
        }

        // Patch VarInt(count) + 1-byte nullByteLen in-place at buf[0..hdrLen).
        mv.visitVarInsn(Opcodes.ALOAD, 5);
        pushInt(mv, count);
        mv.visitVarInsn(Opcodes.ILOAD, 7);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC, WIRE_CODEC, "patchWideNullHeaderPrefix", "([BII)I", false);
        mv.visitVarInsn(Opcodes.ISTORE, 6);

        String beanEncDesc = "(" + targetDesc + "[Ljava/lang/Object;" + OUT_DESC + "[BI)I";
        String pojoEncDesc = "(" + targetDesc + OUT_DESC + "[BI)I";
        for (int c = 0; c < chunkCount; c++) {
          mv.visitVarInsn(Opcodes.ALOAD, 0);
          mv.visitVarInsn(Opcodes.ALOAD, 3);
          if (useRefs && chunkHasRefs[c]) {
            mv.visitVarInsn(Opcodes.ALOAD, 10);
            mv.visitVarInsn(Opcodes.ALOAD, 2);
            mv.visitVarInsn(Opcodes.ALOAD, 5);
            mv.visitVarInsn(Opcodes.ILOAD, 6);
            mv.visitMethodInsn(
                Opcodes.INVOKESPECIAL, ownerInternal, "encBeanC" + c, beanEncDesc, false);
          } else {
            mv.visitVarInsn(Opcodes.ALOAD, 2);
            mv.visitVarInsn(Opcodes.ALOAD, 5);
            mv.visitVarInsn(Opcodes.ILOAD, 6);
            mv.visitMethodInsn(
                Opcodes.INVOKESPECIAL, ownerInternal, "encC" + c, pojoEncDesc, false);
          }
          mv.visitVarInsn(Opcodes.ISTORE, 6);
        }
        emitFlush(mv, 5, 6, 2);
        mv.visitLabel(tryEnd);

        mv.visitVarInsn(Opcodes.ALOAD, 4);
        mv.visitVarInsn(Opcodes.ALOAD, 5);
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, SCRATCH_POOL, "release", "([B)V", false);
        if (useRefs) {
          mv.visitVarInsn(Opcodes.ALOAD, 9);
          mv.visitVarInsn(Opcodes.ALOAD, 10);
          pushInt(mv, refCount);
          mv.visitMethodInsn(
              Opcodes.INVOKEVIRTUAL, REF_SCRATCH_POOL, "release", "([Ljava/lang/Object;I)V", false);
        }
        mv.visitInsn(Opcodes.RETURN);

        mv.visitLabel(catchHandler);
        mv.visitVarInsn(Opcodes.ASTORE, exSlot);
        mv.visitVarInsn(Opcodes.ALOAD, 4);
        mv.visitVarInsn(Opcodes.ALOAD, 5);
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, SCRATCH_POOL, "release", "([B)V", false);
        if (useRefs) {
          mv.visitVarInsn(Opcodes.ALOAD, 9);
          mv.visitVarInsn(Opcodes.ALOAD, 10);
          pushInt(mv, refCount);
          mv.visitMethodInsn(
              Opcodes.INVOKEVIRTUAL, REF_SCRATCH_POOL, "release", "([Ljava/lang/Object;I)V", false);
        }
        mv.visitVarInsn(Opcodes.ALOAD, exSlot);
        mv.visitInsn(Opcodes.ATHROW);

        if (useRefs) {
          mv.visitLabel(catchBufOnly);
          mv.visitVarInsn(Opcodes.ASTORE, exSlot);
          mv.visitVarInsn(Opcodes.ALOAD, 4);
          mv.visitVarInsn(Opcodes.ALOAD, 5);
          mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, SCRATCH_POOL, "release", "([B)V", false);
          mv.visitVarInsn(Opcodes.ALOAD, exSlot);
          mv.visitInsn(Opcodes.ATHROW);
        }
        return new ByteCodeAppender.Size(7, exSlot + 1);
      };
    }
  }

  private static final class DecodeChunkImpl implements Implementation {
    private final FieldSpec[] specs;
    private final int start;
    private final int end;
    private final boolean nullable;

    DecodeChunkImpl(FieldSpec[] specs, int start, int end, boolean nullable) {
      this.specs = specs;
      this.start = start;
      this.end = end;
      this.nullable = nullable;
    }

    @Override
    public InstrumentedType prepare(InstrumentedType instrumentedType) {
      return instrumentedType;
    }

    @Override
    public ByteCodeAppender appender(Target target) {
      return (mv, context, method) -> {
        // Instance slots: 0=this, 1=obj, 2=in, 3=buf [, 4=chunkMask]
        if (nullable) {
          for (int pos = start; pos < end; pos++) {
            int bit = pos - start;
            FieldSpec spec = specs[pos];
            Label isNull = new Label();
            Label after = new Label();
            mv.visitVarInsn(Opcodes.ILOAD, 4);
            pushInt(mv, 1 << bit);
            mv.visitInsn(Opcodes.IAND);
            mv.visitJumpInsn(Opcodes.IFNE, isNull);
            mv.visitVarInsn(Opcodes.ALOAD, 1);
            emitReadValueForDecode(mv, spec, 2, 3);
            emitWriteMember(mv, spec);
            mv.visitJumpInsn(Opcodes.GOTO, after);
            mv.visitLabel(isNull);
            if (spec.kind.isPrimitive()) {
              mv.visitLdcInsn(spec.schemaFieldName);
              mv.visitMethodInsn(
                  Opcodes.INVOKESTATIC,
                  BASE_TYPE,
                  "nullPrimitive",
                  "(Ljava/lang/String;)Ljava/lang/NullPointerException;",
                  false);
              mv.visitInsn(Opcodes.ATHROW);
            } else {
              // Explicitly write null to overwrite any non-null default field initializer in ctor.
              mv.visitVarInsn(Opcodes.ALOAD, 1);
              mv.visitInsn(Opcodes.ACONST_NULL);
              emitWriteMember(mv, spec);
            }
            mv.visitLabel(after);
          }
          mv.visitInsn(Opcodes.RETURN);
          return new ByteCodeAppender.Size(6, 5);
        }

        for (int pos = start; pos < end; pos++) {
          mv.visitVarInsn(Opcodes.ALOAD, 1);
          emitReadValueForDecode(mv, specs[pos], 2, 3);
          emitWriteMember(mv, specs[pos]);
        }
        mv.visitInsn(Opcodes.RETURN);
        return new ByteCodeAppender.Size(6, 4);
      };
    }
  }

  private static final class ChunkedDecodeImpl implements Implementation {
    private final Class<?> targetClass;
    private final int count;
    private final int chunkCount;
    private final boolean staticEncoding;
    private final boolean hasNullableFields;
    private final boolean needsScratch;

    ChunkedDecodeImpl(
        Class<?> targetClass,
        int count,
        int chunkCount,
        boolean staticEncoding,
        boolean hasNullableFields,
        boolean needsScratch) {
      this.targetClass = targetClass;
      this.count = count;
      this.chunkCount = chunkCount;
      this.staticEncoding = staticEncoding;
      this.hasNullableFields = hasNullableFields;
      this.needsScratch = needsScratch;
    }

    @Override
    public InstrumentedType prepare(InstrumentedType instrumentedType) {
      return instrumentedType;
    }

    @Override
    public ByteCodeAppender appender(Target target) {
      return (mv, context, method) -> {
        String ownerInternal = target.getInstrumentedType().getInternalName();
        String targetInternal = Type.getInternalName(targetClass);
        String targetDesc = Type.getDescriptor(targetClass);
        String noNullDesc = "(" + targetDesc + IN_DESC + "[B)V";
        String nullDesc = "(" + targetDesc + IN_DESC + "[BI)V";

        if (staticEncoding) {
          if (!needsScratch) {
            // Slots: 0=this, 1=in, 2=res
            emitNewTarget(mv, targetInternal, 2);
            for (int c = 0; c < chunkCount; c++) {
              mv.visitVarInsn(Opcodes.ALOAD, 0);
              mv.visitVarInsn(Opcodes.ALOAD, 2);
              mv.visitVarInsn(Opcodes.ALOAD, 1);
              mv.visitInsn(Opcodes.ACONST_NULL);
              mv.visitMethodInsn(
                  Opcodes.INVOKESPECIAL, ownerInternal, "decNoNullC" + c, noNullDesc, false);
            }
            mv.visitVarInsn(Opcodes.ALOAD, 2);
            mv.visitInsn(Opcodes.ARETURN);
            return new ByteCodeAppender.Size(5, 3);
          }
          // Slots: 0=this, 1=in, 2=res, 3=pool, 4=buf, 5=ex
          Label tryStart = new Label();
          Label tryEnd = new Label();
          Label catchHandler = new Label();
          mv.visitTryCatchBlock(tryStart, tryEnd, catchHandler, null);

          mv.visitMethodInsn(
              Opcodes.INVOKESTATIC, WIRE_CODEC, "scratchPool", "()" + SCRATCH_POOL_DESC, false);
          mv.visitVarInsn(Opcodes.ASTORE, 3);
          mv.visitVarInsn(Opcodes.ALOAD, 3);
          mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, SCRATCH_POOL, "acquire", "()[B", false);
          mv.visitVarInsn(Opcodes.ASTORE, 4);

          mv.visitLabel(tryStart);
          emitNewTarget(mv, targetInternal, 2);
          for (int c = 0; c < chunkCount; c++) {
            mv.visitVarInsn(Opcodes.ALOAD, 0);
            mv.visitVarInsn(Opcodes.ALOAD, 2);
            mv.visitVarInsn(Opcodes.ALOAD, 1);
            mv.visitVarInsn(Opcodes.ALOAD, 4);
            mv.visitMethodInsn(
                Opcodes.INVOKESPECIAL, ownerInternal, "decNoNullC" + c, noNullDesc, false);
          }
          mv.visitLabel(tryEnd);

          mv.visitVarInsn(Opcodes.ALOAD, 3);
          mv.visitVarInsn(Opcodes.ALOAD, 4);
          mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, SCRATCH_POOL, "release", "([B)V", false);
          mv.visitVarInsn(Opcodes.ALOAD, 2);
          mv.visitInsn(Opcodes.ARETURN);

          mv.visitLabel(catchHandler);
          mv.visitVarInsn(Opcodes.ASTORE, 5);
          mv.visitVarInsn(Opcodes.ALOAD, 3);
          mv.visitVarInsn(Opcodes.ALOAD, 4);
          mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, SCRATCH_POOL, "release", "([B)V", false);
          mv.visitVarInsn(Opcodes.ALOAD, 5);
          mv.visitInsn(Opcodes.ATHROW);
          return new ByteCodeAppender.Size(6, 6);
        }

        // Slots: 0=this, 1=in, 2=res, 3=streamFieldCount, 4=nullByteLen, 5=pool, 6=buf,
        //        7=nullScratch, 8=nullBytes, 9=chunkMask, 10=ex
        mv.visitVarInsn(Opcodes.ALOAD, 1);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC, WIRE_CODEC, "readVarInt", "(" + IN_DESC + ")I", false);
        mv.visitVarInsn(Opcodes.ISTORE, 3);

        Label matchingCount = new Label();
        mv.visitVarInsn(Opcodes.ILOAD, 3);
        pushInt(mv, count);
        mv.visitJumpInsn(Opcodes.IF_ICMPEQ, matchingCount);
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitVarInsn(Opcodes.ILOAD, 3);
        mv.visitVarInsn(Opcodes.ALOAD, 1);
        mv.visitMethodInsn(
            Opcodes.INVOKEVIRTUAL,
            BASE_TYPE,
            "decodeEvolved",
            "(I" + IN_DESC + ")Ljava/lang/Object;",
            false);
        mv.visitInsn(Opcodes.ARETURN);
        mv.visitLabel(matchingCount);

        mv.visitVarInsn(Opcodes.ALOAD, 1);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC, WIRE_CODEC, "readVarInt", "(" + IN_DESC + ")I", false);
        mv.visitVarInsn(Opcodes.ISTORE, 4);

        Label badNullLen = new Label();
        mv.visitVarInsn(Opcodes.ILOAD, 4);
        mv.visitJumpInsn(Opcodes.IFLT, badNullLen);

        Label hasNulls = new Label();
        mv.visitVarInsn(Opcodes.ILOAD, 4);
        mv.visitJumpInsn(Opcodes.IFNE, hasNulls);

        // nullByteLen == 0 fast path: skip ScratchPool completely when !needsScratch.
        if (!needsScratch) {
          emitNewTarget(mv, targetInternal, 2);
          for (int c = 0; c < chunkCount; c++) {
            mv.visitVarInsn(Opcodes.ALOAD, 0);
            mv.visitVarInsn(Opcodes.ALOAD, 2);
            mv.visitVarInsn(Opcodes.ALOAD, 1);
            mv.visitInsn(Opcodes.ACONST_NULL);
            mv.visitMethodInsn(
                Opcodes.INVOKESPECIAL, ownerInternal, "decNoNullC" + c, noNullDesc, false);
          }
          mv.visitVarInsn(Opcodes.ALOAD, 2);
          mv.visitInsn(Opcodes.ARETURN);
        } else {
          Label tryStart0 = new Label();
          Label tryEnd0 = new Label();
          Label catch0 = new Label();
          mv.visitTryCatchBlock(tryStart0, tryEnd0, catch0, null);

          mv.visitMethodInsn(
              Opcodes.INVOKESTATIC, WIRE_CODEC, "scratchPool", "()" + SCRATCH_POOL_DESC, false);
          mv.visitVarInsn(Opcodes.ASTORE, 5);
          mv.visitVarInsn(Opcodes.ALOAD, 5);
          mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, SCRATCH_POOL, "acquire", "()[B", false);
          mv.visitVarInsn(Opcodes.ASTORE, 6);

          mv.visitLabel(tryStart0);
          emitNewTarget(mv, targetInternal, 2);
          for (int c = 0; c < chunkCount; c++) {
            mv.visitVarInsn(Opcodes.ALOAD, 0);
            mv.visitVarInsn(Opcodes.ALOAD, 2);
            mv.visitVarInsn(Opcodes.ALOAD, 1);
            mv.visitVarInsn(Opcodes.ALOAD, 6);
            mv.visitMethodInsn(
                Opcodes.INVOKESPECIAL, ownerInternal, "decNoNullC" + c, noNullDesc, false);
          }
          mv.visitLabel(tryEnd0);

          mv.visitVarInsn(Opcodes.ALOAD, 5);
          mv.visitVarInsn(Opcodes.ALOAD, 6);
          mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, SCRATCH_POOL, "release", "([B)V", false);
          mv.visitVarInsn(Opcodes.ALOAD, 2);
          mv.visitInsn(Opcodes.ARETURN);

          mv.visitLabel(catch0);
          mv.visitVarInsn(Opcodes.ASTORE, 10);
          mv.visitVarInsn(Opcodes.ALOAD, 5);
          mv.visitVarInsn(Opcodes.ALOAD, 6);
          mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, SCRATCH_POOL, "release", "([B)V", false);
          mv.visitVarInsn(Opcodes.ALOAD, 10);
          mv.visitInsn(Opcodes.ATHROW);
        }

        mv.visitLabel(hasNulls);
        if (!hasNullableFields) {
          mv.visitVarInsn(Opcodes.ALOAD, 0);
          mv.visitVarInsn(Opcodes.ILOAD, 3);
          mv.visitVarInsn(Opcodes.ILOAD, 4);
          mv.visitVarInsn(Opcodes.ALOAD, 1);
          mv.visitMethodInsn(
              Opcodes.INVOKEVIRTUAL,
              BASE_TYPE,
              "decodeEvolvedAfterNullLen",
              "(II" + IN_DESC + ")Ljava/lang/Object;",
              false);
          mv.visitInsn(Opcodes.ARETURN);
        } else {
          Label tryStart1 = new Label();
          Label tryEnd1 = new Label();
          Label catch1 = new Label();
          Label tryBufAcqStart = new Label();
          Label tryBufAcqEnd = new Label();
          Label catchNullOnly = new Label();
          if (needsScratch) {
            mv.visitTryCatchBlock(tryBufAcqStart, tryBufAcqEnd, catchNullOnly, null);
          }
          mv.visitTryCatchBlock(tryStart1, tryEnd1, catch1, null);

          mv.visitMethodInsn(
              Opcodes.INVOKESTATIC, WIRE_CODEC, "scratchPool", "()" + SCRATCH_POOL_DESC, false);
          mv.visitVarInsn(Opcodes.ASTORE, 5);
          mv.visitVarInsn(Opcodes.ALOAD, 5);
          mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, SCRATCH_POOL, "acquire", "()[B", false);
          mv.visitVarInsn(Opcodes.ASTORE, 7);
          if (needsScratch) {
            mv.visitLabel(tryBufAcqStart);
            mv.visitVarInsn(Opcodes.ALOAD, 5);
            mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, SCRATCH_POOL, "acquire", "()[B", false);
            mv.visitVarInsn(Opcodes.ASTORE, 6);
            mv.visitLabel(tryBufAcqEnd);
          } else {
            mv.visitInsn(Opcodes.ACONST_NULL);
            mv.visitVarInsn(Opcodes.ASTORE, 6);
          }

          mv.visitLabel(tryStart1);
          mv.visitVarInsn(Opcodes.ALOAD, 1);
          mv.visitVarInsn(Opcodes.ILOAD, 4);
          mv.visitVarInsn(Opcodes.ALOAD, 7);
          mv.visitMethodInsn(
              Opcodes.INVOKESTATIC, WIRE_CODEC, "readNullBytes", "(" + IN_DESC + "I[B)[B", false);
          mv.visitVarInsn(Opcodes.ASTORE, 8);

          emitNewTarget(mv, targetInternal, 2);
          for (int c = 0; c < chunkCount; c++) {
            Label chunkHasNulls = new Label();
            Label nextChunk = new Label();
            mv.visitVarInsn(Opcodes.ALOAD, 8);
            mv.visitVarInsn(Opcodes.ILOAD, 4);
            pushInt(mv, c);
            mv.visitMethodInsn(
                Opcodes.INVOKESTATIC, WIRE_CODEC, "extractChunkNullMask32", "([BII)I", false);
            mv.visitVarInsn(Opcodes.ISTORE, 9);
            mv.visitVarInsn(Opcodes.ILOAD, 9);
            mv.visitJumpInsn(Opcodes.IFNE, chunkHasNulls);
            mv.visitVarInsn(Opcodes.ALOAD, 0);
            mv.visitVarInsn(Opcodes.ALOAD, 2);
            mv.visitVarInsn(Opcodes.ALOAD, 1);
            mv.visitVarInsn(Opcodes.ALOAD, 6);
            mv.visitMethodInsn(
                Opcodes.INVOKESPECIAL, ownerInternal, "decNoNullC" + c, noNullDesc, false);
            mv.visitJumpInsn(Opcodes.GOTO, nextChunk);
            mv.visitLabel(chunkHasNulls);
            mv.visitVarInsn(Opcodes.ALOAD, 0);
            mv.visitVarInsn(Opcodes.ALOAD, 2);
            mv.visitVarInsn(Opcodes.ALOAD, 1);
            mv.visitVarInsn(Opcodes.ALOAD, 6);
            mv.visitVarInsn(Opcodes.ILOAD, 9);
            mv.visitMethodInsn(
                Opcodes.INVOKESPECIAL, ownerInternal, "decNullC" + c, nullDesc, false);
            mv.visitLabel(nextChunk);
          }
          mv.visitLabel(tryEnd1);

          if (needsScratch) {
            mv.visitVarInsn(Opcodes.ALOAD, 5);
            mv.visitVarInsn(Opcodes.ALOAD, 6);
            mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, SCRATCH_POOL, "release", "([B)V", false);
          }
          mv.visitVarInsn(Opcodes.ALOAD, 5);
          mv.visitVarInsn(Opcodes.ALOAD, 7);
          mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, SCRATCH_POOL, "release", "([B)V", false);
          mv.visitVarInsn(Opcodes.ALOAD, 2);
          mv.visitInsn(Opcodes.ARETURN);

          mv.visitLabel(catch1);
          mv.visitVarInsn(Opcodes.ASTORE, 10);
          if (needsScratch) {
            mv.visitVarInsn(Opcodes.ALOAD, 5);
            mv.visitVarInsn(Opcodes.ALOAD, 6);
            mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, SCRATCH_POOL, "release", "([B)V", false);
          }
          mv.visitVarInsn(Opcodes.ALOAD, 5);
          mv.visitVarInsn(Opcodes.ALOAD, 7);
          mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, SCRATCH_POOL, "release", "([B)V", false);
          mv.visitVarInsn(Opcodes.ALOAD, 10);
          mv.visitInsn(Opcodes.ATHROW);

          if (needsScratch) {
            mv.visitLabel(catchNullOnly);
            mv.visitVarInsn(Opcodes.ASTORE, 10);
            mv.visitVarInsn(Opcodes.ALOAD, 5);
            mv.visitVarInsn(Opcodes.ALOAD, 7);
            mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, SCRATCH_POOL, "release", "([B)V", false);
            mv.visitVarInsn(Opcodes.ALOAD, 10);
            mv.visitInsn(Opcodes.ATHROW);
          }
        }

        mv.visitLabel(badNullLen);
        mv.visitVarInsn(Opcodes.ILOAD, 4);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC,
            BASE_TYPE,
            "invalidNullBitmapLength",
            "(I)Ljava/io/IOException;",
            false);
        mv.visitInsn(Opcodes.ATHROW);
        return new ByteCodeAppender.Size(8, 11);
      };
    }
  }

  private static final class IsCheapChunkImpl implements Implementation {
    private final FieldSpec[] specs;
    private final int start;
    private final int end;

    IsCheapChunkImpl(FieldSpec[] specs, int start, int end) {
      this.specs = specs;
      this.start = start;
      this.end = end;
    }

    @Override
    public InstrumentedType prepare(InstrumentedType instrumentedType) {
      return instrumentedType;
    }

    @Override
    public ByteCodeAppender appender(Target target) {
      return (mv, context, method) -> {
        // Instance slots: 0=this, 1=obj, 2=tempRef
        emitIsCheapRange(mv, specs, start, end, 1, 2);
        mv.visitInsn(Opcodes.ICONST_1);
        mv.visitInsn(Opcodes.IRETURN);
        return new ByteCodeAppender.Size(4, 3);
      };
    }
  }

  private static final class ChunkedIsCheapImpl implements Implementation {
    private final Class<?> targetClass;
    private final boolean[] chunkNeedsIsCheap;
    private final boolean anyNeedsIsCheap;

    ChunkedIsCheapImpl(Class<?> targetClass, boolean[] chunkNeedsIsCheap, boolean anyNeedsIsCheap) {
      this.targetClass = targetClass;
      this.chunkNeedsIsCheap = chunkNeedsIsCheap;
      this.anyNeedsIsCheap = anyNeedsIsCheap;
    }

    @Override
    public InstrumentedType prepare(InstrumentedType instrumentedType) {
      return instrumentedType;
    }

    @Override
    public ByteCodeAppender appender(Target target) {
      return (mv, context, method) -> {
        // Slots: 0=this, 1=rawValue [, 2=obj]
        Label nonNull = new Label();
        mv.visitVarInsn(Opcodes.ALOAD, 1);
        mv.visitJumpInsn(Opcodes.IFNONNULL, nonNull);
        mv.visitInsn(Opcodes.ICONST_0);
        mv.visitInsn(Opcodes.IRETURN);
        mv.visitLabel(nonNull);

        if (!anyNeedsIsCheap) {
          mv.visitInsn(Opcodes.ICONST_1);
          mv.visitInsn(Opcodes.IRETURN);
          return new ByteCodeAppender.Size(2, 2);
        }

        String ownerInternal = target.getInstrumentedType().getInternalName();
        String targetDesc = Type.getDescriptor(targetClass);
        String chunkDesc = "(" + targetDesc + ")Z";

        mv.visitVarInsn(Opcodes.ALOAD, 1);
        mv.visitTypeInsn(Opcodes.CHECKCAST, Type.getInternalName(targetClass));
        mv.visitVarInsn(Opcodes.ASTORE, 2);

        for (int c = 0; c < chunkNeedsIsCheap.length; c++) {
          if (!chunkNeedsIsCheap[c]) {
            continue;
          }
          Label ok = new Label();
          mv.visitVarInsn(Opcodes.ALOAD, 0);
          mv.visitVarInsn(Opcodes.ALOAD, 2);
          mv.visitMethodInsn(
              Opcodes.INVOKESPECIAL, ownerInternal, "isCheapC" + c, chunkDesc, false);
          mv.visitJumpInsn(Opcodes.IFNE, ok);
          mv.visitInsn(Opcodes.ICONST_0);
          mv.visitInsn(Opcodes.IRETURN);
          mv.visitLabel(ok);
        }
        mv.visitInsn(Opcodes.ICONST_1);
        mv.visitInsn(Opcodes.IRETURN);
        return new ByteCodeAppender.Size(3, 3);
      };
    }
  }

  private static final class ByteSizeChunkImpl implements Implementation {
    private final FieldSpec[] specs;
    private final int start;
    private final int end;
    private final boolean trackNulls;

    ByteSizeChunkImpl(FieldSpec[] specs, int start, int end, boolean trackNulls) {
      this.specs = specs;
      this.start = start;
      this.end = end;
      this.trackNulls = trackNulls;
    }

    @Override
    public InstrumentedType prepare(InstrumentedType instrumentedType) {
      return instrumentedType;
    }

    @Override
    public ByteCodeAppender appender(Target target) {
      return (mv, context, method) -> {
        // Instance slots: 0=this, 1=obj, 2=observer, 3..4=chunkBytes, 5=tempRef [, 6=localNullTag]
        // When trackNulls is true, returns (chunkBytes << 6) | localNullTag where localNullTag is
        // 0 (no null in chunk) or 1..32 ((pos - start) + 1), eliminating any heap allocation.
        mv.visitInsn(Opcodes.LCONST_0);
        mv.visitVarInsn(Opcodes.LSTORE, 3);
        if (trackNulls) {
          mv.visitInsn(Opcodes.ICONST_0);
          mv.visitVarInsn(Opcodes.ISTORE, 6);
        }
        for (int pos = start; pos < end; pos++) {
          FieldSpec spec = specs[pos];
          switch (spec.kind) {
            case PRIM_BYTE:
            case PRIM_BOOL:
            case PRIM_SHORT:
            case PRIM_FLOAT:
            case PRIM_DOUBLE:
              break;
            case PRIM_INT:
              mv.visitVarInsn(Opcodes.LLOAD, 3);
              emitReadMember(mv, 1, spec);
              mv.visitMethodInsn(Opcodes.INVOKESTATIC, VARINT_TYPE, "getLength", "(I)I", false);
              mv.visitInsn(Opcodes.I2L);
              mv.visitInsn(Opcodes.LADD);
              mv.visitVarInsn(Opcodes.LSTORE, 3);
              break;
            case PRIM_LONG:
              mv.visitVarInsn(Opcodes.LLOAD, 3);
              emitReadMember(mv, 1, spec);
              mv.visitMethodInsn(Opcodes.INVOKESTATIC, VARINT_TYPE, "getLength", "(J)I", false);
              mv.visitInsn(Opcodes.I2L);
              mv.visitInsn(Opcodes.LADD);
              mv.visitVarInsn(Opcodes.LSTORE, 3);
              break;
            default:
              emitReadMember(mv, 1, spec);
              mv.visitVarInsn(Opcodes.ASTORE, 5);
              Label notNull = new Label();
              mv.visitVarInsn(Opcodes.ALOAD, 5);
              mv.visitJumpInsn(Opcodes.IFNONNULL, notNull);
              if (!spec.nullable) {
                emitThrowNullNonNullable(mv, spec.rowIndex);
                mv.visitLabel(notNull);
                ByteSizeImpl.emitAddRefSize(mv, spec, 5, 3, 2);
              } else {
                Label after = new Label();
                if (trackNulls) {
                  pushInt(mv, (pos - start) + 1);
                  mv.visitVarInsn(Opcodes.ISTORE, 6);
                }
                mv.visitJumpInsn(Opcodes.GOTO, after);
                mv.visitLabel(notNull);
                ByteSizeImpl.emitAddRefSize(mv, spec, 5, 3, 2);
                mv.visitLabel(after);
              }
              break;
          }
        }
        if (!trackNulls) {
          mv.visitVarInsn(Opcodes.LLOAD, 3);
          mv.visitInsn(Opcodes.LRETURN);
          return new ByteCodeAppender.Size(6, 6);
        }

        // Overflow guard: if chunkBytes >= (1L << 57), flush directly to observer before packing.
        Label fits = new Label();
        mv.visitVarInsn(Opcodes.LLOAD, 3);
        mv.visitLdcInsn(1L << 57);
        mv.visitInsn(Opcodes.LCMP);
        mv.visitJumpInsn(Opcodes.IFLT, fits);
        mv.visitVarInsn(Opcodes.ALOAD, 2);
        mv.visitVarInsn(Opcodes.LLOAD, 3);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC, "java/lang/Long", "valueOf", "(J)Ljava/lang/Long;", false);
        mv.visitMethodInsn(
            Opcodes.INVOKEVIRTUAL, OBSERVER_TYPE, "update", "(Ljava/lang/Object;)V", false);
        mv.visitInsn(Opcodes.LCONST_0);
        mv.visitVarInsn(Opcodes.LSTORE, 3);
        mv.visitLabel(fits);

        mv.visitVarInsn(Opcodes.LLOAD, 3);
        pushInt(mv, 6);
        mv.visitInsn(Opcodes.LSHL);
        mv.visitVarInsn(Opcodes.ILOAD, 6);
        mv.visitInsn(Opcodes.I2L);
        mv.visitInsn(Opcodes.LOR);
        mv.visitInsn(Opcodes.LRETURN);
        return new ByteCodeAppender.Size(6, 7);
      };
    }
  }

  private static final class ChunkedByteSizeImpl implements Implementation {
    private final Class<?> targetClass;
    private final FieldSpec[] specs;
    private final int chunkCount;
    private final boolean staticEncoding;
    private final boolean hasNullableFields;
    private final boolean[] chunkNeedsByteSize;
    private final boolean[] chunkHasNullable;

    ChunkedByteSizeImpl(
        Class<?> targetClass,
        FieldSpec[] specs,
        int chunkCount,
        boolean staticEncoding,
        boolean hasNullableFields,
        boolean[] chunkNeedsByteSize,
        boolean[] chunkHasNullable) {
      this.targetClass = targetClass;
      this.specs = specs;
      this.chunkCount = chunkCount;
      this.staticEncoding = staticEncoding;
      this.hasNullableFields = hasNullableFields;
      this.chunkNeedsByteSize = chunkNeedsByteSize;
      this.chunkHasNullable = chunkHasNullable;
    }

    @Override
    public InstrumentedType prepare(InstrumentedType instrumentedType) {
      return instrumentedType;
    }

    @Override
    public ByteCodeAppender appender(Target target) {
      return (mv, context, method) -> {
        String ownerInternal = target.getInstrumentedType().getInternalName();
        String targetDesc = Type.getDescriptor(targetClass);
        String chunkDesc = "(" + targetDesc + OBSERVER_DESC + ")J";
        // Slots: 0=this, 1=rawValue, 2=observer, 3=obj, 4..5=total, 6=highestNull,
        //        7..8=packed, 9=tag
        emitNonNullArgCheck(mv, targetClass);

        mv.visitVarInsn(Opcodes.ALOAD, 1);
        mv.visitTypeInsn(Opcodes.CHECKCAST, Type.getInternalName(targetClass));
        mv.visitVarInsn(Opcodes.ASTORE, 3);

        long constBytes = computeFixedConstBytes(specs, staticEncoding, hasNullableFields);
        mv.visitLdcInsn(constBytes);
        mv.visitVarInsn(Opcodes.LSTORE, 4);

        boolean trackNullsOverall = hasNullableFields && !staticEncoding;
        if (trackNullsOverall) {
          mv.visitInsn(Opcodes.ICONST_M1);
          mv.visitVarInsn(Opcodes.ISTORE, 6);
        }

        for (int c = 0; c < chunkCount; c++) {
          if (!chunkNeedsByteSize[c]) {
            continue;
          }
          if (trackNullsOverall && chunkHasNullable[c]) {
            mv.visitVarInsn(Opcodes.ALOAD, 0);
            mv.visitVarInsn(Opcodes.ALOAD, 3);
            mv.visitVarInsn(Opcodes.ALOAD, 2);
            mv.visitMethodInsn(Opcodes.INVOKESPECIAL, ownerInternal, "sizeC" + c, chunkDesc, false);
            mv.visitVarInsn(Opcodes.LSTORE, 7);

            mv.visitVarInsn(Opcodes.LLOAD, 4);
            mv.visitVarInsn(Opcodes.LLOAD, 7);
            pushInt(mv, 6);
            mv.visitInsn(Opcodes.LUSHR);
            mv.visitInsn(Opcodes.LADD);
            mv.visitVarInsn(Opcodes.LSTORE, 4);

            mv.visitVarInsn(Opcodes.LLOAD, 7);
            mv.visitLdcInsn(0x3FL);
            mv.visitInsn(Opcodes.LAND);
            mv.visitInsn(Opcodes.L2I);
            mv.visitVarInsn(Opcodes.ISTORE, 9);
            Label noChunkNull = new Label();
            mv.visitVarInsn(Opcodes.ILOAD, 9);
            mv.visitJumpInsn(Opcodes.IFEQ, noChunkNull);
            pushInt(mv, c * DIRECT_CHUNK_SIZE - 1);
            mv.visitVarInsn(Opcodes.ILOAD, 9);
            mv.visitInsn(Opcodes.IADD);
            mv.visitVarInsn(Opcodes.ISTORE, 6);
            mv.visitLabel(noChunkNull);
          } else {
            mv.visitVarInsn(Opcodes.LLOAD, 4);
            mv.visitVarInsn(Opcodes.ALOAD, 0);
            mv.visitVarInsn(Opcodes.ALOAD, 3);
            mv.visitVarInsn(Opcodes.ALOAD, 2);
            mv.visitMethodInsn(Opcodes.INVOKESPECIAL, ownerInternal, "sizeC" + c, chunkDesc, false);
            mv.visitInsn(Opcodes.LADD);
            mv.visitVarInsn(Opcodes.LSTORE, 4);
          }
        }

        if (trackNullsOverall) {
          mv.visitVarInsn(Opcodes.LLOAD, 4);
          mv.visitVarInsn(Opcodes.ILOAD, 6);
          mv.visitMethodInsn(Opcodes.INVOKESTATIC, WIRE_CODEC, "sizeOfNullMask", "(I)J", false);
          mv.visitInsn(Opcodes.LADD);
          mv.visitVarInsn(Opcodes.LSTORE, 4);
        }

        mv.visitVarInsn(Opcodes.ALOAD, 2);
        mv.visitVarInsn(Opcodes.LLOAD, 4);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC, "java/lang/Long", "valueOf", "(J)Ljava/lang/Long;", false);
        mv.visitMethodInsn(
            Opcodes.INVOKEVIRTUAL, OBSERVER_TYPE, "update", "(Ljava/lang/Object;)V", false);
        mv.visitInsn(Opcodes.RETURN);
        return new ByteCodeAppender.Size(6, 10);
      };
    }
  }
}
