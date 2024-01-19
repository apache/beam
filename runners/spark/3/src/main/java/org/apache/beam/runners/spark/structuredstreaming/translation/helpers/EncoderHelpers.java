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
package org.apache.beam.runners.spark.structuredstreaming.translation.helpers;

import static org.apache.beam.runners.spark.structuredstreaming.translation.helpers.EncoderFactory.invoke;
import static org.apache.beam.runners.spark.structuredstreaming.translation.helpers.EncoderFactory.invokeIfNotNull;
import static org.apache.beam.runners.spark.structuredstreaming.translation.helpers.EncoderFactory.newInstance;
import static org.apache.beam.runners.spark.structuredstreaming.translation.utils.ScalaInterop.match;
import static org.apache.beam.runners.spark.structuredstreaming.translation.utils.ScalaInterop.replace;
import static org.apache.beam.runners.spark.structuredstreaming.translation.utils.ScalaInterop.seqOf;
import static org.apache.beam.runners.spark.structuredstreaming.translation.utils.ScalaInterop.tuple;
import static org.apache.spark.sql.types.DataTypes.BinaryType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.LongType;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow.IntervalWindowCoder;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.PaneInfo.PaneInfoCoder;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.catalyst.SerializerBuildHelper;
import org.apache.spark.sql.catalyst.SerializerBuildHelper.MapElementInformation;
import org.apache.spark.sql.catalyst.analysis.GetColumnByOrdinal;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.expressions.BoundReference;
import org.apache.spark.sql.catalyst.expressions.Coalesce;
import org.apache.spark.sql.catalyst.expressions.CreateNamedStruct;
import org.apache.spark.sql.catalyst.expressions.EqualTo;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.GetStructField;
import org.apache.spark.sql.catalyst.expressions.If;
import org.apache.spark.sql.catalyst.expressions.IsNotNull;
import org.apache.spark.sql.catalyst.expressions.IsNull;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.catalyst.expressions.Literal$;
import org.apache.spark.sql.catalyst.expressions.MapKeys;
import org.apache.spark.sql.catalyst.expressions.MapValues;
import org.apache.spark.sql.catalyst.expressions.objects.MapObjects$;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.ObjectType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.MutablePair;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import scala.Option;
import scala.Some;
import scala.Tuple2;
import scala.collection.IndexedSeq;
import scala.collection.JavaConverters;
import scala.collection.Seq;

/** {@link Encoders} utility class. */
public class EncoderHelpers {
  private static final DataType OBJECT_TYPE = new ObjectType(Object.class);
  private static final DataType TUPLE2_TYPE = new ObjectType(Tuple2.class);
  private static final DataType WINDOWED_VALUE = new ObjectType(WindowedValue.class);
  private static final DataType KV_TYPE = new ObjectType(KV.class);
  private static final DataType MUTABLE_PAIR_TYPE = new ObjectType(MutablePair.class);
  private static final DataType LIST_TYPE = new ObjectType(List.class);

  // Collections / maps of these types can be (de)serialized without (de)serializing each member
  private static final Set<Class<?>> PRIMITIV_TYPES =
      ImmutableSet.of(
          Boolean.class,
          Byte.class,
          Short.class,
          Integer.class,
          Long.class,
          Float.class,
          Double.class);

  // Default encoders by class
  private static final Map<Class<?>, Encoder<?>> DEFAULT_ENCODERS = new ConcurrentHashMap<>();

  // Factory for default encoders by class
  private static final Function<Class<?>, @Nullable Encoder<?>> ENCODER_FACTORY =
      cls -> {
        if (cls.equals(PaneInfo.class)) {
          return paneInfoEncoder();
        } else if (cls.equals(GlobalWindow.class)) {
          return binaryEncoder(GlobalWindow.Coder.INSTANCE, false);
        } else if (cls.equals(IntervalWindow.class)) {
          return binaryEncoder(IntervalWindowCoder.of(), false);
        } else if (cls.equals(Instant.class)) {
          return instantEncoder();
        } else if (cls.equals(String.class)) {
          return Encoders.STRING();
        } else if (cls.equals(Boolean.class)) {
          return Encoders.BOOLEAN();
        } else if (cls.equals(Integer.class)) {
          return Encoders.INT();
        } else if (cls.equals(Long.class)) {
          return Encoders.LONG();
        } else if (cls.equals(Float.class)) {
          return Encoders.FLOAT();
        } else if (cls.equals(Double.class)) {
          return Encoders.DOUBLE();
        } else if (cls.equals(BigDecimal.class)) {
          return Encoders.DECIMAL();
        } else if (cls.equals(byte[].class)) {
          return Encoders.BINARY();
        } else if (cls.equals(Byte.class)) {
          return Encoders.BYTE();
        } else if (cls.equals(Short.class)) {
          return Encoders.SHORT();
        }
        return null;
      };

  private static <T> @Nullable Encoder<T> getOrCreateDefaultEncoder(Class<? super T> cls) {
    return (Encoder<T>) DEFAULT_ENCODERS.computeIfAbsent(cls, ENCODER_FACTORY);
  }

  /** Gets or creates a default {@link Encoder} for {@link T}. */
  public static <T> Encoder<T> encoderOf(Class<? super T> cls) {
    Encoder<T> enc = getOrCreateDefaultEncoder(cls);
    if (enc == null) {
      throw new IllegalArgumentException("No default coder available for class " + cls);
    }
    return enc;
  }

  /**
   * Creates a Spark {@link Encoder} for {@link T} of {@link DataTypes#BinaryType BinaryType}
   * delegating to a Beam {@link Coder} underneath.
   *
   * <p>Note: For common types, if available, default Spark {@link Encoder}s are used instead.
   *
   * @param coder Beam {@link Coder}
   */
  public static <T> Encoder<T> encoderFor(Coder<T> coder) {
    Encoder<T> enc = getOrCreateDefaultEncoder(coder.getEncodedTypeDescriptor().getRawType());
    return enc != null ? enc : binaryEncoder(coder, true);
  }

  /**
   * Creates a Spark {@link Encoder} for {@link T} of {@link StructType} with fields {@code value},
   * {@code timestamp}, {@code windows} and {@code pane}.
   *
   * @param value {@link Encoder} to encode field `{@code value}`.
   * @param window {@link Encoder} to encode individual windows in field `{@code windows}`
   */
  public static <T, W extends BoundedWindow> Encoder<WindowedValue<T>> windowedValueEncoder(
      Encoder<T> value, Encoder<W> window) {
    Encoder<Instant> timestamp = encoderOf(Instant.class);
    Encoder<PaneInfo> pane = encoderOf(PaneInfo.class);
    Encoder<Collection<W>> windows = collectionEncoder(window);
    Expression serializer =
        serializeWindowedValue(rootRef(WINDOWED_VALUE, true), value, timestamp, windows, pane);
    Expression deserializer =
        deserializeWindowedValue(rootCol(serializer.dataType()), value, timestamp, windows, pane);
    return EncoderFactory.create(serializer, deserializer, WindowedValue.class);
  }

  /**
   * Creates a one-of Spark {@link Encoder} of {@link StructType} where each alternative is
   * represented as colum / field named by its index with a separate {@link Encoder} each.
   *
   * <p>Externally this is represented as tuple {@code (index, data)} where an index corresponds to
   * an {@link Encoder} in the provided list.
   *
   * @param encoders {@link Encoder}s for each alternative.
   */
  public static <T> Encoder<Tuple2<Integer, T>> oneOfEncoder(List<Encoder<T>> encoders) {
    Expression serializer = serializeOneOf(rootRef(TUPLE2_TYPE, true), encoders);
    Expression deserializer = deserializeOneOf(rootCol(serializer.dataType()), encoders);
    return EncoderFactory.create(serializer, deserializer, Tuple2.class);
  }

  /**
   * Creates a Spark {@link Encoder} for {@link KV} of {@link StructType} with fields {@code key}
   * and {@code value}.
   *
   * @param key {@link Encoder} to encode field `{@code key}`.
   * @param value {@link Encoder} to encode field `{@code value}`
   */
  public static <K, V> Encoder<KV<K, V>> kvEncoder(Encoder<K> key, Encoder<V> value) {
    Expression serializer = serializeKV(rootRef(KV_TYPE, true), key, value);
    Expression deserializer = deserializeKV(rootCol(serializer.dataType()), key, value);
    return EncoderFactory.create(serializer, deserializer, KV.class);
  }

  /**
   * Creates a Spark {@link Encoder} of {@link ArrayType} for Java {@link Collection}s with nullable
   * elements.
   *
   * @param enc {@link Encoder} to encode collection elements
   */
  public static <T> Encoder<Collection<T>> collectionEncoder(Encoder<T> enc) {
    return collectionEncoder(enc, true);
  }

  /**
   * Creates a Spark {@link Encoder} of {@link ArrayType} for Java {@link Collection}s.
   *
   * @param enc {@link Encoder} to encode collection elements
   * @param nullable Allow nullable collection elements
   */
  public static <T> Encoder<Collection<T>> collectionEncoder(Encoder<T> enc, boolean nullable) {
    DataType type = new ObjectType(Collection.class);
    Expression serializer = serializeSeq(rootRef(type, true), enc, nullable);
    Expression deserializer = deserializeSeq(rootCol(serializer.dataType()), enc, nullable, true);
    return EncoderFactory.create(serializer, deserializer, Collection.class);
  }

  /**
   * Creates a Spark {@link Encoder} of {@link MapType} that deserializes to {@link MapT}.
   *
   * @param key {@link Encoder} to encode keys
   * @param value {@link Encoder} to encode values
   * @param cls Specific class to use, supported are {@link HashMap} and {@link TreeMap}
   */
  public static <MapT extends Map<K, V>, K, V> Encoder<MapT> mapEncoder(
      Encoder<K> key, Encoder<V> value, Class<MapT> cls) {
    Expression serializer = mapSerializer(rootRef(new ObjectType(cls), true), key, value);
    Expression deserializer = mapDeserializer(rootCol(serializer.dataType()), key, value, cls);
    return EncoderFactory.create(serializer, deserializer, cls);
  }

  /**
   * Creates a Spark {@link Encoder} for Spark's {@link MutablePair} of {@link StructType} with
   * fields `{@code _1}` and `{@code _2}`.
   *
   * <p>This is intended to be used in places such as aggregators.
   *
   * @param enc1 {@link Encoder} to encode `{@code _1}`
   * @param enc2 {@link Encoder} to encode `{@code _2}`
   */
  public static <T1, T2> Encoder<MutablePair<T1, T2>> mutablePairEncoder(
      Encoder<T1> enc1, Encoder<T2> enc2) {
    Expression serializer = serializeMutablePair(rootRef(MUTABLE_PAIR_TYPE, true), enc1, enc2);
    Expression deserializer = deserializeMutablePair(rootCol(serializer.dataType()), enc1, enc2);
    return EncoderFactory.create(serializer, deserializer, MutablePair.class);
  }

  /**
   * Creates a Spark {@link Encoder} for {@link PaneInfo} of {@link DataTypes#BinaryType
   * BinaryType}.
   */
  private static Encoder<PaneInfo> paneInfoEncoder() {
    DataType type = new ObjectType(PaneInfo.class);
    return EncoderFactory.create(
        invokeIfNotNull(Utils.class, "paneInfoToBytes", BinaryType, rootRef(type, false)),
        invokeIfNotNull(Utils.class, "paneInfoFromBytes", type, rootCol(BinaryType)),
        PaneInfo.class);
  }

  /**
   * Creates a Spark {@link Encoder} for Joda {@link Instant} of {@link DataTypes#LongType
   * LongType}.
   */
  private static Encoder<Instant> instantEncoder() {
    DataType type = new ObjectType(Instant.class);
    Expression instant = rootRef(type, true);
    Expression millis = rootCol(LongType);
    return EncoderFactory.create(
        nullSafe(instant, invoke(instant, "getMillis", LongType, false)),
        nullSafe(millis, invoke(Instant.class, "ofEpochMilli", type, millis)),
        Instant.class);
  }

  /**
   * Creates a Spark {@link Encoder} for {@link T} of {@link DataTypes#BinaryType BinaryType}
   * delegating to a Beam {@link Coder} underneath.
   *
   * @param coder Beam {@link Coder}
   * @param nullable If to allow nullable items
   */
  private static <T> Encoder<T> binaryEncoder(Coder<T> coder, boolean nullable) {
    Literal litCoder = lit(coder, Coder.class);
    // T could be private, use OBJECT_TYPE for code generation to not risk an IllegalAccessError
    return EncoderFactory.create(
        invokeIfNotNull(
            CoderHelpers.class,
            "toByteArray",
            BinaryType,
            rootRef(OBJECT_TYPE, nullable),
            litCoder),
        invokeIfNotNull(
            CoderHelpers.class, "fromByteArray", OBJECT_TYPE, rootCol(BinaryType), litCoder),
        coder.getEncodedTypeDescriptor().getRawType());
  }

  private static <T, W extends BoundedWindow> Expression serializeWindowedValue(
      Expression in,
      Encoder<T> valueEnc,
      Encoder<Instant> timestampEnc,
      Encoder<Collection<W>> windowsEnc,
      Encoder<PaneInfo> paneEnc) {
    return serializerObject(
        in,
        tuple("value", serializeField(in, valueEnc, "getValue")),
        tuple("timestamp", serializeField(in, timestampEnc, "getTimestamp")),
        tuple("windows", serializeField(in, windowsEnc, "getWindows")),
        tuple("pane", serializeField(in, paneEnc, "getPane")));
  }

  private static Expression serializerObject(Expression in, Tuple2<String, Expression>... fields) {
    return SerializerBuildHelper.createSerializerForObject(in, seqOf(fields));
  }

  private static <T, W extends BoundedWindow> Expression deserializeWindowedValue(
      Expression in,
      Encoder<T> valueEnc,
      Encoder<Instant> timestampEnc,
      Encoder<Collection<W>> windowsEnc,
      Encoder<PaneInfo> paneEnc) {
    Expression value = deserializeField(in, valueEnc, 0, "value");
    Expression windows = deserializeField(in, windowsEnc, 2, "windows");
    Expression timestamp = deserializeField(in, timestampEnc, 1, "timestamp");
    Expression pane = deserializeField(in, paneEnc, 3, "pane");
    // set timestamp to end of window (maxTimestamp) if null
    timestamp =
        ifNotNull(timestamp, invoke(Utils.class, "maxTimestamp", timestamp.dataType(), windows));
    Expression[] fields = new Expression[] {value, timestamp, windows, pane};

    return nullSafe(pane, invoke(WindowedValue.class, "of", WINDOWED_VALUE, fields));
  }

  private static <K, V> Expression serializeMutablePair(
      Expression in, Encoder<K> enc1, Encoder<V> enc2) {
    return serializerObject(
        in,
        tuple("_1", serializeField(in, enc1, "_1")),
        tuple("_2", serializeField(in, enc2, "_2")));
  }

  private static <K, V> Expression deserializeMutablePair(
      Expression in, Encoder<K> enc1, Encoder<V> enc2) {
    Expression field1 = deserializeField(in, enc1, 0, "_1");
    Expression field2 = deserializeField(in, enc2, 1, "_2");
    return invoke(MutablePair.class, "apply", MUTABLE_PAIR_TYPE, field1, field2);
  }

  private static <K, V> Expression serializeKV(
      Expression in, Encoder<K> keyEnc, Encoder<V> valueEnc) {
    return serializerObject(
        in,
        tuple("key", serializeField(in, keyEnc, "getKey")),
        tuple("value", serializeField(in, valueEnc, "getValue")));
  }

  private static <K, V> Expression deserializeKV(
      Expression in, Encoder<K> keyEnc, Encoder<V> valueEnc) {
    Expression key = deserializeField(in, keyEnc, 0, "key");
    Expression value = deserializeField(in, valueEnc, 1, "value");
    return invoke(KV.class, "of", KV_TYPE, key, value);
  }

  public static <T> Expression serializeOneOf(Expression in, List<Encoder<T>> encoders) {
    Expression type = invoke(in, "_1", IntegerType, false);
    Expression[] args = new Expression[encoders.size() * 2];
    for (int i = 0; i < encoders.size(); i++) {
      args[i * 2] = lit(String.valueOf(i));
      args[i * 2 + 1] = serializeOneOfField(in, type, encoders.get(i), i);
    }
    return new CreateNamedStruct(seqOf(args));
  }

  public static <T> Expression deserializeOneOf(Expression in, List<Encoder<T>> encoders) {
    Expression[] args = new Expression[encoders.size()];
    for (int i = 0; i < encoders.size(); i++) {
      args[i] = deserializeOneOfField(in, encoders.get(i), i);
    }
    return new Coalesce(seqOf(args));
  }

  private static <T> Expression serializeOneOfField(
      Expression in, Expression type, Encoder<T> enc, int typeIdx) {
    Expression litNull = lit(null, serializedType(enc));
    Expression value = invoke(in, "_2", deserializedType(enc), false);
    return new If(new EqualTo(type, lit(typeIdx)), serialize(value, enc), litNull);
  }

  private static <T> Expression deserializeOneOfField(Expression in, Encoder<T> enc, int idx) {
    GetStructField field = new GetStructField(in, idx, Option.empty());
    Expression litNull = lit(null, TUPLE2_TYPE);
    Expression newTuple = newInstance(Tuple2.class, TUPLE2_TYPE, lit(idx), deserialize(field, enc));
    return new If(new IsNull(field), litNull, newTuple);
  }

  private static <T> Expression serializeField(Expression in, Encoder<T> enc, String getterName) {
    Expression ref = serializer(enc).collect(match(BoundReference.class)).head();
    return serialize(invoke(in, getterName, ref.dataType(), ref.nullable()), enc);
  }

  private static <T> Expression deserializeField(
      Expression in, Encoder<T> enc, int idx, String name) {
    return deserialize(new GetStructField(in, idx, new Some<>(name)), enc);
  }

  // Note: Currently this doesn't support nullable primitive values
  private static <K, V> Expression mapSerializer(Expression map, Encoder<K> key, Encoder<V> value) {
    DataType keyType = deserializedType(key);
    DataType valueType = deserializedType(value);
    return SerializerBuildHelper.createSerializerForMap(
        map,
        new MapElementInformation(keyType, false, e -> serialize(e, key)),
        new MapElementInformation(valueType, false, e -> serialize(e, value)));
  }

  private static <MapT extends Map<K, V>, K, V> Expression mapDeserializer(
      Expression in, Encoder<K> key, Encoder<V> value, Class<MapT> cls) {
    Preconditions.checkArgument(cls.isAssignableFrom(HashMap.class) || cls.equals(TreeMap.class));
    Expression keys = deserializeSeq(new MapKeys(in), key, false, false);
    Expression values = deserializeSeq(new MapValues(in), value, false, false);
    String fn = cls.equals(TreeMap.class) ? "toTreeMap" : "toMap";
    return invoke(
        Utils.class, fn, new ObjectType(cls), keys, values, mapItemType(key), mapItemType(value));
  }

  // serialized type for primitive types (avoid boxing!), otherwise the deserialized type
  private static Literal mapItemType(Encoder<?> enc) {
    return lit(isPrimitiveEnc(enc) ? serializedType(enc) : deserializedType(enc), DataType.class);
  }

  private static <T> Expression serializeSeq(Expression in, Encoder<T> enc, boolean nullable) {
    if (isPrimitiveEnc(enc)) {
      Expression array = invoke(in, "toArray", new ObjectType(Object[].class), false);
      return SerializerBuildHelper.createSerializerForGenericArray(
          array, serializedType(enc), nullable);
    }
    Expression seq = invoke(Utils.class, "toSeq", new ObjectType(Seq.class), in);
    return MapObjects$.MODULE$.apply(
        exp -> serialize(exp, enc), seq, deserializedType(enc), nullable, Option.empty());
  }

  private static <T> Expression deserializeSeq(
      Expression in, Encoder<T> enc, boolean nullable, boolean exposeAsJava) {
    DataType type = serializedType(enc); // input type is the serializer result type
    if (isPrimitiveEnc(enc)) {
      // Spark may reuse unsafe array data, if directly exposed it must be copied before
      return exposeAsJava
          ? invoke(Utils.class, "copyToList", LIST_TYPE, in, lit(type, DataType.class))
          : in;
    }
    Option<Class<?>> optCls = exposeAsJava ? Option.apply(List.class) : Option.empty();
    // MapObjects will always copy
    return MapObjects$.MODULE$.apply(exp -> deserialize(exp, enc), in, type, nullable, optCls);
  }

  private static <T> boolean isPrimitiveEnc(Encoder<T> enc) {
    return PRIMITIV_TYPES.contains(enc.clsTag().runtimeClass());
  }

  private static <T> Expression serialize(Expression input, Encoder<T> enc) {
    return serializer(enc).transformUp(replace(BoundReference.class, input));
  }

  private static <T> Expression deserialize(Expression input, Encoder<T> enc) {
    return deserializer(enc).transformUp(replace(GetColumnByOrdinal.class, input));
  }

  private static <T> Expression serializer(Encoder<T> enc) {
    return ((ExpressionEncoder<T>) enc).objSerializer();
  }

  private static <T> Expression deserializer(Encoder<T> enc) {
    return ((ExpressionEncoder<T>) enc).objDeserializer();
  }

  private static <T> DataType serializedType(Encoder<T> enc) {
    return ((ExpressionEncoder<T>) enc).objSerializer().dataType();
  }

  private static <T> DataType deserializedType(Encoder<T> enc) {
    return ((ExpressionEncoder<T>) enc).objDeserializer().dataType();
  }

  private static Expression rootRef(DataType dt, boolean nullable) {
    return new BoundReference(0, dt, nullable);
  }

  private static Expression rootCol(DataType dt) {
    return new GetColumnByOrdinal(0, dt);
  }

  private static Expression nullSafe(Expression in, Expression out) {
    return new If(new IsNull(in), lit(null, out.dataType()), out);
  }

  private static Expression ifNotNull(Expression expr, Expression otherwise) {
    return new If(new IsNotNull(expr), expr, otherwise);
  }

  private static <T extends @NonNull Object> Expression lit(T t) {
    return Literal$.MODULE$.apply(t);
  }

  @SuppressWarnings("nullness") // literal NULL is allowed
  private static <T> Expression lit(@Nullable T t, DataType dataType) {
    return new Literal(t, dataType);
  }

  private static <T extends @NonNull Object> Literal lit(T obj, Class<? extends T> cls) {
    return Literal.fromObject(obj, new ObjectType(cls));
  }

  /** Encoder / expression utils that are called from generated code. */
  public static class Utils {

    public static PaneInfo paneInfoFromBytes(byte[] bytes) {
      return CoderHelpers.fromByteArray(bytes, PaneInfoCoder.of());
    }

    public static byte[] paneInfoToBytes(PaneInfo pane) {
      return CoderHelpers.toByteArray(pane, PaneInfoCoder.of());
    }

    /** The end of the only window (max timestamp). */
    public static Instant maxTimestamp(Iterable<BoundedWindow> windows) {
      return Iterables.getOnlyElement(windows).maxTimestamp();
    }

    public static List<Object> copyToList(ArrayData arrayData, DataType type) {
      // Note, this could be optimized for primitive arrays (if elements are not nullable) using
      // Ints.asList(arrayData.toIntArray()) and similar
      return Arrays.asList(arrayData.toObjectArray(type));
    }

    public static Seq<Object> toSeq(ArrayData arrayData) {
      return arrayData.toSeq(OBJECT_TYPE);
    }

    public static Seq<Object> toSeq(Collection<Object> col) {
      if (col instanceof List) {
        return JavaConverters.asScalaBuffer((List<Object>) col);
      }
      return JavaConverters.collectionAsScalaIterable(col).toSeq();
    }

    public static TreeMap<Object, Object> toTreeMap(
        ArrayData keys, ArrayData values, DataType keyType, DataType valueType) {
      return toMap(new TreeMap<>(), keys, values, keyType, valueType);
    }

    public static HashMap<Object, Object> toMap(
        ArrayData keys, ArrayData values, DataType keyType, DataType valueType) {
      HashMap<Object, Object> map = Maps.newHashMapWithExpectedSize(keys.numElements());
      return toMap(map, keys, values, keyType, valueType);
    }

    private static <MapT extends Map<Object, Object>> MapT toMap(
        MapT map, ArrayData keys, ArrayData values, DataType keyType, DataType valueType) {
      IndexedSeq<Object> keysSeq = keys.toSeq(keyType);
      IndexedSeq<Object> valuesSeq = values.toSeq(valueType);
      for (int i = 0; i < keysSeq.size(); i++) {
        map.put(keysSeq.apply(i), valuesSeq.apply(i));
      }
      return map;
    }
  }
}
