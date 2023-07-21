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
package org.apache.beam.runners.spark.structuredstreaming.translation.batch;

import static org.apache.beam.repackaged.core.org.apache.commons.lang3.ArrayUtils.EMPTY_BYTE_ARRAY;
import static org.apache.beam.runners.spark.structuredstreaming.translation.batch.GroupByKeyHelpers.eligibleForGlobalGroupBy;
import static org.apache.beam.runners.spark.structuredstreaming.translation.batch.GroupByKeyHelpers.eligibleForGroupByWindow;
import static org.apache.beam.runners.spark.structuredstreaming.translation.batch.GroupByKeyHelpers.explodeWindowedKey;
import static org.apache.beam.runners.spark.structuredstreaming.translation.batch.GroupByKeyHelpers.valueKey;
import static org.apache.beam.runners.spark.structuredstreaming.translation.batch.GroupByKeyHelpers.valueValue;
import static org.apache.beam.runners.spark.structuredstreaming.translation.batch.GroupByKeyHelpers.windowedKV;
import static org.apache.beam.runners.spark.structuredstreaming.translation.helpers.CoderHelpers.toByteArray;
import static org.apache.beam.runners.spark.structuredstreaming.translation.helpers.EncoderHelpers.collectionEncoder;
import static org.apache.beam.runners.spark.structuredstreaming.translation.helpers.EncoderHelpers.encoderOf;
import static org.apache.beam.runners.spark.structuredstreaming.translation.helpers.EncoderHelpers.kvEncoder;
import static org.apache.beam.runners.spark.structuredstreaming.translation.helpers.EncoderHelpers.windowedValueEncoder;
import static org.apache.beam.runners.spark.structuredstreaming.translation.utils.ScalaInterop.concat;
import static org.apache.beam.runners.spark.structuredstreaming.translation.utils.ScalaInterop.fun1;
import static org.apache.beam.runners.spark.structuredstreaming.translation.utils.ScalaInterop.fun2;
import static org.apache.beam.runners.spark.structuredstreaming.translation.utils.ScalaInterop.javaIterator;
import static org.apache.beam.runners.spark.structuredstreaming.translation.utils.ScalaInterop.listOf;
import static org.apache.beam.runners.spark.structuredstreaming.translation.utils.ScalaInterop.seqOf;
import static org.apache.beam.sdk.transforms.windowing.PaneInfo.NO_FIRING;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.min;
import static org.apache.spark.sql.functions.struct;

import java.io.Serializable;
import org.apache.beam.runners.core.InMemoryStateInternals;
import org.apache.beam.runners.core.ReduceFnRunner;
import org.apache.beam.runners.core.StateInternalsFactory;
import org.apache.beam.runners.core.SystemReduceFn;
import org.apache.beam.runners.spark.structuredstreaming.translation.TransformTranslator;
import org.apache.beam.runners.spark.structuredstreaming.translation.batch.functions.GroupAlsoByWindowViaOutputBufferFn;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo.PaneInfoCoder;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.TypedColumn;
import org.apache.spark.sql.catalyst.expressions.CreateArray;
import org.apache.spark.sql.catalyst.expressions.CreateNamedStruct;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.catalyst.expressions.Literal$;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.checkerframework.checker.nullness.qual.NonNull;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.Seq;
import scala.collection.immutable.List;

/**
 * Translator for {@link GroupByKey} using {@link Dataset#groupByKey} with the build-in aggregation
 * function {@code collect_list} when applicable.
 *
 * <p>Note: Using {@code collect_list} isn't any worse than using {@link ReduceFnRunner}. In the
 * latter case the entire group (iterator) has to be loaded into memory as well. Either way there's
 * a risk of OOM errors. When disabling {@link #useCollectList}, a more memory sensitive iterable is
 * used that can be traversed just once. Attempting to traverse the iterable again will throw.
 *
 * <ul>
 *   <li>When using the default global window, window information is dropped and restored after the
 *       aggregation.
 *   <li>For non-merging windows, windows are exploded and moved into a composite key for better
 *       distribution. Though, to keep the amount of shuffled data low, this is only done if values
 *       are assigned to a single window or if there are only few keys and distributing data is
 *       important. After the aggregation, windowed values are restored from the composite key.
 *   <li>All other cases are implemented using the SDK {@link ReduceFnRunner}.
 * </ul>
 */
class GroupByKeyTranslatorBatch<K, V>
    extends TransformTranslator<
        PCollection<KV<K, V>>, PCollection<KV<K, Iterable<V>>>, GroupByKey<K, V>> {

  /** Literal of binary encoded Pane info. */
  private static final Expression PANE_NO_FIRING = lit(toByteArray(NO_FIRING, PaneInfoCoder.of()));

  /** Defaults for value in single global window. */
  private static final List<Expression> GLOBAL_WINDOW_DETAILS =
      windowDetails(lit(new byte[][] {EMPTY_BYTE_ARRAY}));

  private boolean useCollectList = true;

  GroupByKeyTranslatorBatch() {
    super(0.2f);
  }

  GroupByKeyTranslatorBatch(boolean useCollectList) {
    super(0.2f);
    this.useCollectList = useCollectList;
  }

  @Override
  public void translate(GroupByKey<K, V> transform, Context cxt) {
    WindowingStrategy<?, ?> windowing = cxt.getInput().getWindowingStrategy();
    TimestampCombiner tsCombiner = windowing.getTimestampCombiner();

    Dataset<WindowedValue<KV<K, V>>> input = cxt.getDataset(cxt.getInput());

    KvCoder<K, V> inputCoder = (KvCoder<K, V>) cxt.getInput().getCoder();
    KvCoder<K, Iterable<V>> outputCoder = (KvCoder<K, Iterable<V>>) cxt.getOutput().getCoder();

    Encoder<V> valueEnc = cxt.valueEncoderOf(inputCoder);
    Encoder<K> keyEnc = cxt.keyEncoderOf(inputCoder);

    // In batch we can ignore triggering and allowed lateness parameters
    final Dataset<WindowedValue<KV<K, Iterable<V>>>> result;

    if (useCollectList && eligibleForGlobalGroupBy(windowing, false)) {
      // Collects all values per key in memory. This might be problematic if there's few keys only
      // or some highly skewed distribution.
      result =
          input
              .groupBy(col("value.key").as("key"))
              .agg(collect_list(col("value.value")).as("values"), timestampAggregator(tsCombiner))
              .select(
                  inGlobalWindow(
                      keyValue(col("key").as(keyEnc), col("values").as(iterableEnc(valueEnc))),
                      windowTimestamp(tsCombiner)));

    } else if (eligibleForGlobalGroupBy(windowing, true)) {
      // Produces an iterable that can be traversed exactly once. However, on the plus side, data is
      // not collected in memory until serialized or done by the user.
      result =
          cxt.getDataset(cxt.getInput())
              .groupByKey(valueKey(), keyEnc)
              .mapValues(valueValue(), cxt.valueEncoderOf(inputCoder))
              .mapGroups(fun2((k, it) -> KV.of(k, iterableOnce(it))), cxt.kvEncoderOf(outputCoder))
              .map(fun1(WindowedValue::valueInGlobalWindow), cxt.windowedEncoder(outputCoder));

    } else if (useCollectList
        && eligibleForGroupByWindow(windowing, false)
        && (windowing.getWindowFn().assignsToOneWindow() || transform.fewKeys())) {
      // Using the window as part of the key should help to better distribute the data. However, if
      // values are assigned to multiple windows, more data would be shuffled around. If there's few
      // keys only, this is still valuable.
      // Collects all values per key & window in memory.
      result =
          input
              .select(explode(col("windows")).as("window"), col("value"), col("timestamp"))
              .groupBy(col("value.key").as("key"), col("window"))
              .agg(collect_list(col("value.value")).as("values"), timestampAggregator(tsCombiner))
              .select(
                  inSingleWindow(
                      keyValue(col("key").as(keyEnc), col("values").as(iterableEnc(valueEnc))),
                      col("window").as(cxt.windowEncoder()),
                      windowTimestamp(tsCombiner)));

    } else if (eligibleForGroupByWindow(windowing, true)
        && (windowing.getWindowFn().assignsToOneWindow() || transform.fewKeys())) {
      // Using the window as part of the key should help to better distribute the data. However, if
      // values are assigned to multiple windows, more data would be shuffled around. If there's few
      // keys only, this is still valuable.
      // Produces an iterable that can be traversed exactly once. However, on the plus side, data is
      // not collected in memory until serialized or done by the user.
      Encoder<Tuple2<BoundedWindow, K>> windowedKeyEnc =
          cxt.tupleEncoder(cxt.windowEncoder(), keyEnc);
      result =
          cxt.getDataset(cxt.getInput())
              .flatMap(explodeWindowedKey(valueValue()), cxt.tupleEncoder(windowedKeyEnc, valueEnc))
              .groupByKey(fun1(Tuple2::_1), windowedKeyEnc)
              .mapValues(fun1(Tuple2::_2), valueEnc)
              .mapGroups(
                  fun2((wKey, it) -> windowedKV(wKey, iterableOnce(it))),
                  cxt.windowedEncoder(outputCoder));

    } else {
      // Collects all values per key in memory. This might be problematic if there's few keys only
      // or some highly skewed distribution.

      // FIXME Revisit this case, implementation is far from ideal:
      // - iterator traversed at least twice, forcing materialization in memory

      // group by key, then by windows
      result =
          input
              .groupByKey(valueKey(), keyEnc)
              .flatMapGroups(
                  new GroupAlsoByWindowViaOutputBufferFn<>(
                      windowing,
                      (SerStateInternalsFactory) key -> InMemoryStateInternals.forKey(key),
                      SystemReduceFn.buffering(inputCoder.getValueCoder()),
                      cxt.getOptionsSupplier()),
                  cxt.windowedEncoder(outputCoder));
    }

    cxt.putDataset(cxt.getOutput(), result);
  }

  /** Serializable In-memory state internals factory. */
  private interface SerStateInternalsFactory<K> extends StateInternalsFactory<K>, Serializable {}

  private Encoder<Iterable<V>> iterableEnc(Encoder<V> enc) {
    // safe to use list encoder with collect list
    return (Encoder) collectionEncoder(enc);
  }

  private static Column[] timestampAggregator(TimestampCombiner tsCombiner) {
    if (tsCombiner.equals(TimestampCombiner.END_OF_WINDOW)) {
      return new Column[0]; // no aggregation needed
    }
    Column agg =
        tsCombiner.equals(TimestampCombiner.EARLIEST)
            ? min(col("timestamp"))
            : max(col("timestamp"));
    return new Column[] {agg.as("timestamp")};
  }

  private static Expression windowTimestamp(TimestampCombiner tsCombiner) {
    if (tsCombiner.equals(TimestampCombiner.END_OF_WINDOW)) {
      // null will be set to END_OF_WINDOW by the respective deserializer
      return litNull(DataTypes.LongType);
    }
    return col("timestamp").expr();
  }

  /**
   * Java {@link Iterable} from Scala {@link Iterator} that can be iterated just once so that we
   * don't have to load all data into memory.
   */
  private static <T extends @NonNull Object> Iterable<T> iterableOnce(Iterator<T> it) {
    return () -> {
      checkState(!it.isEmpty(), "Iterator on values can only be consumed once!");
      return javaIterator(it);
    };
  }

  private <T> TypedColumn<?, KV<K, T>> keyValue(TypedColumn<?, K> key, TypedColumn<?, T> value) {
    return struct(key.as("key"), value.as("value")).as(kvEncoder(key.encoder(), value.encoder()));
  }

  private static <InT, T> TypedColumn<InT, WindowedValue<T>> inGlobalWindow(
      TypedColumn<?, T> value, Expression ts) {
    List<Expression> fields = concat(timestampedValue(value, ts), GLOBAL_WINDOW_DETAILS);
    Encoder<WindowedValue<T>> enc =
        windowedValueEncoder(value.encoder(), encoderOf(GlobalWindow.class));
    return (TypedColumn<InT, WindowedValue<T>>) new Column(new CreateNamedStruct(fields)).as(enc);
  }

  public static <InT, T> TypedColumn<InT, WindowedValue<T>> inSingleWindow(
      TypedColumn<?, T> value, TypedColumn<?, ? extends BoundedWindow> window, Expression ts) {
    Expression windows = new CreateArray(listOf(window.expr()));
    Seq<Expression> fields = concat(timestampedValue(value, ts), windowDetails(windows));
    Encoder<WindowedValue<T>> enc = windowedValueEncoder(value.encoder(), window.encoder());
    return (TypedColumn<InT, WindowedValue<T>>) new Column(new CreateNamedStruct(fields)).as(enc);
  }

  private static List<Expression> timestampedValue(Column value, Expression ts) {
    return seqOf(lit("value"), value.expr(), lit("timestamp"), ts).toList();
  }

  private static List<Expression> windowDetails(Expression windows) {
    return seqOf(lit("windows"), windows, lit("pane"), PANE_NO_FIRING).toList();
  }

  private static <T extends @NonNull Object> Expression lit(T t) {
    return Literal$.MODULE$.apply(t);
  }

  @SuppressWarnings("nullness") // NULL literal
  private static Expression litNull(DataType dataType) {
    return new Literal(null, dataType);
  }
}
