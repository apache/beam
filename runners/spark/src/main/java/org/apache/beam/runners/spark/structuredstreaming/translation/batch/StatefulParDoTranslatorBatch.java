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

import static org.apache.beam.runners.spark.structuredstreaming.translation.helpers.EncoderHelpers.oneOfEncoder;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;
import static org.apache.spark.sql.functions.col;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.spark.SparkCommonPipelineOptions;
import org.apache.beam.runners.spark.structuredstreaming.metrics.MetricsAccumulator;
import org.apache.beam.runners.spark.structuredstreaming.translation.TransformTranslator;
import org.apache.beam.runners.spark.structuredstreaming.translation.batch.functions.SparkSideInputReader;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.spark.api.java.function.FlatMapGroupsFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.KeyValueGroupedDataset;
import org.apache.spark.sql.TypedColumn;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

/**
 * Translator for a stateful {@link ParDo.MultiOutput}, or one requiring time sorted input.
 *
 * <p>Selected by {@link PipelineTranslatorBatch} in place of {@link ParDoTranslatorBatch} when the
 * {@link DoFn} uses state, uses timers, or is annotated with {@link DoFn.RequiresTimeSortedInput};
 * see {@link #appliesTo}.
 *
 * <p>Unlike {@link ParDoTranslatorBatch} this translator never produces an {@code
 * UnresolvedTranslation}: a stateful {@link DoFn} must not be fused with neighbouring {@link ParDo
 * ParDos}, because the fused runner cannot drive timers. Resolving the input dataset via {@code
 * Context#getDataset} breaks any pending fusion chain.
 *
 * <p>Additional (tagged) outputs are encoded as one column per tag, as in {@link
 * ParDoTranslatorBatch}.
 */
class StatefulParDoTranslatorBatch<K, V, OutputT>
    extends TransformTranslator<
        PCollection<? extends KV<K, V>>, PCollectionTuple, ParDo.MultiOutput<KV<K, V>, OutputT>> {

  StatefulParDoTranslatorBatch() {
    // A stateful ParDo introduces a shuffle to co-locate and order each key, so it contributes to
    // plan complexity much like GroupByKey rather than like a plain ParDo.
    super(0.2f);
  }

  /**
   * Whether {@code transform} must be translated by this translator rather than {@link
   * ParDoTranslatorBatch}.
   *
   * <p>Note {@link DoFn.RequiresTimeSortedInput} is tested independently of state: the SDK only
   * treats state and timers as making a {@link DoFn} stateful, so a {@code DoFn} carrying only that
   * annotation reaches the runner with neither signature flag set.
   */
  static boolean appliesTo(ParDo.MultiOutput<?, ?> transform) {
    DoFnSignature signature = DoFnSignatures.signatureForDoFn(transform.getFn());
    return signature.usesState()
        || signature.usesTimers()
        || signature.processElement().requiresTimeSortedInput();
  }

  @Override
  protected boolean canTranslate(ParDo.MultiOutput<KV<K, V>, OutputT> transform) {
    DoFn<KV<K, V>, OutputT> doFn = transform.getFn();
    DoFnSignature signature = DoFnSignatures.signatureForDoFn(doFn);

    checkState(
        appliesTo(transform),
        "Not a stateful or time sorted DoFn, should have been translated by %s: %s",
        ParDoTranslatorBatch.class.getSimpleName(),
        doFn);

    checkState(
        isSupported(),
        "Stateful and time sorted ParDo require Spark 3.4+ "
            + "(KeyValueGroupedDataset#flatMapSortedGroups): %s",
        doFn);

    checkState(
        !signature.processElement().isSplittable(),
        "Not expected to directly translate splittable DoFn, should have been overridden: %s",
        doFn);

    // Not implemented: firing @OnWindowExpiration requires tracking the windows observed per key
    // and a dedicated firing pass at the end of each key, see
    // https://github.com/apache/beam/issues/22524
    checkState(
        signature.onWindowExpiration() == null, "onWindowExpiration is not supported: %s", doFn);

    SparkSideInputReader.validateMaterializations(transform.getSideInputs().values());
    return true;
  }

  @Override
  protected void translate(ParDo.MultiOutput<KV<K, V>, OutputT> transform, Context cxt)
      throws IOException {
    PCollection<KV<K, V>> input = (PCollection<KV<K, V>>) cxt.getInput();

    validateKeyCoder(input.getCoder(), transform.getFn());
    validateWindowingStrategy(input.getWindowingStrategy(), transform.getFn());

    TupleTag<OutputT> mainOut = transform.getMainOutputTag();
    // Filter out obsolete PCollections to only cache when absolutely necessary
    Map<TupleTag<?>, PCollection<?>> outputs =
        ParDoTranslatorBatch.skipUnconsumedOutputs(
            cxt.getOutputs(), mainOut, transform.getAdditionalOutputTags(), cxt);

    KvCoder<K, V> inputCoder = (KvCoder<K, V>) input.getCoder();
    Encoder<K> keyEnc = cxt.keyEncoderOf(inputCoder);
    MetricsAccumulator metrics = MetricsAccumulator.getInstance(cxt.getSparkSession());
    SideInputReader sideInputReader =
        ParDoTranslatorBatch.createSideInputReader(transform.getSideInputs().values(), cxt);

    // Group by key, then order each group by event time before handing it to the DoFn. The
    // timestamp is a top level LongType column of the WindowedValue encoder (epoch millis), so
    // ordering is plain signed numeric ordering; no composite sort key is needed. Nulls sort
    // last: a null timestamp encodes END_OF_WINDOW (see GroupByKeyTranslatorBatch), which no
    // concrete timestamp of the same window can exceed. Only null and concrete timestamps of
    // different windows mixed into one key group may still order imprecisely; deriving the
    // timestamp from the window column is not portable across Spark versions.
    Column[] sortCols = new Column[] {col(TIMESTAMP_COLUMN).asc_nulls_last()};

    if (outputs.size() > 1) {
      // In case of multiple outputs / tags, map each tag to a column by index.
      // At the end split the result into multiple datasets selecting one column each.
      Map<String, Integer> tagColIdx = ParDoTranslatorBatch.tagsColumnIndex(outputs.keySet());
      List<Encoder<WindowedValue<Object>>> encoders =
          ParDoTranslatorBatch.createEncoders(outputs, tagColIdx, cxt);

      DoFnRunnerFactory<KV<K, V>, OutputT> runnerFactory =
          DoFnRunnerFactory.simple(cxt.getCurrentTransform(), input, sideInputReader, false);
      StatefulDoFnGroupFunction<K, KV<K, V>, Tuple2<Integer, WindowedValue<Object>>> groupFn =
          StatefulDoFnGroupFunction.multiOutput(
              cxt.getOptionsSupplier(), metrics, runnerFactory, tagColIdx);

      SparkCommonPipelineOptions opts = cxt.getOptions().as(SparkCommonPipelineOptions.class);
      StorageLevel storageLevel = StorageLevel.fromString(opts.getStorageLevel());

      // Persist as wide rows with one column per TupleTag to support different schemas
      Dataset<Tuple2<Integer, WindowedValue<Object>>> allTagsDS =
          cxt.getDataset(input)
              .groupByKey(GroupByKeyHelpers.valueKey(), keyEnc)
              .flatMapSortedGroups(sortCols, groupFn, oneOfEncoder(encoders));
      allTagsDS.persist(storageLevel);

      // divide into separate output datasets per tag
      for (TupleTag<?> tag : outputs.keySet()) {
        int colIdx = checkStateNotNull(tagColIdx.get(tag.getId()), "Unknown tag");
        // Resolve specific column matching the tuple tag (by id)
        TypedColumn<Tuple2<Integer, WindowedValue<Object>>, WindowedValue<Object>> col =
            (TypedColumn) col(Integer.toString(colIdx)).as(encoders.get(colIdx));

        // Caching of the returned outputs is disabled to avoid caching the same data twice.
        cxt.putDataset(
            cxt.getOutput((TupleTag) tag), allTagsDS.filter(col.isNotNull()).select(col), false);
      }
    } else {
      PCollection<OutputT> output = cxt.getOutput(mainOut);
      // Obsolete outputs might have to be filtered out
      boolean filterMainOutput = cxt.getOutputs().size() > 1;
      DoFnRunnerFactory<KV<K, V>, OutputT> runnerFactory =
          DoFnRunnerFactory.simple(
              cxt.getCurrentTransform(), input, sideInputReader, filterMainOutput);
      StatefulDoFnGroupFunction<K, KV<K, V>, WindowedValue<OutputT>> groupFn =
          StatefulDoFnGroupFunction.singleOutput(cxt.getOptionsSupplier(), metrics, runnerFactory);

      Dataset<WindowedValue<OutputT>> result =
          cxt.getDataset(input)
              .groupByKey(GroupByKeyHelpers.valueKey(), keyEnc)
              .flatMapSortedGroups(sortCols, groupFn, cxt.windowedEncoder(output.getCoder()));

      cxt.putDataset(output, result);
    }
  }

  /** Field of the {@code WindowedValue} encoder holding the event time, as epoch millis. */
  private static final String TIMESTAMP_COLUMN = "timestamp";

  private static final boolean SORTED_GROUPS_API_AVAILABLE = sortedGroupsApiAvailable();

  /**
   * Whether this Spark version supports stateful / time sorted ParDo: the required {@code
   * flatMapSortedGroups(Column[], FlatMapGroupsFunction, Encoder)} only exists since Spark 3.4.
   */
  static boolean isSupported() {
    return SORTED_GROUPS_API_AVAILABLE;
  }

  private static boolean sortedGroupsApiAvailable() {
    try {
      KeyValueGroupedDataset.class.getMethod(
          "flatMapSortedGroups", Column[].class, FlatMapGroupsFunction.class, Encoder.class);
      return true;
    } catch (NoSuchMethodException e) {
      return false;
    }
  }

  /**
   * A stateful {@link DoFn} is keyed, and this translator co-locates and orders elements by the
   * encoded key, so the key coder must be deterministic.
   *
   * <p>{@code ParDo} already enforces both of these for {@code DoFns} using state or timers (see
   * {@code ParDo.validateStateApplicableForInput}), but that validation is skipped for a {@link
   * DoFn} carrying only {@link DoFn.RequiresTimeSortedInput}, which still reaches this translator.
   * So it is checked here rather than assumed.
   */
  @VisibleForTesting
  static void validateKeyCoder(Coder<?> coder, DoFn<?, ?> doFn) {
    checkState(
        coder instanceof KvCoder,
        "Input to a stateful or time sorted ParDo requires a %s, but the coder was %s: %s",
        KvCoder.class.getSimpleName(),
        coder,
        doFn);

    Coder<?> keyCoder = ((KvCoder<?, ?>) coder).getKeyCoder();
    try {
      keyCoder.verifyDeterministic();
    } catch (Coder.NonDeterministicException e) {
      throw new IllegalStateException(
          String.format(
              "Input to a stateful or time sorted ParDo requires a deterministic key coder, "
                  + "but %s is not deterministic: %s",
              keyCoder, doFn),
          e);
    }
  }

  /**
   * State is scoped per key and window, which is only well defined if windows are not still subject
   * to merging.
   *
   * <p>This deliberately mirrors Dataflow's {@code verifyStateSupportForWindowingStrategy} and
   * tests {@link WindowingStrategy#needsMerge()} rather than {@code WindowFn#isNonMerging()}: after
   * a {@link org.apache.beam.sdk.transforms.GroupByKey GroupByKey} the strategy keeps its merging
   * {@code WindowFn} but is flagged as already merged, and such pipelines are legal.
   */
  @VisibleForTesting
  static void validateWindowingStrategy(
      WindowingStrategy<?, ?> windowingStrategy, DoFn<?, ?> doFn) {
    checkState(
        !windowingStrategy.needsMerge(),
        "Stateful and time sorted ParDo are not supported for merging windows, "
            + "state cannot be scoped to a window that may still merge. WindowFn: %s, DoFn: %s",
        windowingStrategy.getWindowFn(),
        doFn);
  }
}
