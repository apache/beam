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

import static org.apache.beam.runners.spark.structuredstreaming.translation.batch.DoFnRunnerFactory.simple;
import static org.apache.beam.runners.spark.structuredstreaming.translation.helpers.EncoderHelpers.oneOfEncoder;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;
import static org.apache.spark.sql.functions.col;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Supplier;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.spark.SparkCommonPipelineOptions;
import org.apache.beam.runners.spark.structuredstreaming.metrics.MetricsAccumulator;
import org.apache.beam.runners.spark.structuredstreaming.translation.PipelineTranslator.UnresolvedTranslation;
import org.apache.beam.runners.spark.structuredstreaming.translation.TransformTranslator;
import org.apache.beam.runners.spark.structuredstreaming.translation.batch.functions.SideInputValues;
import org.apache.beam.runners.spark.structuredstreaming.translation.batch.functions.SparkSideInputReader;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.TypedColumn;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

/**
 * Translator for {@link ParDo.MultiOutput} based on {@link DoFnRunners#simpleRunner}.
 *
 * <p>Each tag is encoded as individual column with a respective schema & encoder each.
 *
 * <p>TODO:
 * <li>Add support for state and timers.
 * <li>Add support for SplittableDoFn
 */
class ParDoTranslatorBatch<InputT, OutputT>
    extends TransformTranslator<
        PCollection<? extends InputT>, PCollectionTuple, ParDo.MultiOutput<InputT, OutputT>> {

  ParDoTranslatorBatch() {
    super(0);
  }

  @Override
  public boolean canTranslate(ParDo.MultiOutput<InputT, OutputT> transform) {
    DoFn<InputT, OutputT> doFn = transform.getFn();
    DoFnSignature signature = DoFnSignatures.signatureForDoFn(doFn);

    // TODO: add support of Splittable DoFn
    checkState(
        !signature.processElement().isSplittable(),
        "Not expected to directly translate splittable DoFn, should have been overridden: %s",
        doFn);

    // TODO: add support of states and timers
    checkState(
        !signature.usesState() && !signature.usesTimers(),
        "States and timers are not supported for the moment.");

    checkState(
        signature.onWindowExpiration() == null, "onWindowExpiration is not supported: %s", doFn);

    checkState(
        !signature.processElement().requiresTimeSortedInput(),
        "@RequiresTimeSortedInput is not supported for the moment");

    SparkSideInputReader.validateMaterializations(transform.getSideInputs().values());
    return true;
  }

  @Override
  public void translate(ParDo.MultiOutput<InputT, OutputT> transform, Context cxt)
      throws IOException {

    PCollection<InputT> input = (PCollection<InputT>) cxt.getInput();
    SideInputReader sideInputReader =
        createSideInputReader(transform.getSideInputs().values(), cxt);
    MetricsAccumulator metrics = MetricsAccumulator.getInstance(cxt.getSparkSession());

    TupleTag<OutputT> mainOut = transform.getMainOutputTag();

    // Filter out obsolete PCollections to only cache when absolutely necessary
    Map<TupleTag<?>, PCollection<?>> outputs =
        skipObsoleteOutputs(cxt.getOutputs(), mainOut, transform.getAdditionalOutputTags(), cxt);

    if (outputs.size() > 1) {
      // In case of multiple outputs / tags, map each tag to a column by index.
      // At the end split the result into multiple datasets selecting one column each.
      Map<String, Integer> tagColIdx = tagsColumnIndex((Collection<TupleTag<?>>) outputs.keySet());
      List<Encoder<WindowedValue<Object>>> encoders = createEncoders(outputs, tagColIdx, cxt);

      DoFnPartitionIteratorFactory<InputT, ?, Tuple2<Integer, WindowedValue<Object>>> doFnMapper =
          DoFnPartitionIteratorFactory.multiOutput(
              cxt.getOptionsSupplier(),
              metrics,
              simple(cxt.getCurrentTransform(), input, sideInputReader, false),
              tagColIdx);

      // FIXME What's the strategy to unpersist Datasets / RDDs?

      SparkCommonPipelineOptions opts = cxt.getOptions().as(SparkCommonPipelineOptions.class);
      StorageLevel storageLevel = StorageLevel.fromString(opts.getStorageLevel());

      // Persist as wide rows with one column per TupleTag to support different schemas
      Dataset<Tuple2<Integer, WindowedValue<Object>>> allTagsDS =
          cxt.getDataset(input).mapPartitions(doFnMapper, oneOfEncoder(encoders));
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
      // Provide unresolved translation so that can be fused if possible
      UnresolvedParDo<InputT, OutputT> unresolvedParDo =
          new UnresolvedParDo<>(
              input,
              simple(cxt.getCurrentTransform(), input, sideInputReader, filterMainOutput),
              () -> cxt.windowedEncoder(output.getCoder()));
      cxt.putUnresolved(output, unresolvedParDo);
    }
  }

  /**
   * An unresolved {@link ParDo} translation that can be fused with previous / following ParDos for
   * better performance.
   */
  private static class UnresolvedParDo<InT, T> implements UnresolvedTranslation<InT, T> {
    private final PCollection<InT> input;
    private final DoFnRunnerFactory<InT, T> doFnFact;
    private final Supplier<Encoder<WindowedValue<T>>> encoder;

    UnresolvedParDo(
        PCollection<InT> input,
        DoFnRunnerFactory<InT, T> doFnFact,
        Supplier<Encoder<WindowedValue<T>>> encoder) {
      this.input = input;
      this.doFnFact = doFnFact;
      this.encoder = encoder;
    }

    @Override
    public PCollection<InT> getInput() {
      return input;
    }

    @Override
    public <T2> UnresolvedTranslation<InT, T2> fuse(UnresolvedTranslation<T, T2> next) {
      UnresolvedParDo<T, T2> nextParDo = (UnresolvedParDo<T, T2>) next;
      return new UnresolvedParDo<>(input, doFnFact.fuse(nextParDo.doFnFact), nextParDo.encoder);
    }

    @Override
    public Dataset<WindowedValue<T>> resolve(
        Supplier<PipelineOptions> options, Dataset<WindowedValue<InT>> input) {
      MetricsAccumulator metrics = MetricsAccumulator.getInstance(input.sparkSession());
      DoFnPartitionIteratorFactory<InT, ?, WindowedValue<T>> doFnMapper =
          DoFnPartitionIteratorFactory.singleOutput(options, metrics, doFnFact);
      return input.mapPartitions(doFnMapper, encoder.get());
    }
  }

  /**
   * Filter out obsolete, unused output tags except for {@code mainTag}.
   *
   * <p>This can help to avoid unnecessary caching in case of multiple outputs if only {@code
   * mainTag} is consumed.
   */
  private Map<TupleTag<?>, PCollection<?>> skipObsoleteOutputs(
      Map<TupleTag<?>, PCollection<?>> outputs,
      TupleTag<?> mainTag,
      TupleTagList otherTags,
      Context cxt) {
    switch (outputs.size()) {
      case 1:
        return outputs; // always keep main output
      case 2:
        TupleTag<?> otherTag = otherTags.get(0);
        return cxt.isLeaf(checkStateNotNull(outputs.get(otherTag)))
            ? Collections.singletonMap(mainTag, checkStateNotNull(outputs.get(mainTag)))
            : outputs;
      default:
        Map<TupleTag<?>, PCollection<?>> filtered = Maps.newHashMapWithExpectedSize(outputs.size());
        for (Map.Entry<TupleTag<?>, PCollection<?>> e : outputs.entrySet()) {
          if (e.getKey().equals(mainTag) || !cxt.isLeaf(e.getValue())) {
            filtered.put(e.getKey(), e.getValue());
          }
        }
        return filtered;
    }
  }

  private Map<String, Integer> tagsColumnIndex(Collection<TupleTag<?>> tags) {
    Map<String, Integer> index = Maps.newHashMapWithExpectedSize(tags.size());
    for (TupleTag<?> tag : tags) {
      index.put(tag.getId(), index.size());
    }
    return index;
  }

  /** List of encoders matching the order of tagIds. */
  private List<Encoder<WindowedValue<Object>>> createEncoders(
      Map<TupleTag<?>, PCollection<?>> outputs, Map<String, Integer> tagIdColIdx, Context ctx) {
    ArrayList<Encoder<WindowedValue<Object>>> encoders = new ArrayList<>(outputs.size());
    for (Entry<TupleTag<?>, PCollection<?>> e : outputs.entrySet()) {
      Encoder<WindowedValue<Object>> enc = ctx.windowedEncoder((Coder) e.getValue().getCoder());
      int colIdx = checkStateNotNull(tagIdColIdx.get(e.getKey().getId()));
      encoders.add(colIdx, enc);
    }
    return encoders;
  }

  private <T> SideInputReader createSideInputReader(
      Collection<PCollectionView<?>> views, Context cxt) {
    if (views.isEmpty()) {
      return SparkSideInputReader.empty();
    }
    Map<String, Broadcast<SideInputValues<?>>> broadcasts =
        Maps.newHashMapWithExpectedSize(views.size());
    for (PCollectionView<?> view : views) {
      PCollection<T> pCol = checkStateNotNull((PCollection<T>) view.getPCollection());
      // get broadcasted SideInputValues for pCol, if not available use loader function
      Broadcast<SideInputValues<T>> broadcast =
          cxt.getSideInputBroadcast(pCol, SideInputValues.loader(pCol));
      broadcasts.put(view.getTagInternal().getId(), (Broadcast) broadcast);
    }
    return SparkSideInputReader.create(broadcasts);
  }
}
