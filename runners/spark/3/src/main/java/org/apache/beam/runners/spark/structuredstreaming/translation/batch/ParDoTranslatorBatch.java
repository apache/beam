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

import static java.util.stream.Collectors.toList;
import static org.apache.beam.runners.spark.structuredstreaming.translation.helpers.EncoderHelpers.oneOfEncoder;
import static org.apache.beam.runners.spark.structuredstreaming.translation.utils.ScalaInterop.fun1;
import static org.apache.beam.runners.spark.structuredstreaming.translation.utils.ScalaInterop.tuple;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.storage.StorageLevel.MEMORY_ONLY;

import java.io.IOException;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.construction.ParDoTranslation;
import org.apache.beam.runners.spark.SparkCommonPipelineOptions;
import org.apache.beam.runners.spark.structuredstreaming.translation.TransformTranslator;
import org.apache.beam.runners.spark.structuredstreaming.translation.helpers.CoderHelpers;
import org.apache.beam.runners.spark.structuredstreaming.translation.helpers.SideInputBroadcast;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Streams;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.TypedColumn;
import org.apache.spark.storage.StorageLevel;
import scala.Function1;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.reflect.ClassTag;

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

  private static final ClassTag<WindowedValue<Object>> WINDOWED_VALUE_CTAG =
      ClassTag.apply(WindowedValue.class);

  private static final ClassTag<Tuple2<Integer, WindowedValue<Object>>> TUPLE2_CTAG =
      ClassTag.apply(Tuple2.class);

  @Override
  public void translate(ParDo.MultiOutput<InputT, OutputT> transform, Context cxt)
      throws IOException {
    String stepName = cxt.getCurrentTransform().getFullName();

    SparkCommonPipelineOptions opts = cxt.getOptions().as(SparkCommonPipelineOptions.class);
    StorageLevel storageLevel = StorageLevel.fromString(opts.getStorageLevel());

    // Check for not supported advanced features
    // TODO: add support of Splittable DoFn
    DoFn<InputT, OutputT> doFn = transform.getFn();
    checkState(
        !DoFnSignatures.isSplittable(doFn),
        "Not expected to directly translate splittable DoFn, should have been overridden: %s",
        doFn);

    // TODO: add support of states and timers
    checkState(
        !DoFnSignatures.isStateful(doFn), "States and timers are not supported for the moment.");

    checkState(
        !DoFnSignatures.requiresTimeSortedInput(doFn),
        "@RequiresTimeSortedInput is not supported for the moment");

    TupleTag<OutputT> mainOutputTag = transform.getMainOutputTag();

    DoFnSchemaInformation doFnSchema =
        ParDoTranslation.getSchemaInformation(cxt.getCurrentTransform());

    PCollection<InputT> input = (PCollection<InputT>) cxt.getInput();
    DoFnMapPartitionsFactory<InputT, OutputT> factory =
        new DoFnMapPartitionsFactory<>(
            stepName,
            doFn,
            doFnSchema,
            cxt.getSerializableOptions(),
            input,
            mainOutputTag,
            cxt.getOutputs(),
            transform.getSideInputs(),
            createBroadcastSideInputs(transform.getSideInputs().values(), cxt));

    Dataset<WindowedValue<InputT>> inputDs = cxt.getDataset(input);
    if (cxt.getOutputs().size() > 1) {
      // In case of multiple outputs / tags, map each tag to a column by index.
      // At the end split the result into multiple datasets selecting one column each.
      Map<TupleTag<?>, Integer> tags = ImmutableMap.copyOf(zipwithIndex(cxt.getOutputs().keySet()));

      List<Encoder<WindowedValue<Object>>> encoders =
          createEncoders(cxt.getOutputs(), (Iterable<TupleTag<?>>) tags.keySet(), cxt);

      Function1<Iterator<WindowedValue<InputT>>, Iterator<Tuple2<Integer, WindowedValue<Object>>>>
          doFnMapper = factory.create((tag, v) -> tuple(tags.get(tag), (WindowedValue<Object>) v));

      // FIXME What's the strategy to unpersist Datasets / RDDs?

      // If using storage level MEMORY_ONLY, it's best to persist the dataset as RDD to avoid any
      // serialization / use of encoders. Persisting a Dataset, even if using a "deserialized"
      // storage level, involves converting the data to the internal representation (InternalRow)
      // by use of an encoder.
      // For any other storage level, persist as Dataset, so we can select columns by TupleTag
      // individually without restoring the entire row.
      if (MEMORY_ONLY().equals(storageLevel)) {

        RDD<Tuple2<Integer, WindowedValue<Object>>> allTagsRDD =
            inputDs.rdd().mapPartitions(doFnMapper, false, TUPLE2_CTAG);
        allTagsRDD.persist();

        // divide into separate output datasets per tag
        for (Entry<TupleTag<?>, Integer> e : tags.entrySet()) {
          TupleTag<Object> key = (TupleTag<Object>) e.getKey();
          Integer id = e.getValue();

          RDD<WindowedValue<Object>> rddByTag =
              allTagsRDD
                  .filter(fun1(t -> t._1.equals(id)))
                  .map(fun1(Tuple2::_2), WINDOWED_VALUE_CTAG);

          cxt.putDataset(
              cxt.getOutput(key), cxt.getSparkSession().createDataset(rddByTag, encoders.get(id)));
        }
      } else {
        // Persist as wide rows with one column per TupleTag to support different schemas
        Dataset<Tuple2<Integer, WindowedValue<Object>>> allTagsDS =
            inputDs.mapPartitions(doFnMapper, oneOfEncoder(encoders));
        allTagsDS.persist(storageLevel);

        // divide into separate output datasets per tag
        for (Entry<TupleTag<?>, Integer> e : tags.entrySet()) {
          TupleTag<Object> key = (TupleTag<Object>) e.getKey();
          Integer id = e.getValue();

          // Resolve specific column matching the tuple tag (by id)
          TypedColumn<Tuple2<Integer, WindowedValue<Object>>, WindowedValue<Object>> col =
              (TypedColumn) col(id.toString()).as(encoders.get(id));

          cxt.putDataset(cxt.getOutput(key), allTagsDS.filter(col.isNotNull()).select(col));
        }
      }
    } else {
      PCollection<OutputT> output = cxt.getOutput(mainOutputTag);
      Dataset<WindowedValue<OutputT>> mainDS =
          inputDs.mapPartitions(
              factory.create((tag, value) -> (WindowedValue<OutputT>) value),
              cxt.windowedEncoder(output.getCoder()));

      cxt.putDataset(output, mainDS);
    }
  }

  private List<Encoder<WindowedValue<Object>>> createEncoders(
      Map<TupleTag<?>, PCollection<?>> outputs, Iterable<TupleTag<?>> columns, Context ctx) {
    return Streams.stream(columns)
        .map(tag -> ctx.windowedEncoder(getCoder(outputs.get(tag), tag)))
        .collect(toList());
  }

  private Coder<Object> getCoder(@Nullable PCollection<?> pc, TupleTag<?> tag) {
    if (pc == null) {
      throw new NullPointerException("No PCollection for tag " + tag);
    }
    return (Coder<Object>) pc.getCoder();
  }

  // FIXME Better ways?
  private SideInputBroadcast createBroadcastSideInputs(
      Collection<PCollectionView<?>> sideInputs, Context context) {

    SideInputBroadcast sideInputBroadcast = new SideInputBroadcast();
    for (PCollectionView<?> sideInput : sideInputs) {
      PCollection<?> pc = sideInput.getPCollection();
      if (pc == null) {
        throw new NullPointerException("PCollection for SideInput is null");
      }
      Coder<? extends BoundedWindow> windowCoder =
          pc.getWindowingStrategy().getWindowFn().windowCoder();
      Coder<WindowedValue<?>> windowedValueCoder =
          (Coder<WindowedValue<?>>)
              (Coder<?>) WindowedValue.getFullCoder(pc.getCoder(), windowCoder);
      Dataset<WindowedValue<?>> broadcastSet = context.getSideInputDataset(sideInput);
      List<WindowedValue<?>> valuesList = broadcastSet.collectAsList();
      List<byte[]> codedValues = new ArrayList<>();
      for (WindowedValue<?> v : valuesList) {
        codedValues.add(CoderHelpers.toByteArray(v, windowedValueCoder));
      }

      sideInputBroadcast.add(
          sideInput.getTagInternal().getId(), context.broadcast(codedValues), windowedValueCoder);
    }
    return sideInputBroadcast;
  }

  private static <T> Collection<Entry<T, Integer>> zipwithIndex(Collection<T> col) {
    ArrayList<Entry<T, Integer>> zipped = new ArrayList<>(col.size());
    int i = 0;
    for (T t : col) {
      zipped.add(new SimpleImmutableEntry<>(t, i++));
    }
    return zipped;
  }
}
