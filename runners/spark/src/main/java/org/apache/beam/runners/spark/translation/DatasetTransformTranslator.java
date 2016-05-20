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

package org.apache.beam.runners.spark.translation;

import org.apache.beam.runners.spark.coders.EncoderHelpers;
import org.apache.beam.runners.spark.util.BroadcastHelper;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.AssignWindowsDoFn;
import org.apache.beam.sdk.util.GroupByKeyViaGroupByKeyOnly.GroupByKeyOnly;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.KeyValueGroupedDataset;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import scala.Tuple2;

/**
 * Supports translation between a Beam transforms, and Spark's operations on {@link Dataset}s.
 */
public class DatasetTransformTranslator {

  private static final Logger LOG = LoggerFactory.getLogger(DatasetTransformTranslator.class);

  private DatasetTransformTranslator() {
  }

  private static DatasetEvaluationContext datasetEvaluationContext(EvaluationContext context) {
    return (DatasetEvaluationContext) context;
  }

  private static <K, V> TransformEvaluator<GroupByKeyOnly<K, V>> groupByKey() {
    return new TransformEvaluator<GroupByKeyOnly<K, V>>() {

      @Override
      @SuppressWarnings("unchecked")
      public void evaluate(GroupByKeyOnly<K, V> transform, EvaluationContext context) {
        DatasetEvaluationContext dec = datasetEvaluationContext(context);
        Dataset<WindowedValue<KV<K, V>>> inputDataset =
            (Dataset<WindowedValue<KV<K, V>>>) dec.getInputDataset(transform);
        // extract key to group by key only.
        KeyValueGroupedDataset<K, KV<K, V>> grouped =
            inputDataset.map(WindowingHelpers.<KV<K, V>>unwindowMapFunction(),
            EncoderHelpers.<KV<K, V>>encoder())
            .groupByKey(Functions.<K, V>extractKey(), EncoderHelpers.<K>encoder());
        // materialize grouped values - OOM hazard see KeyValueGroupedDataset#mapGroups.
        Dataset<KV<K, Iterable<V>>> materialized =
            grouped.mapGroups(Functions.<K, V>materializeGroupedKV(),
            EncoderHelpers.<KV<K, Iterable<V>>>encoder());
        // window the result.
        Dataset<WindowedValue<KV<K, Iterable<V>>>> windowedOutput =
            materialized.map(WindowingHelpers.<KV<K, Iterable<V>>>windowMapFunction(),
            EncoderHelpers.<WindowedValue<KV<K, Iterable<V>>>>encoder());
        dec.setOutputDataset(transform, windowedOutput);
      }
    };
  }

  private static <T> TransformEvaluator<Flatten.FlattenPCollectionList<T>> flattenPColl() {
    return new TransformEvaluator<Flatten.FlattenPCollectionList<T>>() {

      @Override
      @SuppressWarnings("unchecked")
      public void evaluate(Flatten.FlattenPCollectionList<T> transform, EvaluationContext context) {
        DatasetEvaluationContext dec = datasetEvaluationContext(context);
        PCollectionList<T> pCollectionList = context.getInput(transform);

        int size = pCollectionList.size();
        if (size == 0) {
          throw new IllegalArgumentException("Cannot flatten an empty PCollectionList");
        }
        Dataset<WindowedValue<T>> flattened =
          (Dataset<WindowedValue<T>>) dec.getDataset(pCollectionList.get(0));
        for (int i = 1; i < size; i++) {
          flattened = flattened.union(
          (Dataset<WindowedValue<T>>) dec.getDataset(pCollectionList.get(i)));
        }
        dec.setOutputDataset(transform, flattened);
      }
    };
  }

  private static <InputT, OutputT> TransformEvaluator<ParDo.Bound<InputT, OutputT>> parDo() {
    return new TransformEvaluator<ParDo.Bound<InputT, OutputT>>() {

      @Override
      @SuppressWarnings("unchecked")
      public void evaluate(ParDo.Bound<InputT, OutputT> transform, EvaluationContext context) {
        DatasetEvaluationContext dec = datasetEvaluationContext(context);

        DoFnFunction<InputT, OutputT> doFn =
            new DoFnFunction<>(transform.getFn(),
                               dec.getRuntimeContext(),
                               getSideInputs(transform.getSideInputs(), dec));

        Dataset<WindowedValue<InputT>> inputDataset =
            (Dataset<WindowedValue<InputT>>) dec.getInputDataset(transform);
        dec.setOutputDataset(transform, inputDataset.mapPartitions(doFn,
            EncoderHelpers.<WindowedValue<OutputT>>encoder()));

      }
    };
  }


  private static <T, W extends BoundedWindow> TransformEvaluator<Window.Bound<T>> window() {
    return new TransformEvaluator<Window.Bound<T>>() {

      @Override
      @SuppressWarnings("unchecked")
      public void evaluate(Window.Bound<T> transform, EvaluationContext context) {
        DatasetEvaluationContext dec = datasetEvaluationContext(context);
        Dataset<WindowedValue<T>> inputDataset = (Dataset<WindowedValue<T>>)
            dec.getInputDataset(transform);

        WindowFn<? super T, W> destWindowFn = (WindowFn<? super T, W>) transform.getWindowFn();
        WindowFn<? super T, W> sourceWindowFn =
            (WindowFn<? super T, W>) context.getInput(transform).getWindowingStrategy()
            .getWindowFn();

        // Avoid running assign windows if both source and destination are global window
        // or if the user has not specified the WindowFn (meaning they are just messing
        // with triggering or allowed lateness)
        if (destWindowFn == null
            || (sourceWindowFn instanceof GlobalWindows
                && destWindowFn instanceof GlobalWindows)) {
          dec.setOutputDataset(transform, inputDataset);
        } else {
          DoFn<T, T> addWindowsDoFn = new AssignWindowsDoFn<>(destWindowFn);
          DoFnFunction<T, T> doFn =
              new DoFnFunction<>(addWindowsDoFn, dec.getRuntimeContext(), null);
          dec.setOutputDataset(transform,
              inputDataset.mapPartitions(doFn, EncoderHelpers.<WindowedValue<T>>encoder()));
        }
      }
    };
  }

  private static <T> TransformEvaluator<Create.Values<T>> createValues() {
    return new TransformEvaluator<Create.Values<T>>() {

      @Override
      public void evaluate(Create.Values<T> transform, EvaluationContext context) {
        DatasetEvaluationContext dec = datasetEvaluationContext(context);

        // Use a coder to convert the objects in the PCollection to byte arrays, so they
        // can be transferred over the network.
        Coder<T> coder = dec.getOutput(transform).getCoder();
        dec.setOutputDatasetFromValues(transform, transform.getElements(), coder);
      }
    };
  }

  private static TransformEvaluator<PTransform<?, ?>> view() {
    return new TransformEvaluator<PTransform<?, ?>>() {

      @Override
      public void evaluate(PTransform<?, ?> transform, EvaluationContext context) {
        DatasetEvaluationContext dec = datasetEvaluationContext(context);
        Iterable<? extends WindowedValue<?>> iter =
            dec.getWindowedValues((PCollection<?>) dec.getInput(transform));
        dec.setPView((PCollectionView<?>) dec.getOutput(transform), iter);
      }
    };
  }

  private static <InputT, OutputT> TransformEvaluator<ParDo.BoundMulti<InputT, OutputT>> multiDo() {
    return new TransformEvaluator<ParDo.BoundMulti<InputT, OutputT>>() {

      @Override
      @SuppressWarnings("unchecked")
      public void evaluate(ParDo.BoundMulti<InputT, OutputT> transform, EvaluationContext context) {
        DatasetEvaluationContext dec = datasetEvaluationContext(context);

        MultiDoFnFunction<InputT, OutputT> multifn =
            new MultiDoFnFunction<>(transform.getFn(),
                                    dec.getRuntimeContext(),
                                    transform.getMainOutputTag(),
                                    getSideInputs(transform.getSideInputs(), dec));

        Dataset<WindowedValue<InputT>> inputDataset =
            (Dataset<WindowedValue<InputT>>) dec.getInputDataset(transform);
        Dataset<Tuple2<TupleTag<?>, WindowedValue<?>>> all =
            inputDataset.mapPartitions(multifn, EncoderHelpers.<TupleTag<?>,
            WindowedValue<?>>tuple2Encoder()).cache();

        PCollectionTuple pct = context.getOutput(transform);
        for (Map.Entry<TupleTag<?>, PCollection<?>> e : pct.getAll().entrySet()) {
          Dataset<Tuple2<TupleTag<?>, WindowedValue<?>>> filtered =
              all.filter(Functions.tupleTagFilter(e.getKey()));
          // Object is the best we can do since different outputs can have different tags
          Dataset<WindowedValue<Object>> values = (Dataset<WindowedValue<Object>>) (Dataset<?>)
          filtered.map(Functions.<TupleTag<?>, WindowedValue<?>>tuple2Get2(),
              EncoderHelpers.<WindowedValue<?>>encoder());
          dec.setDataset(e.getValue(), values);
        }
      }
    };
  }

  private static final Map<Class<? extends PTransform>, TransformEvaluator<?>>
      PRIMITIVES = Maps.newHashMap();

  private static final Map<Class<? extends PTransform>, TransformEvaluator<?>>
      EVALUATORS = Maps.newHashMap();

  static {
    //-------- SDK primitives
//    PRIMITIVES.put(Read.Bounded.class, readBounded());
//    PRIMITIVES.put(Read.Unbounded.class, readUnbounded());
    PRIMITIVES.put(GroupByKeyOnly.class, groupByKey());
    PRIMITIVES.put(Flatten.FlattenPCollectionList.class, flattenPColl());
    PRIMITIVES.put(ParDo.Bound.class, parDo());
    PRIMITIVES.put(Window.Bound.class, window());

    //-------- Composites
    EVALUATORS.put(Create.Values.class, createValues());
    EVALUATORS.put(View.AsSingleton.class, view());
    EVALUATORS.put(View.AsIterable.class, view());
    EVALUATORS.put(View.CreatePCollectionView.class, view());
//    EVALUATORS.put(Combine.GroupedValues.class, combineGrouped());
//    EVALUATORS.put(Combine.Globally.class, combineGlobally());
//    EVALUATORS.put(Combine.PerKey.class, combinePerKey());
    EVALUATORS.put(ParDo.BoundMulti.class, multiDo());

  }

  private static Map<TupleTag<?>, BroadcastHelper<?>>
  getSideInputs(List<PCollectionView<?>> views, DatasetEvaluationContext context) {
    if (views == null) {
      return ImmutableMap.of();
    } else {
      Map<TupleTag<?>, BroadcastHelper<?>> sideInputs = Maps.newHashMap();
      for (PCollectionView<?> view : views) {
        Iterable<? extends WindowedValue<?>> collectionView = context.getPCollectionView(view);
        Coder<Iterable<WindowedValue<?>>> coderInternal = view.getCoderInternal();
        @SuppressWarnings("unchecked")
        BroadcastHelper<?> helper =
                BroadcastHelper.create((Iterable<WindowedValue<?>>) collectionView, coderInternal);
        //broadcast side inputs
        helper.broadcast(context.getSparkContext());
        sideInputs.put(view.getTagInternal(), helper);
      }
      return sideInputs;
    }
  }

  private static class Functions {

    private static <K, V> MapFunction<KV<K, V>, K> extractKey() {

      return new MapFunction<KV<K, V>, K>() {
        @Override
        public K call(KV<K, V> kv) throws Exception {
          return kv.getKey();
        }
      };
    }

    private static <T1, T2> MapFunction<Tuple2<T1, T2>, T2> tuple2Get2() {

      return new MapFunction<Tuple2<T1, T2>, T2>() {
        @Override
        public T2 call(Tuple2<T1, T2> tuple2) throws Exception {
          return tuple2._2();
        }
      };
    }

    private static <K, V> MapGroupsFunction<K, KV<K, V> , KV<K, Iterable<V>>>
    materializeGroupedKV() {

      return new MapGroupsFunction<K, KV<K, V>, KV<K, Iterable<V>>>() {
        @Override
        public KV<K, Iterable<V>> call(K k, Iterator<KV<K, V>> iterator) throws Exception {
          List<V> values = Lists.newArrayList();
          while (iterator.hasNext()) {
            values.add(iterator.next().getValue());
          }
          return KV.of(k, Iterables.unmodifiableIterable(values));
        }
      };
    }

    private static FilterFunction<Tuple2<TupleTag<?>, WindowedValue<?>>>
    tupleTagFilter(final TupleTag<?> tag){

      return new FilterFunction<Tuple2<TupleTag<?>, WindowedValue<?>>>() {
        @Override
        public boolean call(Tuple2<TupleTag<?>, WindowedValue<?>> tuggedWindow) throws Exception {
          return tag.equals(tuggedWindow._1());
        }
      };
    }
  }

  /**
   * Translator to matches Beam transformations with the appropriate evaluator.
   */
  public static class Translator implements SparkPipelineTranslator {

    @Override
    public boolean hasTranslation(Class<? extends PTransform<?, ?>> clazz) {
      // check if the PTransform has an evaluator or primitive support
      return EVALUATORS.containsKey(clazz) || PRIMITIVES.containsKey(clazz);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <TransformT extends PTransform<?, ?>> TransformEvaluator<TransformT> translate(
        Class<TransformT> clazz) {
      // try EVALUATORS first since runner specific implementations should be preferred.
      LOG.debug("Look for a TransformEvaluator for transform {} in EVALUATORS registry",
          clazz.getSimpleName());
      TransformEvaluator<TransformT> transform =
          (TransformEvaluator<TransformT>) EVALUATORS.get(clazz);
      // try PRIMITIVES
      if (transform == null) {
        LOG.debug("Look for a TransformEvaluator for transform {} in PRIMITIVES registry",
            clazz.getSimpleName());
        transform = (TransformEvaluator<TransformT>) PRIMITIVES.get(clazz);
      }
      if (transform == null) {
        throw new IllegalStateException("No TransformEvaluator registered for " + clazz);
      }
      return transform;
    }
  }
}
