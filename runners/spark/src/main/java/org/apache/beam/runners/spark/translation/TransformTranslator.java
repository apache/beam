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

import static org.apache.beam.runners.spark.io.hadoop.ShardNameBuilder.getOutputDirectory;
import static org.apache.beam.runners.spark.io.hadoop.ShardNameBuilder.getOutputFilePrefix;
import static org.apache.beam.runners.spark.io.hadoop.ShardNameBuilder.getOutputFileTemplate;
import static org.apache.beam.runners.spark.io.hadoop.ShardNameBuilder.replaceShardCount;

import org.apache.beam.runners.spark.coders.CoderHelpers;
import org.apache.beam.runners.spark.io.hadoop.HadoopIO;
import org.apache.beam.runners.spark.io.hadoop.ShardNameTemplateHelper;
import org.apache.beam.runners.spark.io.hadoop.TemplatedAvroKeyOutputFormat;
import org.apache.beam.runners.spark.io.hadoop.TemplatedTextOutputFormat;
import org.apache.beam.runners.spark.util.BroadcastHelper;
import org.apache.beam.runners.spark.util.ByteArray;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Combine;
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
import org.apache.beam.sdk.util.GroupAlsoByWindowsViaOutputBufferDoFn;
import org.apache.beam.sdk.util.GroupByKeyViaGroupByKeyOnly.GroupAlsoByWindow;
import org.apache.beam.sdk.util.GroupByKeyViaGroupByKeyOnly.GroupByKeyOnly;
import org.apache.beam.sdk.util.SystemReduceFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.WindowedValueCoder;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.util.state.InMemoryStateInternals;
import org.apache.beam.sdk.util.state.StateInternals;
import org.apache.beam.sdk.util.state.StateInternalsFactory;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import scala.Tuple2;

/**
 * Supports translation between a DataFlow transform, and Spark's operations on RDDs.
 */
public final class TransformTranslator {

  private TransformTranslator() {
  }

  /**
   * Getter of the field.
   */
  public static class FieldGetter {
    private final Map<String, Field> fields;

    public FieldGetter(Class<?> clazz) {
      this.fields = Maps.newHashMap();
      for (Field f : clazz.getDeclaredFields()) {
        f.setAccessible(true);
        this.fields.put(f.getName(), f);
      }
    }

    public <T> T get(String fieldname, Object value) {
      try {
        @SuppressWarnings("unchecked")
        T fieldValue = (T) fields.get(fieldname).get(value);
        return fieldValue;
      } catch (IllegalAccessException e) {
        throw new IllegalStateException(e);
      }
    }
  }

  private static <T> TransformEvaluator<Flatten.FlattenPCollectionList<T>> flattenPColl() {
    return new TransformEvaluator<Flatten.FlattenPCollectionList<T>>() {
      @SuppressWarnings("unchecked")
      @Override
      public void evaluate(Flatten.FlattenPCollectionList<T> transform, EvaluationContext context) {
        PCollectionList<T> pcs = context.getInput(transform);
        JavaRDD<WindowedValue<T>>[] rdds = new JavaRDD[pcs.size()];
        for (int i = 0; i < rdds.length; i++) {
          rdds[i] = (JavaRDD<WindowedValue<T>>) context.getRDD(pcs.get(i));
        }
        JavaRDD<WindowedValue<T>> rdd = context.getSparkContext().union(rdds);
        context.setOutputRDD(transform, rdd);
      }
    };
  }

  private static <K, V> TransformEvaluator<GroupByKeyOnly<K, V>> gbk() {
    return new TransformEvaluator<GroupByKeyOnly<K, V>>() {
      @Override
      public void evaluate(GroupByKeyOnly<K, V> transform, EvaluationContext context) {
        @SuppressWarnings("unchecked")
        JavaRDDLike<WindowedValue<KV<K, V>>, ?> inRDD =
            (JavaRDDLike<WindowedValue<KV<K, V>>, ?>) context.getInputRDD(transform);
        @SuppressWarnings("unchecked")
        KvCoder<K, V> coder = (KvCoder<K, V>) context.getInput(transform).getCoder();
        Coder<K> keyCoder = coder.getKeyCoder();
        Coder<V> valueCoder = coder.getValueCoder();

        // Use coders to convert objects in the PCollection to byte arrays, so they
        // can be transferred over the network for the shuffle.
        JavaRDDLike<WindowedValue<KV<K, Iterable<V>>>, ?> outRDD = fromPair(
              toPair(inRDD.map(WindowingHelpers.<KV<K, V>>unwindowFunction()))
            .mapToPair(CoderHelpers.toByteFunction(keyCoder, valueCoder))
            .groupByKey()
            .mapToPair(CoderHelpers.fromByteFunctionIterable(keyCoder, valueCoder)))
            // empty windows are OK here, see GroupByKey#evaluateHelper in the SDK
            .map(WindowingHelpers.<KV<K, Iterable<V>>>windowFunction());
        context.setOutputRDD(transform, outRDD);
      }
    };
  }

  private static <K, V, W extends BoundedWindow>
      TransformEvaluator<GroupAlsoByWindow<K, V>> gabw() {
    return new TransformEvaluator<GroupAlsoByWindow<K, V>>() {
      @Override
      public void evaluate(GroupAlsoByWindow<K, V> transform, EvaluationContext context) {
        @SuppressWarnings("unchecked")
        JavaRDDLike<WindowedValue<KV<K, Iterable<WindowedValue<V>>>>, ?> inRDD =
            (JavaRDDLike<WindowedValue<KV<K, Iterable<WindowedValue<V>>>>, ?>)
                context.getInputRDD(transform);

        Coder<KV<K, Iterable<WindowedValue<V>>>> inputCoder =
            context.getInput(transform).getCoder();
        Coder<K> keyCoder = transform.getKeyCoder(inputCoder);
        Coder<V> valueCoder = transform.getValueCoder(inputCoder);

        @SuppressWarnings("unchecked")
        KvCoder<K, Iterable<WindowedValue<V>>> inputKvCoder =
            (KvCoder<K, Iterable<WindowedValue<V>>>) context.getInput(transform).getCoder();
        Coder<Iterable<WindowedValue<V>>> inputValueCoder = inputKvCoder.getValueCoder();

        IterableCoder<WindowedValue<V>> inputIterableValueCoder =
            (IterableCoder<WindowedValue<V>>) inputValueCoder;
        Coder<WindowedValue<V>> inputIterableElementCoder = inputIterableValueCoder.getElemCoder();
        WindowedValueCoder<V> inputIterableWindowedValueCoder =
            (WindowedValueCoder<V>) inputIterableElementCoder;

        Coder<V> inputIterableElementValueCoder = inputIterableWindowedValueCoder.getValueCoder();

        @SuppressWarnings("unchecked")
        WindowingStrategy<?, W> windowingStrategy =
            (WindowingStrategy<?, W>) transform.getWindowingStrategy();

        DoFn<KV<K, Iterable<WindowedValue<V>>>, KV<K, Iterable<V>>> gabwDoFn =
            new GroupAlsoByWindowsViaOutputBufferDoFn<K, V, Iterable<V>, W>(
                windowingStrategy,
                new InMemoryStateInternalsFactory<K>(),
                SystemReduceFn.<K, V, W>buffering(inputIterableElementValueCoder));

        // GroupAlsoByWindow current uses a dummy in-memory StateInternals
        JavaRDDLike<WindowedValue<KV<K, Iterable<V>>>, ?> outRDD =
            inRDD.mapPartitions(
                new DoFnFunction<KV<K, Iterable<WindowedValue<V>>>, KV<K, Iterable<V>>>(
                    gabwDoFn, context.getRuntimeContext(), null));

        context.setOutputRDD(transform, outRDD);
      }
    };
  }

  private static final FieldGetter GROUPED_FG = new FieldGetter(Combine.GroupedValues.class);

  private static <K, InputT, OutputT> TransformEvaluator<Combine.GroupedValues<K, InputT, OutputT>>
  grouped() {
    return new TransformEvaluator<Combine.GroupedValues<K, InputT, OutputT>>() {
      @Override
      public void evaluate(Combine.GroupedValues<K, InputT, OutputT> transform,
                           EvaluationContext context) {
        Combine.KeyedCombineFn<K, InputT, ?, OutputT> keyed = GROUPED_FG.get("fn", transform);
        @SuppressWarnings("unchecked")
        JavaRDDLike<WindowedValue<KV<K, Iterable<InputT>>>, ?> inRDD =
            (JavaRDDLike<WindowedValue<KV<K, Iterable<InputT>>>, ?>) context.getInputRDD(transform);
        context.setOutputRDD(transform,
            inRDD.map(new KVFunction<>(keyed)));
      }
    };
  }

  private static final FieldGetter COMBINE_GLOBALLY_FG = new FieldGetter(Combine.Globally.class);

  private static <InputT, AccumT, OutputT> TransformEvaluator<Combine.Globally<InputT, OutputT>>
  combineGlobally() {
    return new TransformEvaluator<Combine.Globally<InputT, OutputT>>() {

      @Override
      public void evaluate(Combine.Globally<InputT, OutputT> transform, EvaluationContext context) {
        final Combine.CombineFn<InputT, AccumT, OutputT> globally =
            COMBINE_GLOBALLY_FG.get("fn", transform);

        @SuppressWarnings("unchecked")
        JavaRDDLike<WindowedValue<InputT>, ?> inRdd =
            (JavaRDDLike<WindowedValue<InputT>, ?>) context.getInputRDD(transform);

        final Coder<InputT> iCoder = context.getInput(transform).getCoder();
        final Coder<AccumT> aCoder;
        try {
          aCoder = globally.getAccumulatorCoder(
              context.getPipeline().getCoderRegistry(), iCoder);
        } catch (CannotProvideCoderException e) {
          throw new IllegalStateException("Could not determine coder for accumulator", e);
        }

        // Use coders to convert objects in the PCollection to byte arrays, so they
        // can be transferred over the network for the shuffle.
        JavaRDD<byte[]> inRddBytes = inRdd
            .map(WindowingHelpers.<InputT>unwindowFunction())
            .map(CoderHelpers.toByteFunction(iCoder));

        /*AccumT*/ byte[] acc = inRddBytes.aggregate(
            CoderHelpers.toByteArray(globally.createAccumulator(), aCoder),
            new Function2</*AccumT*/ byte[], /*InputT*/ byte[], /*AccumT*/ byte[]>() {
              @Override
              public /*AccumT*/ byte[] call(/*AccumT*/ byte[] ab, /*InputT*/ byte[] ib)
                  throws Exception {
                AccumT a = CoderHelpers.fromByteArray(ab, aCoder);
                InputT i = CoderHelpers.fromByteArray(ib, iCoder);
                return CoderHelpers.toByteArray(globally.addInput(a, i), aCoder);
              }
            },
            new Function2</*AccumT*/ byte[], /*AccumT*/ byte[], /*AccumT*/ byte[]>() {
              @Override
              public /*AccumT*/ byte[] call(/*AccumT*/ byte[] a1b, /*AccumT*/ byte[] a2b)
                  throws Exception {
                AccumT a1 = CoderHelpers.fromByteArray(a1b, aCoder);
                AccumT a2 = CoderHelpers.fromByteArray(a2b, aCoder);
                // don't use Guava's ImmutableList.of as values may be null
                List<AccumT> accumulators = Collections.unmodifiableList(Arrays.asList(a1, a2));
                AccumT merged = globally.mergeAccumulators(accumulators);
                return CoderHelpers.toByteArray(merged, aCoder);
              }
            }
        );
        OutputT output = globally.extractOutput(CoderHelpers.fromByteArray(acc, aCoder));

        Coder<OutputT> coder = context.getOutput(transform).getCoder();
        JavaRDD<byte[]> outRdd = context.getSparkContext().parallelize(
            // don't use Guava's ImmutableList.of as output may be null
            CoderHelpers.toByteArrays(Collections.singleton(output), coder));
        context.setOutputRDD(transform, outRdd.map(CoderHelpers.fromByteFunction(coder))
            .map(WindowingHelpers.<OutputT>windowFunction()));
      }
    };
  }

  private static final FieldGetter COMBINE_PERKEY_FG = new FieldGetter(Combine.PerKey.class);

  private static <K, InputT, AccumT, OutputT>
  TransformEvaluator<Combine.PerKey<K, InputT, OutputT>> combinePerKey() {
    return new TransformEvaluator<Combine.PerKey<K, InputT, OutputT>>() {
      @Override
      public void evaluate(Combine.PerKey<K, InputT, OutputT>
                               transform, EvaluationContext context) {
        final Combine.KeyedCombineFn<K, InputT, AccumT, OutputT> keyed =
            COMBINE_PERKEY_FG.get("fn", transform);
        @SuppressWarnings("unchecked")
        JavaRDDLike<WindowedValue<KV<K, InputT>>, ?> inRdd =
            (JavaRDDLike<WindowedValue<KV<K, InputT>>, ?>) context.getInputRDD(transform);

        @SuppressWarnings("unchecked")
        KvCoder<K, InputT> inputCoder = (KvCoder<K, InputT>)
            context.getInput(transform).getCoder();
        Coder<K> keyCoder = inputCoder.getKeyCoder();
        Coder<InputT> viCoder = inputCoder.getValueCoder();
        Coder<AccumT> vaCoder;
        try {
          vaCoder = keyed.getAccumulatorCoder(
              context.getPipeline().getCoderRegistry(), keyCoder, viCoder);
        } catch (CannotProvideCoderException e) {
          throw new IllegalStateException("Could not determine coder for accumulator", e);
        }
        Coder<KV<K, InputT>> kviCoder = KvCoder.of(keyCoder, viCoder);
        Coder<KV<K, AccumT>> kvaCoder = KvCoder.of(keyCoder, vaCoder);

        // We need to duplicate K as both the key of the JavaPairRDD as well as inside the value,
        // since the functions passed to combineByKey don't receive the associated key of each
        // value, and we need to map back into methods in Combine.KeyedCombineFn, which each
        // require the key in addition to the InputT's and AccumT's being merged/accumulated.
        // Once Spark provides a way to include keys in the arguments of combine/merge functions,
        // we won't need to duplicate the keys anymore.

        // Key has to bw windowed in order to group by window as well
        JavaPairRDD<WindowedValue<K>, WindowedValue<KV<K, InputT>>> inRddDuplicatedKeyPair =
            inRdd.flatMapToPair(
                new PairFlatMapFunction<WindowedValue<KV<K, InputT>>, WindowedValue<K>,
                    WindowedValue<KV<K, InputT>>>() {
                  @Override
                  public Iterable<Tuple2<WindowedValue<K>,
                      WindowedValue<KV<K, InputT>>>>
                  call(WindowedValue<KV<K, InputT>> kv) {
                      List<Tuple2<WindowedValue<K>,
                          WindowedValue<KV<K, InputT>>>> tuple2s =
                          Lists.newArrayListWithCapacity(kv.getWindows().size());
                      for (BoundedWindow boundedWindow: kv.getWindows()) {
                        WindowedValue<K> wk = WindowedValue.of(kv.getValue().getKey(),
                            boundedWindow.maxTimestamp(), boundedWindow, kv.getPane());
                        tuple2s.add(new Tuple2<>(wk, kv));
                      }
                    return tuple2s;
                  }
                });
        //-- windowed coders
        final WindowedValue.FullWindowedValueCoder<K> wkCoder =
                WindowedValue.FullWindowedValueCoder.of(keyCoder,
                context.getInput(transform).getWindowingStrategy().getWindowFn().windowCoder());
        final WindowedValue.FullWindowedValueCoder<KV<K, InputT>> wkviCoder =
                WindowedValue.FullWindowedValueCoder.of(kviCoder,
                context.getInput(transform).getWindowingStrategy().getWindowFn().windowCoder());
        final WindowedValue.FullWindowedValueCoder<KV<K, AccumT>> wkvaCoder =
                WindowedValue.FullWindowedValueCoder.of(kvaCoder,
                context.getInput(transform).getWindowingStrategy().getWindowFn().windowCoder());

        // Use coders to convert objects in the PCollection to byte arrays, so they
        // can be transferred over the network for the shuffle.
        JavaPairRDD<ByteArray, byte[]> inRddDuplicatedKeyPairBytes = inRddDuplicatedKeyPair
            .mapToPair(CoderHelpers.toByteFunction(wkCoder, wkviCoder));

        // The output of combineByKey will be "AccumT" (accumulator)
        // types rather than "OutputT" (final output types) since Combine.CombineFn
        // only provides ways to merge VAs, and no way to merge VOs.
        JavaPairRDD</*K*/ ByteArray, /*KV<K, AccumT>*/ byte[]> accumulatedBytes =
            inRddDuplicatedKeyPairBytes.combineByKey(
            new Function</*KV<K, InputT>*/ byte[], /*KV<K, AccumT>*/ byte[]>() {
              @Override
              public /*KV<K, AccumT>*/ byte[] call(/*KV<K, InputT>*/ byte[] input) {
                WindowedValue<KV<K, InputT>> wkvi =
                    CoderHelpers.fromByteArray(input, wkviCoder);
                AccumT va = keyed.createAccumulator(wkvi.getValue().getKey());
                va = keyed.addInput(wkvi.getValue().getKey(), va, wkvi.getValue().getValue());
                WindowedValue<KV<K, AccumT>> wkva =
                    WindowedValue.of(KV.of(wkvi.getValue().getKey(), va), wkvi.getTimestamp(),
                    wkvi.getWindows(), wkvi.getPane());
                return CoderHelpers.toByteArray(wkva, wkvaCoder);
              }
            },
            new Function2</*KV<K, AccumT>*/ byte[],
                /*KV<K, InputT>*/ byte[],
                /*KV<K, AccumT>*/ byte[]>() {
              @Override
              public /*KV<K, AccumT>*/ byte[] call(/*KV<K, AccumT>*/ byte[] acc,
                  /*KV<K, InputT>*/ byte[] input) {
                WindowedValue<KV<K, AccumT>> wkva =
                    CoderHelpers.fromByteArray(acc, wkvaCoder);
                WindowedValue<KV<K, InputT>> wkvi =
                    CoderHelpers.fromByteArray(input, wkviCoder);
                AccumT va =
                    keyed.addInput(wkva.getValue().getKey(), wkva.getValue().getValue(),
                    wkvi.getValue().getValue());
                wkva = WindowedValue.of(KV.of(wkva.getValue().getKey(), va), wkva.getTimestamp(),
                    wkva.getWindows(), wkva.getPane());
                return CoderHelpers.toByteArray(wkva, wkvaCoder);
              }
            },
            new Function2</*KV<K, AccumT>*/ byte[],
                /*KV<K, AccumT>*/ byte[],
                /*KV<K, AccumT>*/ byte[]>() {
              @Override
              public /*KV<K, AccumT>*/ byte[] call(/*KV<K, AccumT>*/ byte[] acc1,
                  /*KV<K, AccumT>*/ byte[] acc2) {
                WindowedValue<KV<K, AccumT>> wkva1 =
                    CoderHelpers.fromByteArray(acc1, wkvaCoder);
                WindowedValue<KV<K, AccumT>> wkva2 =
                    CoderHelpers.fromByteArray(acc2, wkvaCoder);
                AccumT va = keyed.mergeAccumulators(wkva1.getValue().getKey(),
                    // don't use Guava's ImmutableList.of as values may be null
                    Collections.unmodifiableList(Arrays.asList(wkva1.getValue().getValue(),
                    wkva2.getValue().getValue())));
                WindowedValue<KV<K, AccumT>> wkva =
                    WindowedValue.of(KV.of(wkva1.getValue().getKey(),
                    va), wkva1.getTimestamp(), wkva1.getWindows(), wkva1.getPane());
                return CoderHelpers.toByteArray(wkva, wkvaCoder);
              }
            });

        JavaPairRDD<WindowedValue<K>, WindowedValue<OutputT>> extracted = accumulatedBytes
            .mapToPair(CoderHelpers.fromByteFunction(wkCoder, wkvaCoder))
            .mapValues(
                new Function<WindowedValue<KV<K, AccumT>>, WindowedValue<OutputT>>() {
                  @Override
                  public WindowedValue<OutputT> call(WindowedValue<KV<K, AccumT>> acc) {
                    return WindowedValue.of(keyed.extractOutput(acc.getValue().getKey(),
                        acc.getValue().getValue()), acc.getTimestamp(),
                        acc.getWindows(), acc.getPane());
                  }
                });

        context.setOutputRDD(transform,
            fromPair(extracted)
            .map(new Function<KV<WindowedValue<K>, WindowedValue<OutputT>>,
                WindowedValue<KV<K, OutputT>>>() {
              @Override
              public WindowedValue<KV<K, OutputT>> call(KV<WindowedValue<K>,
                  WindowedValue<OutputT>> kwvo)
                  throws Exception {
                WindowedValue<OutputT> wvo = kwvo.getValue();
                KV<K, OutputT> kvo = KV.of(kwvo.getKey().getValue(), wvo.getValue());
                return WindowedValue.of(kvo, wvo.getTimestamp(), wvo.getWindows(), wvo.getPane());
              }
            }));
      }
    };
  }

  private static final class KVFunction<K, InputT, OutputT>
      implements Function<WindowedValue<KV<K, Iterable<InputT>>>,
      WindowedValue<KV<K, OutputT>>> {
    private final Combine.KeyedCombineFn<K, InputT, ?, OutputT> keyed;

     KVFunction(Combine.KeyedCombineFn<K, InputT, ?, OutputT> keyed) {
      this.keyed = keyed;
    }

    @Override
    public WindowedValue<KV<K, OutputT>> call(WindowedValue<KV<K,
        Iterable<InputT>>> windowedKv)
        throws Exception {
      KV<K, Iterable<InputT>> kv = windowedKv.getValue();
      return WindowedValue.of(KV.of(kv.getKey(), keyed.apply(kv.getKey(), kv.getValue())),
          windowedKv.getTimestamp(), windowedKv.getWindows(), windowedKv.getPane());
    }
  }

  private static <K, V> JavaPairRDD<K, V> toPair(JavaRDDLike<KV<K, V>, ?> rdd) {
    return rdd.mapToPair(new PairFunction<KV<K, V>, K, V>() {
      @Override
      public Tuple2<K, V> call(KV<K, V> kv) {
        return new Tuple2<>(kv.getKey(), kv.getValue());
      }
    });
  }

  private static <K, V> JavaRDDLike<KV<K, V>, ?> fromPair(JavaPairRDD<K, V> rdd) {
    return rdd.map(new Function<Tuple2<K, V>, KV<K, V>>() {
      @Override
      public KV<K, V> call(Tuple2<K, V> t2) {
        return KV.of(t2._1(), t2._2());
      }
    });
  }

  private static <InputT, OutputT> TransformEvaluator<ParDo.Bound<InputT, OutputT>> parDo() {
    return new TransformEvaluator<ParDo.Bound<InputT, OutputT>>() {
      @Override
      public void evaluate(ParDo.Bound<InputT, OutputT> transform, EvaluationContext context) {
        DoFnFunction<InputT, OutputT> dofn =
            new DoFnFunction<>(transform.getFn(),
                context.getRuntimeContext(),
                getSideInputs(transform.getSideInputs(), context));
        @SuppressWarnings("unchecked")
        JavaRDDLike<WindowedValue<InputT>, ?> inRDD =
            (JavaRDDLike<WindowedValue<InputT>, ?>) context.getInputRDD(transform);
        context.setOutputRDD(transform, inRDD.mapPartitions(dofn));
      }
    };
  }

  private static final FieldGetter MULTIDO_FG = new FieldGetter(ParDo.BoundMulti.class);

  private static <InputT, OutputT> TransformEvaluator<ParDo.BoundMulti<InputT, OutputT>> multiDo() {
    return new TransformEvaluator<ParDo.BoundMulti<InputT, OutputT>>() {
      @Override
      public void evaluate(ParDo.BoundMulti<InputT, OutputT> transform, EvaluationContext context) {
        TupleTag<OutputT> mainOutputTag = MULTIDO_FG.get("mainOutputTag", transform);
        MultiDoFnFunction<InputT, OutputT> multifn = new MultiDoFnFunction<>(
            transform.getFn(),
            context.getRuntimeContext(),
            mainOutputTag,
            getSideInputs(transform.getSideInputs(), context));

        @SuppressWarnings("unchecked")
        JavaRDDLike<WindowedValue<InputT>, ?> inRDD =
            (JavaRDDLike<WindowedValue<InputT>, ?>) context.getInputRDD(transform);
        JavaPairRDD<TupleTag<?>, WindowedValue<?>> all = inRDD
            .mapPartitionsToPair(multifn)
            .cache();

        PCollectionTuple pct = context.getOutput(transform);
        for (Map.Entry<TupleTag<?>, PCollection<?>> e : pct.getAll().entrySet()) {
          @SuppressWarnings("unchecked")
          JavaPairRDD<TupleTag<?>, WindowedValue<?>> filtered =
              all.filter(new TupleTagFilter(e.getKey()));
          @SuppressWarnings("unchecked")
          // Object is the best we can do since different outputs can have different tags
          JavaRDD<WindowedValue<Object>> values =
              (JavaRDD<WindowedValue<Object>>) (JavaRDD<?>) filtered.values();
          context.setRDD(e.getValue(), values);
        }
      }
    };
  }


  private static <T> TransformEvaluator<TextIO.Read.Bound<T>> readText() {
    return new TransformEvaluator<TextIO.Read.Bound<T>>() {
      @Override
      public void evaluate(TextIO.Read.Bound<T> transform, EvaluationContext context) {
        String pattern = transform.getFilepattern();
        JavaRDD<WindowedValue<String>> rdd = context.getSparkContext().textFile(pattern)
                .map(WindowingHelpers.<String>windowFunction());
        context.setOutputRDD(transform, rdd);
      }
    };
  }

  private static <T> TransformEvaluator<TextIO.Write.Bound<T>> writeText() {
    return new TransformEvaluator<TextIO.Write.Bound<T>>() {
      @Override
      public void evaluate(TextIO.Write.Bound<T> transform, EvaluationContext context) {
        @SuppressWarnings("unchecked")
        JavaPairRDD<T, Void> last =
            ((JavaRDDLike<WindowedValue<T>, ?>) context.getInputRDD(transform))
            .map(WindowingHelpers.<T>unwindowFunction())
            .mapToPair(new PairFunction<T, T,
                    Void>() {
              @Override
              public Tuple2<T, Void> call(T t) throws Exception {
                return new Tuple2<>(t, null);
              }
            });
        ShardTemplateInformation shardTemplateInfo =
            new ShardTemplateInformation(transform.getNumShards(),
                transform.getShardTemplate(), transform.getFilenamePrefix(),
                transform.getFilenameSuffix());
        writeHadoopFile(last, new Configuration(), shardTemplateInfo, Text.class,
            NullWritable.class, TemplatedTextOutputFormat.class);
      }
    };
  }

  private static <T> TransformEvaluator<AvroIO.Read.Bound<T>> readAvro() {
    return new TransformEvaluator<AvroIO.Read.Bound<T>>() {
      @Override
      public void evaluate(AvroIO.Read.Bound<T> transform, EvaluationContext context) {
        String pattern = transform.getFilepattern();
        JavaSparkContext jsc = context.getSparkContext();
        @SuppressWarnings("unchecked")
        JavaRDD<AvroKey<T>> avroFile = (JavaRDD<AvroKey<T>>) (JavaRDD<?>)
            jsc.newAPIHadoopFile(pattern,
                                 AvroKeyInputFormat.class,
                                 AvroKey.class, NullWritable.class,
                                 new Configuration()).keys();
        JavaRDD<WindowedValue<T>> rdd = avroFile.map(
            new Function<AvroKey<T>, T>() {
              @Override
              public T call(AvroKey<T> key) {
                return key.datum();
              }
            }).map(WindowingHelpers.<T>windowFunction());
        context.setOutputRDD(transform, rdd);
      }
    };
  }

  private static <T> TransformEvaluator<AvroIO.Write.Bound<T>> writeAvro() {
    return new TransformEvaluator<AvroIO.Write.Bound<T>>() {
      @Override
      public void evaluate(AvroIO.Write.Bound<T> transform, EvaluationContext context) {
        Job job;
        try {
          job = Job.getInstance();
        } catch (IOException e) {
          throw new IllegalStateException(e);
        }
        AvroJob.setOutputKeySchema(job, transform.getSchema());
        @SuppressWarnings("unchecked")
        JavaPairRDD<AvroKey<T>, NullWritable> last =
            ((JavaRDDLike<WindowedValue<T>, ?>) context.getInputRDD(transform))
            .map(WindowingHelpers.<T>unwindowFunction())
            .mapToPair(new PairFunction<T, AvroKey<T>, NullWritable>() {
              @Override
              public Tuple2<AvroKey<T>, NullWritable> call(T t) throws Exception {
                return new Tuple2<>(new AvroKey<>(t), NullWritable.get());
              }
            });
        ShardTemplateInformation shardTemplateInfo =
            new ShardTemplateInformation(transform.getNumShards(),
            transform.getShardTemplate(), transform.getFilenamePrefix(),
            transform.getFilenameSuffix());
        writeHadoopFile(last, job.getConfiguration(), shardTemplateInfo,
            AvroKey.class, NullWritable.class, TemplatedAvroKeyOutputFormat.class);
      }
    };
  }

  private static <K, V> TransformEvaluator<HadoopIO.Read.Bound<K, V>> readHadoop() {
    return new TransformEvaluator<HadoopIO.Read.Bound<K, V>>() {
      @Override
      public void evaluate(HadoopIO.Read.Bound<K, V> transform, EvaluationContext context) {
        String pattern = transform.getFilepattern();
        JavaSparkContext jsc = context.getSparkContext();
        @SuppressWarnings ("unchecked")
        JavaPairRDD<K, V> file = jsc.newAPIHadoopFile(pattern,
            transform.getFormatClass(),
            transform.getKeyClass(), transform.getValueClass(),
            new Configuration());
        JavaRDD<WindowedValue<KV<K, V>>> rdd =
            file.map(new Function<Tuple2<K, V>, KV<K, V>>() {
          @Override
          public KV<K, V> call(Tuple2<K, V> t2) throws Exception {
            return KV.of(t2._1(), t2._2());
          }
        }).map(WindowingHelpers.<KV<K, V>>windowFunction());
        context.setOutputRDD(transform, rdd);
      }
    };
  }

  private static <K, V> TransformEvaluator<HadoopIO.Write.Bound<K, V>> writeHadoop() {
    return new TransformEvaluator<HadoopIO.Write.Bound<K, V>>() {
      @Override
      public void evaluate(HadoopIO.Write.Bound<K, V> transform, EvaluationContext context) {
        @SuppressWarnings("unchecked")
        JavaPairRDD<K, V> last = ((JavaRDDLike<WindowedValue<KV<K, V>>, ?>) context
            .getInputRDD(transform))
            .map(WindowingHelpers.<KV<K, V>>unwindowFunction())
            .mapToPair(new PairFunction<KV<K, V>, K, V>() {
              @Override
              public Tuple2<K, V> call(KV<K, V> t) throws Exception {
                return new Tuple2<>(t.getKey(), t.getValue());
              }
            });
        ShardTemplateInformation shardTemplateInfo =
            new ShardTemplateInformation(transform.getNumShards(),
                transform.getShardTemplate(), transform.getFilenamePrefix(),
                transform.getFilenameSuffix());
        Configuration conf = new Configuration();
        for (Map.Entry<String, String> e : transform.getConfigurationProperties().entrySet()) {
          conf.set(e.getKey(), e.getValue());
        }
        writeHadoopFile(last, conf, shardTemplateInfo,
            transform.getKeyClass(), transform.getValueClass(), transform.getFormatClass());
      }
    };
  }

  private static final class ShardTemplateInformation {
    private final int numShards;
    private final String shardTemplate;
    private final String filenamePrefix;
    private final String filenameSuffix;

    private ShardTemplateInformation(int numShards, String shardTemplate, String
        filenamePrefix, String filenameSuffix) {
      this.numShards = numShards;
      this.shardTemplate = shardTemplate;
      this.filenamePrefix = filenamePrefix;
      this.filenameSuffix = filenameSuffix;
    }

    int getNumShards() {
      return numShards;
    }

    String getShardTemplate() {
      return shardTemplate;
    }

    String getFilenamePrefix() {
      return filenamePrefix;
    }

    String getFilenameSuffix() {
      return filenameSuffix;
    }
  }

  private static <K, V> void writeHadoopFile(JavaPairRDD<K, V> rdd, Configuration conf,
      ShardTemplateInformation shardTemplateInfo, Class<?> keyClass, Class<?> valueClass,
      Class<? extends FileOutputFormat> formatClass) {
    int numShards = shardTemplateInfo.getNumShards();
    String shardTemplate = shardTemplateInfo.getShardTemplate();
    String filenamePrefix = shardTemplateInfo.getFilenamePrefix();
    String filenameSuffix = shardTemplateInfo.getFilenameSuffix();
    if (numShards != 0) {
      // number of shards was set explicitly, so repartition
      rdd = rdd.repartition(numShards);
    }
    int actualNumShards = rdd.partitions().size();
    String template = replaceShardCount(shardTemplate, actualNumShards);
    String outputDir = getOutputDirectory(filenamePrefix, template);
    String filePrefix = getOutputFilePrefix(filenamePrefix, template);
    String fileTemplate = getOutputFileTemplate(filenamePrefix, template);

    conf.set(ShardNameTemplateHelper.OUTPUT_FILE_PREFIX, filePrefix);
    conf.set(ShardNameTemplateHelper.OUTPUT_FILE_TEMPLATE, fileTemplate);
    conf.set(ShardNameTemplateHelper.OUTPUT_FILE_SUFFIX, filenameSuffix);
    rdd.saveAsNewAPIHadoopFile(outputDir, keyClass, valueClass, formatClass, conf);
  }

  private static <T, W extends BoundedWindow> TransformEvaluator<Window.Bound<T>> window() {
    return new TransformEvaluator<Window.Bound<T>>() {
      @Override
      public void evaluate(Window.Bound<T> transform, EvaluationContext context) {
        @SuppressWarnings("unchecked")
        JavaRDDLike<WindowedValue<T>, ?> inRDD =
            (JavaRDDLike<WindowedValue<T>, ?>) context.getInputRDD(transform);

        @SuppressWarnings("unchecked")
        WindowFn<? super T, W> windowFn = (WindowFn<? super T, W>) transform.getWindowFn();

        // Avoid running assign windows if both source and destination are global window
        // or if the user has not specified the WindowFn (meaning they are just messing
        // with triggering or allowed lateness)
        if (windowFn == null
            || (context.getInput(transform).getWindowingStrategy().getWindowFn()
                    instanceof GlobalWindows
                && windowFn instanceof GlobalWindows)) {
          context.setOutputRDD(transform, inRDD);
        } else {
          DoFn<T, T> addWindowsDoFn = new AssignWindowsDoFn<>(windowFn);
          DoFnFunction<T, T> dofn =
              new DoFnFunction<>(addWindowsDoFn, context.getRuntimeContext(), null);
          context.setOutputRDD(transform, inRDD.mapPartitions(dofn));
        }
      }
    };
  }

  private static <T> TransformEvaluator<Create.Values<T>> create() {
    return new TransformEvaluator<Create.Values<T>>() {
      @Override
      public void evaluate(Create.Values<T> transform, EvaluationContext context) {
        Iterable<T> elems = transform.getElements();
        // Use a coder to convert the objects in the PCollection to byte arrays, so they
        // can be transferred over the network.
        Coder<T> coder = context.getOutput(transform).getCoder();
        context.setOutputRDDFromValues(transform, elems, coder);
      }
    };
  }

  private static <T> TransformEvaluator<View.AsSingleton<T>> viewAsSingleton() {
    return new TransformEvaluator<View.AsSingleton<T>>() {
      @Override
      public void evaluate(View.AsSingleton<T> transform, EvaluationContext context) {
        Iterable<? extends WindowedValue<?>> iter =
                context.getWindowedValues(context.getInput(transform));
        context.setPView(context.getOutput(transform), iter);
      }
    };
  }

  private static <T> TransformEvaluator<View.AsIterable<T>> viewAsIter() {
    return new TransformEvaluator<View.AsIterable<T>>() {
      @Override
      public void evaluate(View.AsIterable<T> transform, EvaluationContext context) {
        Iterable<? extends WindowedValue<?>> iter =
                context.getWindowedValues(context.getInput(transform));
        context.setPView(context.getOutput(transform), iter);
      }
    };
  }

  private static <ReadT, WriteT> TransformEvaluator<View.CreatePCollectionView<ReadT, WriteT>>
  createPCollView() {
    return new TransformEvaluator<View.CreatePCollectionView<ReadT, WriteT>>() {
      @Override
      public void evaluate(View.CreatePCollectionView<ReadT, WriteT> transform,
                           EvaluationContext context) {
        Iterable<? extends WindowedValue<?>> iter =
            context.getWindowedValues(context.getInput(transform));
        context.setPView(context.getOutput(transform), iter);
      }
    };
  }

  private static final class TupleTagFilter<V>
      implements Function<Tuple2<TupleTag<V>, WindowedValue<?>>, Boolean> {

    private final TupleTag<V> tag;

    private TupleTagFilter(TupleTag<V> tag) {
      this.tag = tag;
    }

    @Override
    public Boolean call(Tuple2<TupleTag<V>, WindowedValue<?>> input) {
      return tag.equals(input._1());
    }
  }

  private static Map<TupleTag<?>, BroadcastHelper<?>> getSideInputs(
      List<PCollectionView<?>> views,
      EvaluationContext context) {
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

  private static final Map<Class<? extends PTransform>, TransformEvaluator<?>> EVALUATORS = Maps
      .newHashMap();

  static {
    EVALUATORS.put(TextIO.Read.Bound.class, readText());
    EVALUATORS.put(TextIO.Write.Bound.class, writeText());
    EVALUATORS.put(AvroIO.Read.Bound.class, readAvro());
    EVALUATORS.put(AvroIO.Write.Bound.class, writeAvro());
    EVALUATORS.put(HadoopIO.Read.Bound.class, readHadoop());
    EVALUATORS.put(HadoopIO.Write.Bound.class, writeHadoop());
    EVALUATORS.put(ParDo.Bound.class, parDo());
    EVALUATORS.put(ParDo.BoundMulti.class, multiDo());
    EVALUATORS.put(GroupByKeyOnly.class, gbk());
    EVALUATORS.put(GroupAlsoByWindow.class, gabw());
    EVALUATORS.put(Combine.GroupedValues.class, grouped());
    EVALUATORS.put(Combine.Globally.class, combineGlobally());
    EVALUATORS.put(Combine.PerKey.class, combinePerKey());
    EVALUATORS.put(Flatten.FlattenPCollectionList.class, flattenPColl());
    EVALUATORS.put(Create.Values.class, create());
    EVALUATORS.put(View.AsSingleton.class, viewAsSingleton());
    EVALUATORS.put(View.AsIterable.class, viewAsIter());
    EVALUATORS.put(View.CreatePCollectionView.class, createPCollView());
    EVALUATORS.put(Window.Bound.class, window());
  }

  public static <TransformT extends PTransform<?, ?>> TransformEvaluator<TransformT>
  getTransformEvaluator(Class<TransformT> clazz) {
    @SuppressWarnings("unchecked")
    TransformEvaluator<TransformT> transform =
        (TransformEvaluator<TransformT>) EVALUATORS.get(clazz);
    if (transform == null) {
      throw new IllegalStateException("No TransformEvaluator registered for " + clazz);
    }
    return transform;
  }

  /**
   * Translator matches Dataflow transformation with the appropriate evaluator.
   */
  public static class Translator implements SparkPipelineTranslator {

    @Override
    public boolean hasTranslation(Class<? extends PTransform<?, ?>> clazz) {
      return EVALUATORS.containsKey(clazz);
    }

    @Override
    public <TransformT extends PTransform<?, ?>> TransformEvaluator<TransformT> translate(
        Class<TransformT> clazz) {
      return getTransformEvaluator(clazz);
    }
  }

  private static class InMemoryStateInternalsFactory<K> implements StateInternalsFactory<K>,
      Serializable {
    @Override
    public StateInternals<K> stateInternalsForKey(K key) {
      return InMemoryStateInternals.forKey(key);
    }
  }
}
