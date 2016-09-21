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

import static com.google.common.base.Preconditions.checkState;
import static org.apache.beam.runners.spark.io.hadoop.ShardNameBuilder.getOutputDirectory;
import static org.apache.beam.runners.spark.io.hadoop.ShardNameBuilder.getOutputFilePrefix;
import static org.apache.beam.runners.spark.io.hadoop.ShardNameBuilder.getOutputFileTemplate;
import static org.apache.beam.runners.spark.io.hadoop.ShardNameBuilder.replaceShardCount;

import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.beam.runners.core.AssignWindowsDoFn;
import org.apache.beam.runners.core.GroupByKeyViaGroupByKeyOnly.GroupAlsoByWindow;
import org.apache.beam.runners.core.GroupByKeyViaGroupByKeyOnly.GroupByKeyOnly;
import org.apache.beam.runners.spark.aggregators.AccumulatorSingleton;
import org.apache.beam.runners.spark.aggregators.NamedAggregators;
import org.apache.beam.runners.spark.coders.CoderHelpers;
import org.apache.beam.runners.spark.io.hadoop.HadoopIO;
import org.apache.beam.runners.spark.io.hadoop.ShardNameTemplateHelper;
import org.apache.beam.runners.spark.io.hadoop.TemplatedAvroKeyOutputFormat;
import org.apache.beam.runners.spark.io.hadoop.TemplatedTextOutputFormat;
import org.apache.beam.runners.spark.util.BroadcastHelper;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.OldDoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;


/**
 * Supports translation between a Beam transform, and Spark's operations on RDDs.
 */
public final class TransformTranslator {

  private TransformTranslator() {
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

  private static <K, V> TransformEvaluator<GroupByKeyOnly<K, V>> gbko() {
    return new TransformEvaluator<GroupByKeyOnly<K, V>>() {
      @Override
      public void evaluate(GroupByKeyOnly<K, V> transform, EvaluationContext context) {
        @SuppressWarnings("unchecked")
        JavaRDD<WindowedValue<KV<K, V>>> inRDD =
            (JavaRDD<WindowedValue<KV<K, V>>>) context.getInputRDD(transform);

        @SuppressWarnings("unchecked")
        final KvCoder<K, V> coder = (KvCoder<K, V>) context.getInput(transform).getCoder();

        context.setOutputRDD(transform, GroupCombineFunctions.groupByKeyOnly(inRDD, coder));
      }
    };
  }

  private static <K, V, W extends BoundedWindow>
      TransformEvaluator<GroupAlsoByWindow<K, V>> gabw() {
    return new TransformEvaluator<GroupAlsoByWindow<K, V>>() {
      @Override
      public void evaluate(GroupAlsoByWindow<K, V> transform, EvaluationContext context) {
        @SuppressWarnings("unchecked")
        JavaRDD<WindowedValue<KV<K, Iterable<WindowedValue<V>>>>> inRDD =
            (JavaRDD<WindowedValue<KV<K, Iterable<WindowedValue<V>>>>>)
                context.getInputRDD(transform);

        @SuppressWarnings("unchecked")
        final KvCoder<K, Iterable<WindowedValue<V>>> inputKvCoder =
            (KvCoder<K, Iterable<WindowedValue<V>>>) context.getInput(transform).getCoder();

        final Accumulator<NamedAggregators> accum =
            AccumulatorSingleton.getInstance(context.getSparkContext());

        context.setOutputRDD(transform, GroupCombineFunctions.groupAlsoByWindow(inRDD, transform,
            context.getRuntimeContext(), accum, inputKvCoder));
      }
    };
  }

  private static <K, InputT, OutputT> TransformEvaluator<Combine.GroupedValues<K, InputT, OutputT>>
  grouped() {
    return new TransformEvaluator<Combine.GroupedValues<K, InputT, OutputT>>() {
      @Override
      public void evaluate(Combine.GroupedValues<K, InputT, OutputT> transform,
                           EvaluationContext context) {
        @SuppressWarnings("unchecked")
        JavaRDDLike<WindowedValue<KV<K, Iterable<InputT>>>, ?> inRDD =
            (JavaRDDLike<WindowedValue<KV<K, Iterable<InputT>>>, ?>)
                context.getInputRDD(transform);
        context.setOutputRDD(transform, inRDD.map(
            new TranslationUtils.CombineGroupedValues<>(transform)));
      }
    };
  }

  private static <InputT, AccumT, OutputT> TransformEvaluator<Combine.Globally<InputT, OutputT>>
  combineGlobally() {
    return new TransformEvaluator<Combine.Globally<InputT, OutputT>>() {

      @Override
      public void evaluate(Combine.Globally<InputT, OutputT> transform, EvaluationContext context) {
        @SuppressWarnings("unchecked")
        JavaRDD<WindowedValue<InputT>> inRdd =
            (JavaRDD<WindowedValue<InputT>>) context.getInputRDD(transform);

        @SuppressWarnings("unchecked")
        final Combine.CombineFn<InputT, AccumT, OutputT> globally =
            (Combine.CombineFn<InputT, AccumT, OutputT>) transform.getFn();

        final Coder<InputT> iCoder = context.getInput(transform).getCoder();
        final Coder<AccumT> aCoder;
        try {
          aCoder = globally.getAccumulatorCoder(
              context.getPipeline().getCoderRegistry(), iCoder);
        } catch (CannotProvideCoderException e) {
          throw new IllegalStateException("Could not determine coder for accumulator", e);
        }

        final Coder<OutputT> oCoder = context.getOutput(transform).getCoder();
        JavaRDD<byte[]> outRdd = context.getSparkContext().parallelize(
            // don't use Guava's ImmutableList.of as output may be null
            CoderHelpers.toByteArrays(Collections.singleton(
                GroupCombineFunctions.combineGlobally(inRdd, globally, iCoder, aCoder)), oCoder));
        context.setOutputRDD(transform, outRdd.map(CoderHelpers.fromByteFunction(oCoder))
            .map(WindowingHelpers.<OutputT>windowFunction()));
      }
    };
  }

  private static <K, InputT, AccumT, OutputT>
  TransformEvaluator<Combine.PerKey<K, InputT, OutputT>> combinePerKey() {
    return new TransformEvaluator<Combine.PerKey<K, InputT, OutputT>>() {
      @Override
      public void evaluate(Combine.PerKey<K, InputT, OutputT> transform,
                           EvaluationContext context) {
        @SuppressWarnings("unchecked")
        final Combine.KeyedCombineFn<K, InputT, AccumT, OutputT> keyed =
            (Combine.KeyedCombineFn<K, InputT, AccumT, OutputT>) transform.getFn();

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

        @SuppressWarnings("unchecked")
        JavaRDD<WindowedValue<KV<K, InputT>>> inRdd =
                (JavaRDD<WindowedValue<KV<K, InputT>>>) context.getInputRDD(transform);

        context.setOutputRDD(transform, GroupCombineFunctions.combinePerKey(inRdd, keyed, wkCoder,
            wkviCoder, wkvaCoder));
      }
    };
  }

  private static <InputT, OutputT> TransformEvaluator<ParDo.Bound<InputT, OutputT>> parDo() {
    return new TransformEvaluator<ParDo.Bound<InputT, OutputT>>() {
      @Override
      public void evaluate(ParDo.Bound<InputT, OutputT> transform, EvaluationContext context) {
        @SuppressWarnings("unchecked")
        JavaRDDLike<WindowedValue<InputT>, ?> inRDD =
            (JavaRDDLike<WindowedValue<InputT>, ?>) context.getInputRDD(transform);
        Accumulator<NamedAggregators> accum =
            AccumulatorSingleton.getInstance(context.getSparkContext());
        Map<TupleTag<?>, BroadcastHelper<?>> sideInputs =
            TranslationUtils.getSideInputs(transform.getSideInputs(), context);
        context.setOutputRDD(transform,
            inRDD.mapPartitions(new DoFnFunction<>(accum, transform.getFn(),
                context.getRuntimeContext(), sideInputs)));
      }
    };
  }

  private static <InputT, OutputT> TransformEvaluator<ParDo.BoundMulti<InputT, OutputT>>
  multiDo() {
    return new TransformEvaluator<ParDo.BoundMulti<InputT, OutputT>>() {
      @Override
      public void evaluate(ParDo.BoundMulti<InputT, OutputT> transform, EvaluationContext context) {
        @SuppressWarnings("unchecked")
        JavaRDDLike<WindowedValue<InputT>, ?> inRDD =
            (JavaRDDLike<WindowedValue<InputT>, ?>) context.getInputRDD(transform);
        Accumulator<NamedAggregators> accum =
            AccumulatorSingleton.getInstance(context.getSparkContext());
        JavaPairRDD<TupleTag<?>, WindowedValue<?>> all = inRDD
            .mapPartitionsToPair(
                new MultiDoFnFunction<>(accum, transform.getFn(), context.getRuntimeContext(),
                transform.getMainOutputTag(), TranslationUtils.getSideInputs(
                    transform.getSideInputs(), context)))
                        .cache();
        PCollectionTuple pct = context.getOutput(transform);
        for (Map.Entry<TupleTag<?>, PCollection<?>> e : pct.getAll().entrySet()) {
          @SuppressWarnings("unchecked")
          JavaPairRDD<TupleTag<?>, WindowedValue<?>> filtered =
              all.filter(new TranslationUtils.TupleTagFilter(e.getKey()));
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

        if (TranslationUtils.skipAssignWindows(transform, context)) {
          context.setOutputRDD(transform, inRDD);
        } else {
          @SuppressWarnings("unchecked")
          WindowFn<? super T, W> windowFn = (WindowFn<? super T, W>) transform.getWindowFn();
          OldDoFn<T, T> addWindowsDoFn = new AssignWindowsDoFn<>(windowFn);
          Accumulator<NamedAggregators> accum =
              AccumulatorSingleton.getInstance(context.getSparkContext());
          context.setOutputRDD(transform,
              inRDD.mapPartitions(new DoFnFunction<>(accum, addWindowsDoFn,
                  context.getRuntimeContext(), null)));
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
    EVALUATORS.put(GroupByKeyOnly.class, gbko());
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

  /**
   * Translator matches Beam transformation with the appropriate evaluator.
   */
  public static class Translator implements SparkPipelineTranslator {

    @Override
    public boolean hasTranslation(Class<? extends PTransform<?, ?>> clazz) {
      return EVALUATORS.containsKey(clazz);
    }

    @Override
    public <TransformT extends PTransform<?, ?>> TransformEvaluator<TransformT>
        translateBounded (Class<TransformT> clazz) {
      @SuppressWarnings("unchecked") TransformEvaluator<TransformT> transformEvaluator =
          (TransformEvaluator<TransformT>) EVALUATORS.get(clazz);
      checkState(transformEvaluator != null,
          "No TransformEvaluator registered for BOUNDED transform %s", clazz);
      return transformEvaluator;
    }

    @Override
    public <TransformT extends PTransform<?, ?>> TransformEvaluator<TransformT>
        translateUnbounded(Class<TransformT> clazz) {
      throw new IllegalStateException("TransformTranslator used in a batch pipeline only "
          + "supports BOUNDED transforms.");
    }
  }
}
