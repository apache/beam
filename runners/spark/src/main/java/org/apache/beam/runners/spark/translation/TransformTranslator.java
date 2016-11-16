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
import java.util.Map;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.beam.runners.core.AssignWindowsDoFn;
import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.runners.spark.aggregators.AccumulatorSingleton;
import org.apache.beam.runners.spark.aggregators.NamedAggregators;
import org.apache.beam.runners.spark.io.SourceRDD;
import org.apache.beam.runners.spark.io.hadoop.HadoopIO;
import org.apache.beam.runners.spark.io.hadoop.ShardNameTemplateHelper;
import org.apache.beam.runners.spark.io.hadoop.TemplatedAvroKeyOutputFormat;
import org.apache.beam.runners.spark.io.hadoop.TemplatedTextOutputFormat;
import org.apache.beam.runners.spark.util.BroadcastHelper;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.CombineWithContext;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.OldDoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.CombineFnUtil;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
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
        JavaRDD<WindowedValue<T>> unionRDD;
        if (pcs.size() == 0) {
          unionRDD = context.getSparkContext().emptyRDD();
        } else {
          JavaRDD<WindowedValue<T>>[] rdds = new JavaRDD[pcs.size()];
          for (int i = 0; i < rdds.length; i++) {
            rdds[i] = ((BoundedDataset<T>) context.borrowDataset(pcs.get(i))).getRDD();
          }
          unionRDD = context.getSparkContext().union(rdds);
        }
        context.putDataset(transform, new BoundedDataset<>(unionRDD));
      }
    };
  }

  private static <K, V> TransformEvaluator<GroupByKey<K, V>> groupByKey() {
    return new TransformEvaluator<GroupByKey<K, V>>() {
      @Override
      public void evaluate(GroupByKey<K, V> transform, EvaluationContext context) {
        @SuppressWarnings("unchecked")
        JavaRDD<WindowedValue<KV<K, V>>> inRDD =
            ((BoundedDataset<KV<K, V>>) context.borrowDataset(transform)).getRDD();

        @SuppressWarnings("unchecked")
        final KvCoder<K, V> coder = (KvCoder<K, V>) context.getInput(transform).getCoder();

        final Accumulator<NamedAggregators> accum =
                AccumulatorSingleton.getInstance(context.getSparkContext());

        context.putDataset(transform,
            new BoundedDataset<>(GroupCombineFunctions.groupByKey(inRDD, accum, coder,
                context.getRuntimeContext(), context.getInput(transform).getWindowingStrategy())));
      }
    };
  }

  private static <K, InputT, OutputT> TransformEvaluator<Combine.GroupedValues<K, InputT, OutputT>>
  combineGrouped() {
    return new TransformEvaluator<Combine.GroupedValues<K, InputT, OutputT>>() {
      @Override
      public void evaluate(Combine.GroupedValues<K, InputT, OutputT> transform,
                           EvaluationContext context) {
        // get the applied combine function.
        PCollection<? extends KV<K, ? extends Iterable<InputT>>> input =
            context.getInput(transform);
        WindowingStrategy<?, ?> windowingStrategy = input.getWindowingStrategy();
        @SuppressWarnings("unchecked")
        CombineWithContext.KeyedCombineFnWithContext<K, InputT, ?, OutputT> fn =
            (CombineWithContext.KeyedCombineFnWithContext<K, InputT, ?, OutputT>)
                CombineFnUtil.toFnWithContext(transform.getFn());

        @SuppressWarnings("unchecked")
        JavaRDD<WindowedValue<KV<K, Iterable<InputT>>>> inRDD =
            ((BoundedDataset<KV<K, Iterable<InputT>>>)
                context.borrowDataset(transform)).getRDD();

        SparkKeyedCombineFn<K, InputT, ?, OutputT> combineFnWithContext =
            new SparkKeyedCombineFn<>(fn, context.getRuntimeContext(),
                TranslationUtils.getSideInputs(transform.getSideInputs(), context),
                windowingStrategy);
        context.putDataset(transform, new BoundedDataset<>(inRDD.map(new TranslationUtils
            .CombineGroupedValues<>(
            combineFnWithContext))));
      }
    };
  }

  private static <InputT, AccumT, OutputT> TransformEvaluator<Combine.Globally<InputT, OutputT>>
  combineGlobally() {
    return new TransformEvaluator<Combine.Globally<InputT, OutputT>>() {

      @Override
      public void evaluate(Combine.Globally<InputT, OutputT> transform, EvaluationContext context) {
        final PCollection<InputT> input = context.getInput(transform);
        // serializable arguments to pass.
        final Coder<InputT> iCoder = context.getInput(transform).getCoder();
        final Coder<OutputT> oCoder = context.getOutput(transform).getCoder();
        @SuppressWarnings("unchecked")
        final CombineWithContext.CombineFnWithContext<InputT, AccumT, OutputT> combineFn =
            (CombineWithContext.CombineFnWithContext<InputT, AccumT, OutputT>)
                CombineFnUtil.toFnWithContext(transform.getFn());
        final WindowingStrategy<?, ?> windowingStrategy = input.getWindowingStrategy();
        final SparkRuntimeContext runtimeContext = context.getRuntimeContext();
        final Map<TupleTag<?>, KV<WindowingStrategy<?, ?>, BroadcastHelper<?>>> sideInputs =
            TranslationUtils.getSideInputs(transform.getSideInputs(), context);
        final boolean hasDefault = transform.isInsertDefault();

        @SuppressWarnings("unchecked")
        JavaRDD<WindowedValue<InputT>> inRdd =
            ((BoundedDataset<InputT>) context.borrowDataset(transform)).getRDD();

        context.putDataset(transform, new BoundedDataset<>(GroupCombineFunctions
            .combineGlobally(inRdd, combineFn,
                iCoder, oCoder, runtimeContext, windowingStrategy, sideInputs, hasDefault)));
      }
    };
  }

  private static <K, InputT, AccumT, OutputT>
  TransformEvaluator<Combine.PerKey<K, InputT, OutputT>> combinePerKey() {
    return new TransformEvaluator<Combine.PerKey<K, InputT, OutputT>>() {
      @Override
      public void evaluate(Combine.PerKey<K, InputT, OutputT> transform,
                           EvaluationContext context) {
        final PCollection<KV<K, InputT>> input = context.getInput(transform);
        // serializable arguments to pass.
        @SuppressWarnings("unchecked")
        final KvCoder<K, InputT> inputCoder =
            (KvCoder<K, InputT>) context.getInput(transform).getCoder();
        @SuppressWarnings("unchecked")
        final CombineWithContext.KeyedCombineFnWithContext<K, InputT, AccumT, OutputT> combineFn =
            (CombineWithContext.KeyedCombineFnWithContext<K, InputT, AccumT, OutputT>)
                CombineFnUtil.toFnWithContext(transform.getFn());
        final WindowingStrategy<?, ?> windowingStrategy = input.getWindowingStrategy();
        final SparkRuntimeContext runtimeContext = context.getRuntimeContext();
        final Map<TupleTag<?>, KV<WindowingStrategy<?, ?>, BroadcastHelper<?>>> sideInputs =
            TranslationUtils.getSideInputs(transform.getSideInputs(), context);

        @SuppressWarnings("unchecked")
        JavaRDD<WindowedValue<KV<K, InputT>>> inRdd =
            ((BoundedDataset<KV<K, InputT>>) context.borrowDataset(transform)).getRDD();

        context.putDataset(transform, new BoundedDataset<>(GroupCombineFunctions
            .combinePerKey(inRdd, combineFn,
                inputCoder, runtimeContext, windowingStrategy, sideInputs)));
      }
    };
  }

  private static <InputT, OutputT> TransformEvaluator<ParDo.Bound<InputT, OutputT>> parDo() {
    return new TransformEvaluator<ParDo.Bound<InputT, OutputT>>() {
      @Override
      public void evaluate(ParDo.Bound<InputT, OutputT> transform, EvaluationContext context) {
        DoFn<InputT, OutputT> doFn = transform.getNewFn();
        if (DoFnSignatures.getSignature(doFn.getClass()).stateDeclarations().size() > 0) {
          throw new UnsupportedOperationException(
              String.format(
                  "Found %s annotations on %s, but %s cannot yet be used with state in the %s.",
                  DoFn.StateId.class.getSimpleName(),
                  doFn.getClass().getName(),
                  DoFn.class.getSimpleName(),
                  SparkRunner.class.getSimpleName()));
        }
        @SuppressWarnings("unchecked")
        JavaRDD<WindowedValue<InputT>> inRDD =
            ((BoundedDataset<InputT>) context.borrowDataset(transform)).getRDD();
        @SuppressWarnings("unchecked")
        final WindowFn<Object, ?> windowFn =
            (WindowFn<Object, ?>) context.getInput(transform).getWindowingStrategy().getWindowFn();
        Accumulator<NamedAggregators> accum =
            AccumulatorSingleton.getInstance(context.getSparkContext());
        Map<TupleTag<?>, KV<WindowingStrategy<?, ?>, BroadcastHelper<?>>> sideInputs =
            TranslationUtils.getSideInputs(transform.getSideInputs(), context);
        context.putDataset(transform,
            new BoundedDataset<>(inRDD.mapPartitions(new DoFnFunction<>(accum, transform.getFn(),
                context.getRuntimeContext(), sideInputs, windowFn))));
      }
    };
  }

  private static <InputT, OutputT> TransformEvaluator<ParDo.BoundMulti<InputT, OutputT>>
  multiDo() {
    return new TransformEvaluator<ParDo.BoundMulti<InputT, OutputT>>() {
      @Override
      public void evaluate(ParDo.BoundMulti<InputT, OutputT> transform, EvaluationContext context) {
        DoFn<InputT, OutputT> doFn = transform.getNewFn();
        if (DoFnSignatures.getSignature(doFn.getClass()).stateDeclarations().size() > 0) {
          throw new UnsupportedOperationException(
              String.format(
                  "Found %s annotations on %s, but %s cannot yet be used with state in the %s.",
                  DoFn.StateId.class.getSimpleName(),
                  doFn.getClass().getName(),
                  DoFn.class.getSimpleName(),
                  SparkRunner.class.getSimpleName()));
        }
        @SuppressWarnings("unchecked")
        JavaRDD<WindowedValue<InputT>> inRDD =
            ((BoundedDataset<InputT>) context.borrowDataset(transform)).getRDD();
        @SuppressWarnings("unchecked")
        final WindowFn<Object, ?> windowFn =
            (WindowFn<Object, ?>) context.getInput(transform).getWindowingStrategy().getWindowFn();
        Accumulator<NamedAggregators> accum =
            AccumulatorSingleton.getInstance(context.getSparkContext());
        JavaPairRDD<TupleTag<?>, WindowedValue<?>> all = inRDD
            .mapPartitionsToPair(
                new MultiDoFnFunction<>(accum, transform.getFn(), context.getRuntimeContext(),
                transform.getMainOutputTag(), TranslationUtils.getSideInputs(
                    transform.getSideInputs(), context), windowFn)).cache();
        PCollectionTuple pct = context.getOutput(transform);
        for (Map.Entry<TupleTag<?>, PCollection<?>> e : pct.getAll().entrySet()) {
          @SuppressWarnings("unchecked")
          JavaPairRDD<TupleTag<?>, WindowedValue<?>> filtered =
              all.filter(new TranslationUtils.TupleTagFilter(e.getKey()));
          @SuppressWarnings("unchecked")
          // Object is the best we can do since different outputs can have different tags
          JavaRDD<WindowedValue<Object>> values =
              (JavaRDD<WindowedValue<Object>>) (JavaRDD<?>) filtered.values();
          context.putDataset(e.getValue(), new BoundedDataset<>(values));
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
        context.putDataset(transform, new BoundedDataset<>(rdd));
      }
    };
  }

  private static <T> TransformEvaluator<TextIO.Write.Bound<T>> writeText() {
    return new TransformEvaluator<TextIO.Write.Bound<T>>() {
      @Override
      public void evaluate(TextIO.Write.Bound<T> transform, EvaluationContext context) {
        @SuppressWarnings("unchecked")
        JavaPairRDD<T, Void> last =
            ((BoundedDataset<T>) context.borrowDataset(transform)).getRDD()
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
        context.putDataset(transform, new BoundedDataset<>(rdd));
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
            ((BoundedDataset<T>) context.borrowDataset(transform)).getRDD()
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

  private static <T> TransformEvaluator<Read.Bounded<T>> readBounded() {
    return new TransformEvaluator<Read.Bounded<T>>() {
      @Override
      public void evaluate(Read.Bounded<T> transform, EvaluationContext context) {
        final JavaSparkContext jsc = context.getSparkContext();
        final SparkRuntimeContext runtimeContext = context.getRuntimeContext();
        // create an RDD from a BoundedSource.
        JavaRDD<WindowedValue<T>> input = new SourceRDD.Bounded<>(
            jsc.sc(), transform.getSource(), runtimeContext).toJavaRDD();
        // cache to avoid re-evaluation of the source by Spark's lazy DAG evaluation.
        context.putDataset(transform, new BoundedDataset<>(input.cache()));
      }
    };
  }

  private static <K, V> TransformEvaluator<HadoopIO.Read.Bound<K, V>> readHadoop() {
    return new TransformEvaluator<HadoopIO.Read.Bound<K, V>>() {
      @Override
      public void evaluate(HadoopIO.Read.Bound<K, V> transform, EvaluationContext context) {
        String pattern = transform.getFilepattern();
        JavaSparkContext jsc = context.getSparkContext();
        @SuppressWarnings("unchecked")
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
        context.putDataset(transform, new BoundedDataset<>(rdd));
      }
    };
  }

  private static <K, V> TransformEvaluator<HadoopIO.Write.Bound<K, V>> writeHadoop() {
    return new TransformEvaluator<HadoopIO.Write.Bound<K, V>>() {
      @Override
      public void evaluate(HadoopIO.Write.Bound<K, V> transform, EvaluationContext context) {
        @SuppressWarnings("unchecked")
        JavaPairRDD<K, V> last = ((BoundedDataset<KV<K, V>>) context.borrowDataset(transform))
            .getRDD()
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
        JavaRDD<WindowedValue<T>> inRDD =
            ((BoundedDataset<T>) context.borrowDataset(transform)).getRDD();

        if (TranslationUtils.skipAssignWindows(transform, context)) {
          context.putDataset(transform, new BoundedDataset<>(inRDD));
        } else {
          @SuppressWarnings("unchecked")
          WindowFn<? super T, W> windowFn = (WindowFn<? super T, W>) transform.getWindowFn();
          OldDoFn<T, T> addWindowsDoFn = new AssignWindowsDoFn<>(windowFn);
          Accumulator<NamedAggregators> accum =
              AccumulatorSingleton.getInstance(context.getSparkContext());
          context.putDataset(transform,
              new BoundedDataset<>(inRDD.mapPartitions(new DoFnFunction<>(accum, addWindowsDoFn,
                  context.getRuntimeContext(), null, null))));
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
        context.putBoundedDatasetFromValues(transform, elems, coder);
      }
    };
  }

  private static <T> TransformEvaluator<View.AsSingleton<T>> viewAsSingleton() {
    return new TransformEvaluator<View.AsSingleton<T>>() {
      @Override
      public void evaluate(View.AsSingleton<T> transform, EvaluationContext context) {
        Iterable<? extends WindowedValue<?>> iter =
            context.getWindowedValues(context.getInput(transform));
        context.putPView(context.getOutput(transform), iter);
      }
    };
  }

  private static <T> TransformEvaluator<View.AsIterable<T>> viewAsIter() {
    return new TransformEvaluator<View.AsIterable<T>>() {
      @Override
      public void evaluate(View.AsIterable<T> transform, EvaluationContext context) {
        Iterable<? extends WindowedValue<?>> iter =
            context.getWindowedValues(context.getInput(transform));
        context.putPView(context.getOutput(transform), iter);
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
        context.putPView(context.getOutput(transform), iter);
      }
    };
  }

  private static final Map<Class<? extends PTransform>, TransformEvaluator<?>> EVALUATORS = Maps
      .newHashMap();

  static {
    EVALUATORS.put(Read.Bounded.class, readBounded());
    EVALUATORS.put(HadoopIO.Read.Bound.class, readHadoop());
    EVALUATORS.put(HadoopIO.Write.Bound.class, writeHadoop());
    EVALUATORS.put(ParDo.Bound.class, parDo());
    EVALUATORS.put(ParDo.BoundMulti.class, multiDo());
    EVALUATORS.put(GroupByKey.class, groupByKey());
    EVALUATORS.put(Combine.GroupedValues.class, combineGrouped());
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
