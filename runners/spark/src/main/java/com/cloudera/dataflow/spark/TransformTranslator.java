/*
 * Copyright (c) 2014, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package com.cloudera.dataflow.spark;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.api.client.util.Maps;
import com.google.cloud.dataflow.sdk.coders.CannotProvideCoderException;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.io.AvroIO;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.Flatten;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.View;
import com.google.cloud.dataflow.sdk.transforms.windowing.GlobalWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionList;
import com.google.cloud.dataflow.sdk.values.PCollectionTuple;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import static com.cloudera.dataflow.spark.ShardNameBuilder.getOutputDirectory;
import static com.cloudera.dataflow.spark.ShardNameBuilder.getOutputFilePrefix;
import static com.cloudera.dataflow.spark.ShardNameBuilder.getOutputFileTemplate;
import static com.cloudera.dataflow.spark.ShardNameBuilder.replaceShardCount;

import com.cloudera.dataflow.hadoop.HadoopIO;

/**
 * Supports translation between a DataFlow transform, and Spark's operations on RDDs.
 */
public final class TransformTranslator {

  private TransformTranslator() {
  }

  private static class FieldGetter {
    private final Map<String, Field> fields;

    FieldGetter(Class<?> clazz) {
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
        JavaRDD<T>[] rdds = new JavaRDD[pcs.size()];
        for (int i = 0; i < rdds.length; i++) {
          rdds[i] = (JavaRDD<T>) context.getRDD(pcs.get(i));
        }
        JavaRDD<T> rdd = context.getSparkContext().union(rdds);
        context.setOutputRDD(transform, rdd);
      }
    };
  }

  private static <K, V> TransformEvaluator<GroupByKey.GroupByKeyOnly<K, V>> gbk() {
    return new TransformEvaluator<GroupByKey.GroupByKeyOnly<K, V>>() {
      @Override
      public void evaluate(GroupByKey.GroupByKeyOnly<K, V> transform, EvaluationContext context) {
        @SuppressWarnings("unchecked")
        JavaRDDLike<KV<K, V>, ?> inRDD =
            (JavaRDDLike<KV<K, V>, ?>) context.getInputRDD(transform);
        @SuppressWarnings("unchecked")
        KvCoder<K, V> coder = (KvCoder<K, V>) context.getInput(transform).getCoder();
        final Coder<K> keyCoder = coder.getKeyCoder();
        final Coder<V> valueCoder = coder.getValueCoder();

        // Use coders to convert objects in the PCollection to byte arrays, so they
        // can be transferred over the network for the shuffle.
        JavaRDDLike<KV<K, Iterable<V>>, ?> outRDD = fromPair(toPair(inRDD)
            .mapToPair(CoderHelpers.toByteFunction(keyCoder, valueCoder))
            .groupByKey()
            .mapToPair(CoderHelpers.fromByteFunctionIterable(keyCoder, valueCoder)));
        context.setOutputRDD(transform, outRDD);
      }
    };
  }

  private static final FieldGetter GROUPED_FG = new FieldGetter(Combine.GroupedValues.class);

  private static <K, VI, VO> TransformEvaluator<Combine.GroupedValues<K, VI, VO>> grouped() {
    return new TransformEvaluator<Combine.GroupedValues<K, VI, VO>>() {
      @Override
      public void evaluate(Combine.GroupedValues<K, VI, VO> transform, EvaluationContext context) {
        Combine.KeyedCombineFn<K, VI, ?, VI> keyed = GROUPED_FG.get("fn", transform);
        @SuppressWarnings("unchecked")
        JavaRDDLike<KV<K, Iterable<VI>>, ?> inRDD =
            (JavaRDDLike<KV<K, Iterable<VI>>, ?>) context.getInputRDD(transform);
        context.setOutputRDD(transform, inRDD.map(new KVFunction<>(keyed)));
      }
    };
  }

  private static final FieldGetter COMBINE_GLOBALLY_FG = new FieldGetter(Combine.Globally.class);

  private static <I, A, O> TransformEvaluator<Combine.Globally<I, O>> combineGlobally() {
    return new TransformEvaluator<Combine.Globally<I, O>>() {

      @Override
      public void evaluate(Combine.Globally<I, O> transform, EvaluationContext context) {
        final Combine.CombineFn<I, A, O> globally = COMBINE_GLOBALLY_FG.get("fn", transform);

        @SuppressWarnings("unchecked")
        JavaRDDLike<I, ?> inRdd = (JavaRDDLike<I, ?>) context.getInputRDD(transform);

        final Coder<I> iCoder = context.getInput(transform).getCoder();
        final Coder<A> aCoder;
        try {
          aCoder = globally.getAccumulatorCoder(
              context.getPipeline().getCoderRegistry(), iCoder);
        } catch (CannotProvideCoderException e) {
          throw new IllegalStateException("Could not determine coder for accumulator", e);
        }

        // Use coders to convert objects in the PCollection to byte arrays, so they
        // can be transferred over the network for the shuffle.
        JavaRDD<byte[]> inRddBytes = inRdd.map(CoderHelpers.toByteFunction(iCoder));

        /*A*/ byte[] acc = inRddBytes.aggregate(
            CoderHelpers.toByteArray(globally.createAccumulator(), aCoder),
            new Function2</*A*/ byte[], /*I*/ byte[], /*A*/ byte[]>() {
              @Override
              public /*A*/ byte[] call(/*A*/ byte[] ab, /*I*/ byte[] ib) throws Exception {
                A a = CoderHelpers.fromByteArray(ab, aCoder);
                I i = CoderHelpers.fromByteArray(ib, iCoder);
                return CoderHelpers.toByteArray(globally.addInput(a, i), aCoder);
              }
            },
            new Function2</*A*/ byte[], /*A*/ byte[], /*A*/ byte[]>() {
              @Override
              public /*A*/ byte[] call(/*A*/ byte[] a1b, /*A*/ byte[] a2b) throws Exception {
                A a1 = CoderHelpers.fromByteArray(a1b, aCoder);
                A a2 = CoderHelpers.fromByteArray(a2b, aCoder);
                // don't use Guava's ImmutableList.of as values may be null
                List<A> accumulators = Collections.unmodifiableList(Arrays.asList(a1, a2));
                A merged = globally.mergeAccumulators(accumulators);
                return CoderHelpers.toByteArray(merged, aCoder);
              }
            }
        );
        O output = globally.extractOutput(CoderHelpers.fromByteArray(acc, aCoder));

        Coder<O> coder = context.getOutput(transform).getCoder();
        JavaRDD<byte[]> outRdd = context.getSparkContext().parallelize(
            CoderHelpers.toByteArrays(ImmutableList.of(output), coder));
        context.setOutputRDD(transform, outRdd.map(CoderHelpers.fromByteFunction(coder)));
      }
    };
  }

  private static final FieldGetter COMBINE_PERKEY_FG = new FieldGetter(Combine.PerKey.class);

  private static <K, VI, VA, VO> TransformEvaluator<Combine.PerKey<K, VI, VO>> combinePerKey() {
    return new TransformEvaluator<Combine.PerKey<K, VI, VO>>() {
      @Override
      public void evaluate(Combine.PerKey<K, VI, VO> transform, EvaluationContext context) {
        final Combine.KeyedCombineFn<K, VI, VA, VO> keyed =
            COMBINE_PERKEY_FG.get("fn", transform);
        @SuppressWarnings("unchecked")
        JavaRDDLike<KV<K, VI>, ?> inRdd =
            (JavaRDDLike<KV<K, VI>, ?>) context.getInputRDD(transform);

        @SuppressWarnings("unchecked")
        KvCoder<K, VI> inputCoder = (KvCoder<K, VI>) context.getInput(transform).getCoder();
        Coder<K> keyCoder = inputCoder.getKeyCoder();
        Coder<VI> viCoder = inputCoder.getValueCoder();
        Coder<VA> vaCoder = null;
        try {
          vaCoder = keyed.getAccumulatorCoder(
              context.getPipeline().getCoderRegistry(), keyCoder, viCoder);
        } catch (CannotProvideCoderException e) {
          throw new IllegalStateException("Could not determine coder for accumulator", e);
        }
        final Coder<KV<K, VI>> kviCoder = KvCoder.of(keyCoder, viCoder);
        final Coder<KV<K, VA>> kvaCoder = KvCoder.of(keyCoder, vaCoder);

        // We need to duplicate K as both the key of the JavaPairRDD as well as inside the value,
        // since the functions passed to combineByKey don't receive the associated key of each
        // value, and we need to map back into methods in Combine.KeyedCombineFn, which each
        // require the key in addition to the VI's and VA's being merged/accumulated. Once Spark
        // provides a way to include keys in the arguments of combine/merge functions, we won't
        // need to duplicate the keys anymore.
        JavaPairRDD<K, KV<K, VI>> inRddDuplicatedKeyPair = inRdd.mapToPair(
            new PairFunction<KV<K, VI>, K, KV<K, VI>>() {
              @Override
              public Tuple2<K, KV<K, VI>> call(KV<K, VI> kv) {
                return new Tuple2<>(kv.getKey(), kv);
              }
            });

        // Use coders to convert objects in the PCollection to byte arrays, so they
        // can be transferred over the network for the shuffle.
        JavaPairRDD<ByteArray, byte[]> inRddDuplicatedKeyPairBytes = inRddDuplicatedKeyPair
            .mapToPair(CoderHelpers.toByteFunction(keyCoder, kviCoder));

        // The output of combineByKey will be "VA" (accumulator) types rather than "VO" (final
        // output types) since Combine.CombineFn only provides ways to merge VAs, and no way
        // to merge VOs.
        JavaPairRDD</*K*/ ByteArray, /*KV<K, VA>*/ byte[]> accumulatedBytes =
            inRddDuplicatedKeyPairBytes.combineByKey(
            new Function</*KV<K, VI>*/ byte[], /*KV<K, VA>*/ byte[]>() {
              @Override
              public /*KV<K, VA>*/ byte[] call(/*KV<K, VI>*/ byte[] input) {
                KV<K, VI> kvi = CoderHelpers.fromByteArray(input, kviCoder);
                VA va = keyed.createAccumulator(kvi.getKey());
                va = keyed.addInput(kvi.getKey(), va, kvi.getValue());
                return CoderHelpers.toByteArray(KV.of(kvi.getKey(), va), kvaCoder);
              }
            },
            new Function2</*KV<K, VA>*/ byte[], /*KV<K, VI>*/ byte[], /*KV<K, VA>*/ byte[]>() {
              @Override
              public /*KV<K, VA>*/ byte[] call(/*KV<K, VA>*/ byte[] acc,
                  /*KV<K, VI>*/ byte[] input) {
                KV<K, VA> kva = CoderHelpers.fromByteArray(acc, kvaCoder);
                KV<K, VI> kvi = CoderHelpers.fromByteArray(input, kviCoder);
                VA va = keyed.addInput(kva.getKey(), kva.getValue(), kvi.getValue());
                kva = KV.of(kva.getKey(), va);
                return CoderHelpers.toByteArray(KV.of(kva.getKey(), kva.getValue()), kvaCoder);
              }
            },
            new Function2</*KV<K, VA>*/ byte[], /*KV<K, VA>*/ byte[], /*KV<K, VA>*/ byte[]>() {
              @Override
              public /*KV<K, VA>*/ byte[] call(/*KV<K, VA>*/ byte[] acc1,
                  /*KV<K, VA>*/ byte[] acc2) {
                KV<K, VA> kva1 = CoderHelpers.fromByteArray(acc1, kvaCoder);
                KV<K, VA> kva2 = CoderHelpers.fromByteArray(acc2, kvaCoder);
                VA va = keyed.mergeAccumulators(kva1.getKey(),
                    // don't use Guava's ImmutableList.of as values may be null
                    Collections.unmodifiableList(Arrays.asList(kva1.getValue(), kva2.getValue())));
                return CoderHelpers.toByteArray(KV.of(kva1.getKey(), va), kvaCoder);
              }
            });

        JavaPairRDD<K, VO> extracted = accumulatedBytes
            .mapToPair(CoderHelpers.fromByteFunction(keyCoder, kvaCoder))
            .mapValues(
                new Function<KV<K, VA>, VO>() {
                  @Override
                  public VO call(KV<K, VA> acc) {
                    return keyed.extractOutput(acc.getKey(), acc.getValue());
                  }
                });
        context.setOutputRDD(transform, fromPair(extracted));
      }
    };
  }

  private static final class KVFunction<K, V> implements Function<KV<K, Iterable<V>>, KV<K, V>> {
    private final Combine.KeyedCombineFn<K, V, ?, V> keyed;

    KVFunction(Combine.KeyedCombineFn<K, V, ?, V> keyed) {
      this.keyed = keyed;
    }

    @Override
    public KV<K, V> call(KV<K, Iterable<V>> kv) throws Exception {
      return KV.of(kv.getKey(), keyed.apply(kv.getKey(), kv.getValue()));
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

  private static <I, O> TransformEvaluator<ParDo.Bound<I, O>> parDo() {
    return new TransformEvaluator<ParDo.Bound<I, O>>() {
      @Override
      public void evaluate(ParDo.Bound<I, O> transform, EvaluationContext context) {
        DoFnFunction<I, O> dofn =
            new DoFnFunction<>(transform.getFn(),
                context.getRuntimeContext(),
                getSideInputs(transform.getSideInputs(), context));
        @SuppressWarnings("unchecked")
        JavaRDDLike<I, ?> inRDD = (JavaRDDLike<I, ?>) context.getInputRDD(transform);
        context.setOutputRDD(transform, inRDD.mapPartitions(dofn));
      }
    };
  }

  private static final FieldGetter MULTIDO_FG = new FieldGetter(ParDo.BoundMulti.class);

  private static <I, O> TransformEvaluator<ParDo.BoundMulti<I, O>> multiDo() {
    return new TransformEvaluator<ParDo.BoundMulti<I, O>>() {
      @Override
      public void evaluate(ParDo.BoundMulti<I, O> transform, EvaluationContext context) {
        TupleTag<O> mainOutputTag = MULTIDO_FG.get("mainOutputTag", transform);
        MultiDoFnFunction<I, O> multifn = new MultiDoFnFunction<>(
            transform.getFn(),
            context.getRuntimeContext(),
            mainOutputTag,
            getSideInputs(transform.getSideInputs(), context));

        @SuppressWarnings("unchecked")
        JavaRDDLike<I, ?> inRDD = (JavaRDDLike<I, ?>) context.getInputRDD(transform);
        JavaPairRDD<TupleTag<?>, Object> all = inRDD
            .mapPartitionsToPair(multifn)
            .cache();

        PCollectionTuple pct = context.getOutput(transform);
        for (Map.Entry<TupleTag<?>, PCollection<?>> e : pct.getAll().entrySet()) {
          @SuppressWarnings("unchecked")
          JavaPairRDD<TupleTag<?>, Object> filtered =
              all.filter(new TupleTagFilter(e.getKey()));
          context.setRDD(e.getValue(), filtered.values());
        }
      }
    };
  }


  private static <T> TransformEvaluator<TextIO.Read.Bound<T>> readText() {
    return new TransformEvaluator<TextIO.Read.Bound<T>>() {
      @Override
      public void evaluate(TextIO.Read.Bound<T> transform, EvaluationContext context) {
        String pattern = transform.getFilepattern();
        JavaRDD<String> rdd = context.getSparkContext().textFile(pattern);
        context.setOutputRDD(transform, rdd);
      }
    };
  }

  private static <T> TransformEvaluator<TextIO.Write.Bound<T>> writeText() {
    return new TransformEvaluator<TextIO.Write.Bound<T>>() {
      @Override
      public void evaluate(TextIO.Write.Bound<T> transform, EvaluationContext context) {
        @SuppressWarnings("unchecked")
        JavaPairRDD<T, Void> last = ((JavaRDDLike<T, ?>) context.getInputRDD(transform))
            .mapToPair(new PairFunction<T, T,
                Void>() {
              @Override
              public Tuple2<T, Void> call(T t) throws Exception {
                return new Tuple2<>(t, null);
              }
            });
        int shardCount = transform.getNumShards();
        if (shardCount == 0) {
          // use default number of shards, but find the actual number for the template
          shardCount = last.partitions().size();
        } else {
          // number of shards was set explicitly, so repartition
          last = last.repartition(transform.getNumShards());
        }

        String template = replaceShardCount(transform.getShardTemplate(), shardCount);
        String outputDir = getOutputDirectory(transform.getFilenamePrefix(), template);
        String filePrefix = getOutputFilePrefix(transform.getFilenamePrefix(), template);
        String fileTemplate = getOutputFileTemplate(transform.getFilenamePrefix(), template);
        String fileSuffix = transform.getFilenameSuffix();

        Configuration conf = new Configuration();
        conf.set(TemplatedTextOutputFormat.OUTPUT_FILE_PREFIX, filePrefix);
        conf.set(TemplatedTextOutputFormat.OUTPUT_FILE_TEMPLATE, fileTemplate);
        conf.set(TemplatedTextOutputFormat.OUTPUT_FILE_SUFFIX, fileSuffix);
        last.saveAsNewAPIHadoopFile(outputDir, Text.class, NullWritable.class,
            TemplatedTextOutputFormat.class, conf);
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
        JavaRDD<T> rdd = avroFile.map(
            new Function<AvroKey<T>, T>() {
              @Override
              public T call(AvroKey<T> key) {
                return key.datum();
              }
            });
        context.setOutputRDD(transform, rdd);
      }
    };
  }

  private static <T> TransformEvaluator<AvroIO.Write.Bound<T>> writeAvro() {
    return new TransformEvaluator<AvroIO.Write.Bound<T>>() {
      @Override
      public void evaluate(AvroIO.Write.Bound<T> transform, EvaluationContext context) {
        String pattern = transform.getFilenamePrefix();
        @SuppressWarnings("unchecked")
        JavaRDDLike<T, ?> last = (JavaRDDLike<T, ?>) context.getInputRDD(transform);
        Job job;
        try {
          job = Job.getInstance();
        } catch (IOException e) {
          throw new IllegalStateException(e);
        }
        AvroJob.setOutputKeySchema(job, transform.getSchema());
        last.mapToPair(new PairFunction<T, AvroKey<T>, NullWritable>() {
            @Override
            public Tuple2<AvroKey<T>, NullWritable> call(T t) throws Exception {
              return new Tuple2<>(new AvroKey<>(t), NullWritable.get());
            }})
          .saveAsNewAPIHadoopFile(pattern, AvroKey.class, NullWritable.class,
              AvroKeyOutputFormat.class, job.getConfiguration());

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
        JavaRDD<KV<K, V>> rdd = file.map(new Function<Tuple2<K, V>, KV<K, V>>() {
          @Override
          public KV<K, V> call(Tuple2<K, V> t2) throws Exception {
            return KV.of(t2._1(), t2._2());
          }
        });
        context.setOutputRDD(transform, rdd);
      }
    };
  }

  private static <T> TransformEvaluator<Window.Bound<T>> window() {
    return new TransformEvaluator<Window.Bound<T>>() {
      @Override
      public void evaluate(Window.Bound<T> transform, EvaluationContext context) {
        if (transform.getWindowingStrategy().getWindowFn() instanceof GlobalWindows) {
          context.setOutputRDD(transform, context.getInputRDD(transform));
        } else {
          throw new UnsupportedOperationException("Non-global windowing not supported");
        }
      }
    };
  }

  private static <T> TransformEvaluator<Create<T>> create() {
    return new TransformEvaluator<Create<T>>() {
      @Override
      public void evaluate(Create<T> transform, EvaluationContext context) {
        Iterable<T> elems = transform.getElements();
        // Use a coder to convert the objects in the PCollection to byte arrays, so they
        // can be transferred over the network.
        Coder<T> coder = context.getOutput(transform).getCoder();
        JavaRDD<byte[]> rdd = context.getSparkContext().parallelize(
            CoderHelpers.toByteArrays(elems, coder));
        context.setOutputRDD(transform, rdd.map(CoderHelpers.fromByteFunction(coder)));
      }
    };
  }

  private static <T> TransformEvaluator<View.AsSingleton<T>> viewAsSingleton() {
    return new TransformEvaluator<View.AsSingleton<T>>() {
      @Override
      public void evaluate(View.AsSingleton<T> transform, EvaluationContext context) {
        Iterable<T> input = context.get(context.getInput(transform));
        context.setPView(context.getOutput(transform), Iterables.transform(input,
            new WindowingFunction<T>()));
      }
    };
  }

  private static <T> TransformEvaluator<View.AsIterable<T>> viewAsIter() {
    return new TransformEvaluator<View.AsIterable<T>>() {
      @Override
      public void evaluate(View.AsIterable<T> transform, EvaluationContext context) {
        Iterable<T> input = context.get(context.getInput(transform));
        context.setPView(context.getOutput(transform), Iterables.transform(input,
            new WindowingFunction<T>()));
      }
    };
  }

  private static <R, W> TransformEvaluator<View.CreatePCollectionView<R, W>> createPCollView() {
    return new TransformEvaluator<View.CreatePCollectionView<R, W>>() {
      @Override
      public void evaluate(View.CreatePCollectionView<R, W> transform, EvaluationContext context) {
        Iterable<WindowedValue<?>> iter = Iterables.transform(
            context.get(context.getInput(transform)), new WindowingFunction<R>());
        context.setPView(context.getOutput(transform), iter);
      }
    };
  }

  private static class WindowingFunction<R> implements com.google.common.base.Function<R,
      WindowedValue<?>> {
    @Override
    public WindowedValue<R> apply(R t) {
      return WindowedValue.valueInGlobalWindow(t);
    }
  }

  private static final class TupleTagFilter<V>
      implements Function<Tuple2<TupleTag<V>, Object>, Boolean> {

    private final TupleTag<V> tag;

    private TupleTagFilter(TupleTag<V> tag) {
      this.tag = tag;
    }

    @Override
    public Boolean call(Tuple2<TupleTag<V>, Object> input) {
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
        Iterable<WindowedValue<?>> collectionView = context.getPCollectionView(view);
        Coder<Iterable<WindowedValue<?>>> coderInternal = view.getCoderInternal();
        BroadcastHelper<?> helper = new BroadcastHelper<>(collectionView, coderInternal);
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
    EVALUATORS.put(ParDo.Bound.class, parDo());
    EVALUATORS.put(ParDo.BoundMulti.class, multiDo());
    EVALUATORS.put(GroupByKey.GroupByKeyOnly.class, gbk());
    EVALUATORS.put(Combine.GroupedValues.class, grouped());
    EVALUATORS.put(Combine.Globally.class, combineGlobally());
    EVALUATORS.put(Combine.PerKey.class, combinePerKey());
    EVALUATORS.put(Flatten.FlattenPCollectionList.class, flattenPColl());
    EVALUATORS.put(Create.class, create());
    EVALUATORS.put(View.AsSingleton.class, viewAsSingleton());
    EVALUATORS.put(View.AsIterable.class, viewAsIter());
    EVALUATORS.put(View.CreatePCollectionView.class, createPCollView());
    EVALUATORS.put(Window.Bound.class, window());
  }

  public static <PT extends PTransform> boolean hasTransformEvaluator(Class<PT> clazz) {
    return EVALUATORS.containsKey(clazz);
  }

  public static <PT extends PTransform> TransformEvaluator<PT> getTransformEvaluator(Class<PT>
                                                                                         clazz) {
    @SuppressWarnings("unchecked")
    TransformEvaluator<PT> transform = (TransformEvaluator<PT>) EVALUATORS.get(clazz);
    if (transform == null) {
      throw new IllegalStateException("No TransformEvaluator registered for " + clazz);
    }
    return transform;
  }
}
