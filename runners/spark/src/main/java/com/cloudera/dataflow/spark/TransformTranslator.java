package com.cloudera.dataflow.spark;

import com.google.api.client.util.Maps;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.io.AvroIO;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.transforms.*;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.values.*;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.lang.reflect.Field;
import java.util.Map;

/**
 * Supports translation between a DataFlow transform, and Spark's operations on RDDs.
 */
public class TransformTranslator {
    private static class FieldGetter {
        private Map<String, Field> fields;

        public FieldGetter(Class<?> clazz) {
            this.fields = Maps.newHashMap();
            for (Field f : clazz.getDeclaredFields()) {
                try {
                    f.setAccessible(true);
                    this.fields.put(f.getName(), f);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }

        public <T> T get(String fieldname, Object value) {
            try {
                return (T) fields.get(fieldname).get(value);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static TransformEvaluator<Flatten> FLATTEN = new TransformEvaluator<Flatten>() {
        @Override
        public void evaluate(Flatten transform, EvaluationContext context) {
            PCollectionList<?> pcs = (PCollectionList<?>) context.getPipeline().getInput(transform);
            JavaRDD[] rdds = new JavaRDD[pcs.size()];
            for (int i = 0; i < rdds.length; i++) {
                rdds[i] = (JavaRDD) context.getRDD(pcs.get(i));
            }
            JavaRDD rdd = context.getSparkContext().union(rdds);
            context.setOutputRDD(transform, rdd);
        }
    };
    private static TransformEvaluator<GroupByKey.GroupByKeyOnly> GBK = new TransformEvaluator<GroupByKey.GroupByKeyOnly>() {
        @Override
        public void evaluate(GroupByKey.GroupByKeyOnly transform, EvaluationContext context) {
            context.setOutputRDD(transform, fromPair(toPair(context.getInputRDD(transform)).groupByKey()));
        }
    };

    private static FieldGetter GROUPED_FG = new FieldGetter(Combine.GroupedValues.class);
    private static TransformEvaluator<Combine.GroupedValues> GROUPED = new TransformEvaluator<Combine.GroupedValues>() {
        @Override
        public void evaluate(Combine.GroupedValues transform, EvaluationContext context) {
            final Combine.KeyedCombineFn keyed = GROUPED_FG.get("fn", transform);
            context.setOutputRDD(transform, context.getInputRDD(transform).map(new Function() {
                @Override
                public Object call(Object input) throws Exception {
                    KV<Object, Iterable> kv = (KV<Object, Iterable>) input;
                    return KV.of(kv.getKey(), keyed.apply(kv.getKey(), kv.getValue()));
                }
            }));
        }
    };

    private static JavaPairRDD toPair(JavaRDDLike rdd) {
        return rdd.mapToPair(new PairFunction() {
            @Override
            public Tuple2 call(Object o) throws Exception {
                KV kv = (KV) o;
                return new Tuple2(kv.getKey(), kv.getValue());
            }
        });
    }

    private static JavaRDDLike fromPair(JavaPairRDD rdd) {
        return rdd.map(new Function() {
            @Override
            public Object call(Object o) throws Exception {
                Tuple2 t2 = (Tuple2) o;
                return KV.of(t2._1(), t2._2());
            }
        });
    }


    private static TransformEvaluator<ParDo.Bound> PARDO = new TransformEvaluator<ParDo.Bound>() {
        @Override
        public void evaluate(ParDo.Bound transform, EvaluationContext context) {
            DoFnFunction dofn = new DoFnFunction(transform.getFn(),
                    context.getRuntimeContext(),
                    getSideInputs(transform.getSideInputs(), context));
            context.setOutputRDD(transform, context.getInputRDD(transform).mapPartitions(dofn));
        }
    };

    private static FieldGetter MULTIDO_FG = new FieldGetter(ParDo.BoundMulti.class);
    private static TransformEvaluator<ParDo.BoundMulti> MULTIDO = new TransformEvaluator<ParDo.BoundMulti>() {
        @Override
        public void evaluate(ParDo.BoundMulti transform, EvaluationContext context) {
            MultiDoFnFunction multifn = new MultiDoFnFunction(
                    transform.getFn(),
                    context.getRuntimeContext(),
                    (TupleTag) MULTIDO_FG.get("mainOutputTag", transform),
                    getSideInputs(transform.getSideInputs(), context));

            JavaPairRDD<TupleTag, Object> all = context.getInputRDD(transform)
                    .mapPartitionsToPair(multifn)
                    .cache();

            PCollectionTuple pct = (PCollectionTuple) context.getOutput(transform);
            for (Map.Entry<TupleTag<?>, PCollection<?>> e : pct.getAll().entrySet()) {
                TupleTagFilter filter = new TupleTagFilter(e.getKey());
                JavaPairRDD<TupleTag, Object> filtered = all.filter(filter);
                context.setRDD(e.getValue(), filtered.values());
            }
        }
    };


    private static TransformEvaluator<TextIO.Read.Bound> READ_TEXT = new TransformEvaluator<TextIO.Read.Bound>() {
        @Override
        public void evaluate(TextIO.Read.Bound transform, EvaluationContext context) {
            String pattern = transform.getFilepattern();
            JavaRDD rdd = context.getSparkContext().textFile(pattern);
            context.setOutputRDD(transform, rdd);
        }
    };

    private static TransformEvaluator<TextIO.Write.Bound> WRITE_TEXT = new TransformEvaluator<TextIO.Write.Bound>() {
        @Override
        public void evaluate(TextIO.Write.Bound transform, EvaluationContext context) {
            JavaRDDLike last = context.getInputRDD(transform);
            String pattern = transform.getFilenamePrefix();
            last.saveAsTextFile(pattern);
        }
    };

    private static TransformEvaluator<AvroIO.Read.Bound> READ_AVRO = new TransformEvaluator<AvroIO.Read.Bound>() {
        @Override
        public void evaluate(AvroIO.Read.Bound transform, EvaluationContext context) {
            String pattern = transform.getFilepattern();
            JavaRDD rdd = context.getSparkContext().textFile(pattern);
            context.setOutputRDD(transform, rdd);
        }
    };

    private static TransformEvaluator<AvroIO.Write.Bound> WRITE_AVRO = new TransformEvaluator<AvroIO.Write.Bound>() {
        @Override
        public void evaluate(AvroIO.Write.Bound transform, EvaluationContext context) {
            JavaRDDLike last = context.getInputRDD(transform);
            String pattern = transform.getFilenamePrefix();
            last.saveAsTextFile(pattern);
        }
    };

    private static TransformEvaluator<Create> CREATE = new TransformEvaluator<Create>() {
        @Override
        public void evaluate(Create transform, EvaluationContext context) {
            Iterable elems = transform.getElements();
            Coder coder = ((PCollection) context.getOutput(transform)).getCoder();
            JavaRDD rdd = context.getSparkContext().parallelize(
                    CoderHelpers.toByteArrays(elems, coder));
            context.setOutputRDD(transform, rdd.map(CoderHelpers.fromByteFunction(coder)));
        }
    };

    private static TransformEvaluator<CreatePObject> CREATE_POBJ = new TransformEvaluator<CreatePObject>() {
        @Override
        public void evaluate(CreatePObject transform, EvaluationContext context) {
            context.setPObjectValue((PObject) context.getOutput(transform), transform.getElement());
        }
    };

    private static TransformEvaluator<Convert.ToIterable> TO_ITER = new TransformEvaluator<Convert.ToIterable>() {
        @Override
        public void evaluate(Convert.ToIterable transform, EvaluationContext context) {
            PCollection<?> in = (PCollection<?>) context.getInput(transform);
            PObject<?> out = (PObject<?>) context.getOutput(transform);
            context.setPObjectValue(out, context.get(in));
        }
    };

    private static TransformEvaluator<Convert.ToIterableWindowedValue> TO_ITER_WIN =
            new TransformEvaluator<Convert.ToIterableWindowedValue>() {
                @Override
                public void evaluate(Convert.ToIterableWindowedValue transform, EvaluationContext context) {
                    PCollection<?> in = (PCollection<?>) context.getInput(transform);
                    PObject<?> out = (PObject<?>) context.getOutput(transform);
                    context.setPObjectValue(out, Iterables.transform(context.get(in),
                            new com.google.common.base.Function<Object, WindowedValue>() {
                                @Override
                                public WindowedValue apply(Object o) {
                                    return WindowedValue.valueInGlobalWindow(o);
                                }
                            }));
                }
            };

    private static class TupleTagFilter implements Function<Tuple2<TupleTag, Object>, Boolean> {
        private TupleTag tag;

        public TupleTagFilter(TupleTag tag) {
            this.tag = tag;
        }

        @Override
        public Boolean call(Tuple2<TupleTag, Object> input) throws Exception {
            return tag.equals(input._1());
        }
    }

    private static TransformEvaluator<SeqDo.BoundMulti> SEQDO = new TransformEvaluator<SeqDo.BoundMulti>() {
        @Override
        public void evaluate(SeqDo.BoundMulti transform, EvaluationContext context) {
            PObjectValueTuple inputValues = context.getPObjectTuple(transform);
            PObjectValueTuple outputValues = transform.getFn().process(inputValues);
            context.setPObjectTuple(transform, outputValues);
        }
    };

    private static Map<TupleTag<?>, BroadcastHelper<?>> getSideInputs(
            Iterable<PCollectionView<?, ?>> views,
            EvaluationContext context) {
        if (views == null) {
            return ImmutableMap.of();
        } else {
            Map<TupleTag<?>, BroadcastHelper<?>>sideInputs = Maps.newHashMap();
            for (PCollectionView<?, ?> view : views) {
                sideInputs.put(view.getTagInternal(), context.getBroadcastHelper(view.getPObjectInternal()));
            }
            return sideInputs;
        }
    }

    private static final Map<Class<? extends PTransform>, TransformEvaluator> mEvaluators = Maps.newHashMap();
    static {
        mEvaluators.put(TextIO.Read.Bound.class, READ_TEXT);
        mEvaluators.put(TextIO.Write.Bound.class, WRITE_TEXT);
        mEvaluators.put(AvroIO.Read.Bound.class, READ_AVRO);
        mEvaluators.put(AvroIO.Write.Bound.class, WRITE_AVRO);
        mEvaluators.put(ParDo.Bound.class, PARDO);
        mEvaluators.put(ParDo.BoundMulti.class, MULTIDO);
        mEvaluators.put(SeqDo.BoundMulti.class, SEQDO);
        mEvaluators.put(GroupByKey.GroupByKeyOnly.class, GBK);
        mEvaluators.put(Combine.GroupedValues.class, GROUPED);
        mEvaluators.put(Flatten.class, FLATTEN);
        mEvaluators.put(Create.class, CREATE);
        mEvaluators.put(CreatePObject.class, CREATE_POBJ);
        mEvaluators.put(Convert.ToIterable.class, TO_ITER);
        mEvaluators.put(Convert.ToIterableWindowedValue.class, TO_ITER_WIN);
    }

    public static TransformEvaluator getTransformEvaluator(Class<? extends PTransform> clazz) {
        TransformEvaluator transform = mEvaluators.get(clazz);
        if (transform == null) {
            throw new IllegalStateException("No TransformEvaluator registered for " + clazz);
        }
        return transform;
    }
}
