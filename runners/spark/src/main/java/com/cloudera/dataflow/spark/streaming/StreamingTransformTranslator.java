/*
 * Copyright (c) 2015, Cloudera, Inc. All Rights Reserved.
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
package com.cloudera.dataflow.spark.streaming;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.api.client.util.Lists;
import com.google.api.client.util.Maps;
import com.google.api.client.util.Sets;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.VoidCoder;
import com.google.cloud.dataflow.sdk.io.AvroIO;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.repackaged.com.google.common.reflect.TypeToken;
import com.google.cloud.dataflow.sdk.transforms.AppliedPTransform;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.Flatten;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.SlidingWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn;
import com.google.cloud.dataflow.sdk.util.AssignWindowsDoFn;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollectionList;
import com.google.cloud.dataflow.sdk.values.PDone;

import kafka.serializer.Decoder;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaDStreamLike;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

import com.cloudera.dataflow.hadoop.HadoopIO;
import com.cloudera.dataflow.io.ConsoleIO;
import com.cloudera.dataflow.io.CreateStream;
import com.cloudera.dataflow.io.KafkaIO;
import com.cloudera.dataflow.spark.DoFnFunction;
import com.cloudera.dataflow.spark.EvaluationContext;
import com.cloudera.dataflow.spark.SparkPipelineTranslator;
import com.cloudera.dataflow.spark.TransformEvaluator;
import com.cloudera.dataflow.spark.TransformTranslator;
import com.cloudera.dataflow.spark.WindowingHelpers;

/**
 * Supports translation between a DataFlow transform, and Spark's operations on DStreams.
 */
public final class StreamingTransformTranslator {

  private StreamingTransformTranslator() {
  }

  private static <T> TransformEvaluator<ConsoleIO.Write.Unbound<T>> print() {
    return new TransformEvaluator<ConsoleIO.Write.Unbound<T>>() {
      @Override
      public void evaluate(ConsoleIO.Write.Unbound<T> transform, EvaluationContext context) {
        @SuppressWarnings("unchecked")
        JavaDStreamLike<WindowedValue<T>, ?, JavaRDD<WindowedValue<T>>> dstream =
            (JavaDStreamLike<WindowedValue<T>, ?, JavaRDD<WindowedValue<T>>>)
            ((StreamingEvaluationContext) context).getStream(transform);
        dstream.map(WindowingHelpers.<T>unwindowFunction()).print(transform.getNum());
      }
    };
  }

  private static <K, V> TransformEvaluator<KafkaIO.Read.Unbound<K, V>> kafka() {
    return new TransformEvaluator<KafkaIO.Read.Unbound<K, V>>() {
      @Override
      public void evaluate(KafkaIO.Read.Unbound<K, V> transform, EvaluationContext context) {
        StreamingEvaluationContext sec = (StreamingEvaluationContext) context;
        JavaStreamingContext jssc = sec.getStreamingContext();
        Class<K> keyClazz = transform.getKeyClass();
        Class<V> valueClazz = transform.getValueClass();
        Class<? extends Decoder<K>> keyDecoderClazz = transform.getKeyDecoderClass();
        Class<? extends Decoder<V>> valueDecoderClazz = transform.getValueDecoderClass();
        Map<String, String> kafkaParams = transform.getKafkaParams();
        Set<String> topics = transform.getTopics();
        JavaPairInputDStream<K, V> inputPairStream = KafkaUtils.createDirectStream(jssc, keyClazz,
                valueClazz, keyDecoderClazz, valueDecoderClazz, kafkaParams, topics);
        JavaDStream<WindowedValue<KV<K, V>>> inputStream =
            inputPairStream.map(new Function<Tuple2<K, V>, KV<K, V>>() {
          @Override
          public KV<K, V> call(Tuple2<K, V> t2) throws Exception {
            return KV.of(t2._1(), t2._2());
          }
        }).map(WindowingHelpers.<KV<K, V>>windowFunction());
        sec.setStream(transform, inputStream);
      }
    };
  }

  private static <T> TransformEvaluator<Create.Values<T>> create() {
    return new TransformEvaluator<Create.Values<T>>() {
      @SuppressWarnings("unchecked")
      @Override
      public void evaluate(Create.Values<T> transform, EvaluationContext context) {
        StreamingEvaluationContext sec = (StreamingEvaluationContext) context;
        Iterable<T> elems = transform.getElements();
        Coder<T> coder = sec.getOutput(transform).getCoder();
        if (coder != VoidCoder.of()) {
          // actual create
          sec.setOutputRDDFromValues(transform, elems, coder);
        } else {
          // fake create as an input
          // creates a stream with a single batch containing a single null element
          // to invoke following transformations once
          // to support DataflowAssert
          sec.setDStreamFromQueue(transform,
              Collections.<Iterable<Void>>singletonList(Collections.singletonList((Void) null)),
              (Coder<Void>) coder);
        }
      }
    };
  }

  private static <T> TransformEvaluator<CreateStream.QueuedValues<T>> createFromQueue() {
    return new TransformEvaluator<CreateStream.QueuedValues<T>>() {
      @Override
      public void evaluate(CreateStream.QueuedValues<T> transform, EvaluationContext context) {
        StreamingEvaluationContext sec = (StreamingEvaluationContext) context;
        Iterable<Iterable<T>> values = transform.getQueuedValues();
        Coder<T> coder = sec.getOutput(transform).getCoder();
        sec.setDStreamFromQueue(transform, values, coder);
      }
    };
  }

  private static <T> TransformEvaluator<Flatten.FlattenPCollectionList<T>> flattenPColl() {
    return new TransformEvaluator<Flatten.FlattenPCollectionList<T>>() {
      @SuppressWarnings("unchecked")
      @Override
      public void evaluate(Flatten.FlattenPCollectionList<T> transform, EvaluationContext context) {
        StreamingEvaluationContext sec = (StreamingEvaluationContext) context;
        PCollectionList<T> pcs = sec.getInput(transform);
        JavaDStream<WindowedValue<T>> first =
            (JavaDStream<WindowedValue<T>>) sec.getStream(pcs.get(0));
        List<JavaDStream<WindowedValue<T>>> rest = Lists.newArrayListWithCapacity(pcs.size() - 1);
        for (int i = 1; i < pcs.size(); i++) {
          rest.add((JavaDStream<WindowedValue<T>>) sec.getStream(pcs.get(i)));
        }
        JavaDStream<WindowedValue<T>> dstream = sec.getStreamingContext().union(first, rest);
        sec.setStream(transform, dstream);
      }
    };
  }

  private static <PT extends PTransform<?, ?>> TransformEvaluator<PT> rddTransform(
      final SparkPipelineTranslator rddTranslator) {
    return new TransformEvaluator<PT>() {
      @SuppressWarnings("unchecked")
      @Override
      public void evaluate(PT transform, EvaluationContext context) {
        TransformEvaluator<PT> rddEvaluator =
            rddTranslator.translate((Class<PT>) transform.getClass());

        StreamingEvaluationContext sec = (StreamingEvaluationContext) context;
        if (sec.hasStream(transform)) {
          JavaDStreamLike<WindowedValue<Object>, ?, JavaRDD<WindowedValue<Object>>> dStream =
              (JavaDStreamLike<WindowedValue<Object>, ?, JavaRDD<WindowedValue<Object>>>)
              sec.getStream(transform);

          sec.setStream(transform, dStream
              .transform(new RDDTransform<>(sec, rddEvaluator, transform)));
        } else {
          // if the transformation requires direct access to RDD (not in stream)
          // this is used for "fake" transformations like with DataflowAssert
          rddEvaluator.evaluate(transform, context);
        }
      }
    };
  }

  /**
   * RDD transform function If the transformation function doesn't have an input, create a fake one
   * as an empty RDD.
   *
   * @param <PT> PTransform type
   */
  private static final class RDDTransform<PT extends PTransform<?, ?>>
      implements Function<JavaRDD<WindowedValue<Object>>, JavaRDD<WindowedValue<Object>>> {

    private final StreamingEvaluationContext context;
    private final AppliedPTransform<?, ?, ?> appliedPTransform;
    private final TransformEvaluator<PT> rddEvaluator;
    private final PT transform;


    private RDDTransform(StreamingEvaluationContext context, TransformEvaluator<PT> rddEvaluator,
        PT transform) {
      this.context = context;
      this.appliedPTransform = context.getCurrentTransform();
      this.rddEvaluator = rddEvaluator;
      this.transform = transform;
    }

    @Override
    @SuppressWarnings("unchecked")
    public JavaRDD<WindowedValue<Object>>
        call(JavaRDD<WindowedValue<Object>> rdd) throws Exception {
      AppliedPTransform<?, ?, ?> existingAPT = context.getCurrentTransform();
      context.setCurrentTransform(appliedPTransform);
      context.setInputRDD(transform, rdd);
      rddEvaluator.evaluate(transform, context);
      if (!context.hasOutputRDD(transform)) {
        // fake RDD as output
        context.setOutputRDD(transform,
            context.getSparkContext().<WindowedValue<Object>>emptyRDD());
      }
      JavaRDD<WindowedValue<Object>> outRDD =
          (JavaRDD<WindowedValue<Object>>) context.getOutputRDD(transform);
      context.setCurrentTransform(existingAPT);
      return outRDD;
    }
  }

  @SuppressWarnings("unchecked")
  private static <PT extends PTransform<?, ?>> TransformEvaluator<PT> foreachRDD(
      final SparkPipelineTranslator rddTranslator) {
    return new TransformEvaluator<PT>() {
      @Override
      public void evaluate(PT transform, EvaluationContext context) {
        TransformEvaluator<PT> rddEvaluator =
            rddTranslator.translate((Class<PT>) transform.getClass());

        StreamingEvaluationContext sec = (StreamingEvaluationContext) context;
        if (sec.hasStream(transform)) {
          JavaDStreamLike<WindowedValue<Object>, ?, JavaRDD<WindowedValue<Object>>> dStream =
              (JavaDStreamLike<WindowedValue<Object>, ?, JavaRDD<WindowedValue<Object>>>)
              sec.getStream(transform);

          dStream.foreachRDD(new RDDOutputOperator<>(sec, rddEvaluator, transform));
        } else {
          rddEvaluator.evaluate(transform, context);
        }
      }
    };
  }

  /**
   * RDD output function.
   *
   * @param <PT> PTransform type
   */
  private static final class RDDOutputOperator<PT extends PTransform<?, ?>>
      implements Function<JavaRDD<WindowedValue<Object>>, Void> {

    private final StreamingEvaluationContext context;
    private final AppliedPTransform<?, ?, ?> appliedPTransform;
    private final TransformEvaluator<PT> rddEvaluator;
    private final PT transform;


    private RDDOutputOperator(StreamingEvaluationContext context,
        TransformEvaluator<PT> rddEvaluator, PT transform) {
      this.context = context;
      this.appliedPTransform = context.getCurrentTransform();
      this.rddEvaluator = rddEvaluator;
      this.transform = transform;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Void call(JavaRDD<WindowedValue<Object>> rdd) throws Exception {
      AppliedPTransform<?, ?, ?> existingAPT = context.getCurrentTransform();
      context.setCurrentTransform(appliedPTransform);
      context.setInputRDD(transform, rdd);
      rddEvaluator.evaluate(transform, context);
      context.setCurrentTransform(existingAPT);
      return null;
    }
  }

  private static final TransformTranslator.FieldGetter WINDOW_FG =
      new TransformTranslator.FieldGetter(Window.Bound.class);

  private static <T, W extends BoundedWindow> TransformEvaluator<Window.Bound<T>> window() {
    return new TransformEvaluator<Window.Bound<T>>() {
      @Override
      public void evaluate(Window.Bound<T> transform, EvaluationContext context) {
        StreamingEvaluationContext sec = (StreamingEvaluationContext) context;
        //--- first we apply windowing to the stream
        WindowFn<? super T, W> windowFn = WINDOW_FG.get("windowFn", transform);
        @SuppressWarnings("unchecked")
        JavaDStream<WindowedValue<T>> dStream =
            (JavaDStream<WindowedValue<T>>) sec.getStream(transform);
        if (windowFn instanceof FixedWindows) {
          Duration windowDuration = Durations.milliseconds(((FixedWindows) windowFn).getSize()
              .getMillis());
          sec.setStream(transform, dStream.window(windowDuration));
        } else if (windowFn instanceof SlidingWindows) {
          Duration windowDuration = Durations.milliseconds(((SlidingWindows) windowFn).getSize()
              .getMillis());
          Duration slideDuration = Durations.milliseconds(((SlidingWindows) windowFn).getPeriod()
              .getMillis());
          sec.setStream(transform, dStream.window(windowDuration, slideDuration));
        }
        //--- then we apply windowing to the elements
        DoFn<T, T> addWindowsDoFn = new AssignWindowsDoFn<>(windowFn);
        DoFnFunction<T, T> dofn = new DoFnFunction<>(addWindowsDoFn,
            ((StreamingEvaluationContext)context).getRuntimeContext(), null);
        @SuppressWarnings("unchecked")
        JavaDStreamLike<WindowedValue<T>, ?, JavaRDD<WindowedValue<T>>> dstream =
            (JavaDStreamLike<WindowedValue<T>, ?, JavaRDD<WindowedValue<T>>>)
            sec.getStream(transform);
        sec.setStream(transform, dstream.mapPartitions(dofn));
      }
    };
  }

  private static final Map<Class<? extends PTransform>, TransformEvaluator<?>> EVALUATORS = Maps
      .newHashMap();

  static {
    EVALUATORS.put(ConsoleIO.Write.Unbound.class, print());
    EVALUATORS.put(CreateStream.QueuedValues.class, createFromQueue());
    EVALUATORS.put(Create.Values.class, create());
    EVALUATORS.put(KafkaIO.Read.Unbound.class, kafka());
    EVALUATORS.put(Window.Bound.class, window());
    EVALUATORS.put(Flatten.FlattenPCollectionList.class, flattenPColl());
  }

  private static final Set<Class<? extends PTransform>> UNSUPPORTED_EVALUATORS = Sets
      .newHashSet();

  static {
    //TODO - add support for the following
    UNSUPPORTED_EVALUATORS.add(TextIO.Read.Bound.class);
    UNSUPPORTED_EVALUATORS.add(TextIO.Write.Bound.class);
    UNSUPPORTED_EVALUATORS.add(AvroIO.Read.Bound.class);
    UNSUPPORTED_EVALUATORS.add(AvroIO.Write.Bound.class);
    UNSUPPORTED_EVALUATORS.add(HadoopIO.Read.Bound.class);
    UNSUPPORTED_EVALUATORS.add(HadoopIO.Write.Bound.class);
  }

  @SuppressWarnings("unchecked")
  private static <PT extends PTransform<?, ?>> TransformEvaluator<PT>
      getTransformEvaluator(Class<PT> clazz, SparkPipelineTranslator rddTranslator) {
    TransformEvaluator<PT> transform = (TransformEvaluator<PT>) EVALUATORS.get(clazz);
    if (transform == null) {
      if (UNSUPPORTED_EVALUATORS.contains(clazz)) {
        throw new UnsupportedOperationException("Dataflow transformation " + clazz
          .getCanonicalName()
          + " is currently unsupported by the Spark streaming pipeline");
      }
      // DStream transformations will transform an RDD into another RDD
      // Actions will create output
      // In Dataflow it depends on the PTransform's Input and Output class
      Class<?> pTOutputClazz = getPTransformOutputClazz(clazz);
      if (PDone.class.equals(pTOutputClazz)) {
        return foreachRDD(rddTranslator);
      } else {
        return rddTransform(rddTranslator);
      }
    }
    return transform;
  }

  private static <PT extends PTransform<?, ?>> Class<?> getPTransformOutputClazz(Class<PT> clazz) {
    Type[] types = ((ParameterizedType) clazz.getGenericSuperclass()).getActualTypeArguments();
    return TypeToken.of(clazz).resolveType(types[1]).getRawType();
  }

  /**
   * Translator matches Dataflow transformation with the appropriate Spark streaming evaluator.
   * rddTranslator uses Spark evaluators in transform/foreachRDD to evaluate the transformation
   */
  public static class Translator implements SparkPipelineTranslator {

    private final SparkPipelineTranslator rddTranslator;

    public Translator(SparkPipelineTranslator rddTranslator) {
      this.rddTranslator = rddTranslator;
    }

    @Override
    public boolean hasTranslation(Class<? extends PTransform<?, ?>> clazz) {
      // streaming includes rdd transformations as well
      return EVALUATORS.containsKey(clazz) || rddTranslator.hasTranslation(clazz);
    }

    @Override
    public <PT extends PTransform<?, ?>> TransformEvaluator<PT> translate(Class<PT> clazz) {
      return getTransformEvaluator(clazz, rddTranslator);
    }
  }
}
