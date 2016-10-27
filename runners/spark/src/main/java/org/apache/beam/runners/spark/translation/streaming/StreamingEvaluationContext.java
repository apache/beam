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
package org.apache.beam.runners.spark.translation.streaming;


import com.google.common.collect.Iterables;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.beam.runners.spark.coders.CoderHelpers;
import org.apache.beam.runners.spark.translation.EvaluationContext;
import org.apache.beam.runners.spark.translation.SparkRuntimeContext;
import org.apache.beam.runners.spark.translation.WindowingHelpers;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaDStreamLike;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.joda.time.Duration;


/**
 * Streaming evaluation context helps to handle streaming.
 */
public class StreamingEvaluationContext extends EvaluationContext {

  private final JavaStreamingContext jssc;
  private final long timeout;
  private final Map<PValue, DStreamHolder<?>> pstreams = new LinkedHashMap<>();
  private final Set<DStreamHolder<?>> leafStreams = new LinkedHashSet<>();

  public StreamingEvaluationContext(JavaSparkContext jsc, Pipeline pipeline,
      JavaStreamingContext jssc, long timeout) {
    super(jsc, pipeline);
    this.jssc = jssc;
    this.timeout = timeout;
  }

  /**
   * DStream holder Can also crate a DStream from a supplied queue of values, but mainly for
   * testing.
   */
  private class DStreamHolder<T> {

    private Iterable<Iterable<T>> values;
    private Coder<T> coder;
    private JavaDStream<WindowedValue<T>> dStream;

    DStreamHolder(Iterable<Iterable<T>> values, Coder<T> coder) {
      this.values = values;
      this.coder = coder;
    }

    DStreamHolder(JavaDStream<WindowedValue<T>> dStream) {
      this.dStream = dStream;
    }

    @SuppressWarnings("unchecked")
    JavaDStream<WindowedValue<T>> getDStream() {
      if (dStream == null) {
        WindowedValue.ValueOnlyWindowedValueCoder<T> windowCoder =
            WindowedValue.getValueOnlyCoder(coder);
        // create the DStream from queue
        Queue<JavaRDD<WindowedValue<T>>> rddQueue = new LinkedBlockingQueue<>();
        JavaRDD<WindowedValue<T>> lastRDD = null;
        for (Iterable<T> v : values) {
          Iterable<WindowedValue<T>> windowedValues =
              Iterables.transform(v, WindowingHelpers.<T>windowValueFunction());
          JavaRDD<WindowedValue<T>> rdd = getSparkContext().parallelize(
              CoderHelpers.toByteArrays(windowedValues, windowCoder)).map(
                  CoderHelpers.fromByteFunction(windowCoder));
          rddQueue.offer(rdd);
          lastRDD = rdd;
        }
        // create dstream from queue, one at a time,
        // with last as default in case batches repeat (graceful stops for example).
        // if the stream is empty, avoid creating a default empty RDD.
        // mainly for unit test so no reason to have this configurable.
        dStream = lastRDD != null ? jssc.queueStream(rddQueue, true, lastRDD)
            : jssc.queueStream(rddQueue, true);
      }
      return dStream;
    }
  }

  <T> void setDStreamFromQueue(
      PTransform<?, ?> transform, Iterable<Iterable<T>> values, Coder<T> coder) {
    pstreams.put((PValue) getOutput(transform), new DStreamHolder<>(values, coder));
  }

  <T> void setStream(PTransform<?, ?> transform, JavaDStream<WindowedValue<T>> dStream) {
    setStream((PValue) getOutput(transform), dStream);
  }

  <T> void setStream(PValue pvalue, JavaDStream<WindowedValue<T>> dStream) {
    DStreamHolder<T> dStreamHolder = new DStreamHolder<>(dStream);
    pstreams.put(pvalue, dStreamHolder);
    leafStreams.add(dStreamHolder);
  }

  boolean hasStream(PTransform<?, ?> transform) {
    PValue pvalue = (PValue) getInput(transform);
    return hasStream(pvalue);
  }

  boolean hasStream(PValue pvalue) {
    return pstreams.containsKey(pvalue);
  }

  JavaDStreamLike<?, ?, ?> getStream(PTransform<?, ?> transform) {
    return getStream((PValue) getInput(transform));
  }

  JavaDStreamLike<?, ?, ?> getStream(PValue pvalue) {
    DStreamHolder<?> dStreamHolder = pstreams.get(pvalue);
    JavaDStreamLike<?, ?, ?> dStream = dStreamHolder.getDStream();
    leafStreams.remove(dStreamHolder);
    return dStream;
  }

  // used to set the RDD from the DStream in the RDDHolder for transformation
  <T> void setInputRDD(
      PTransform<? extends PInput, ?> transform, JavaRDDLike<WindowedValue<T>, ?> rdd) {
    setRDD((PValue) getInput(transform), rdd);
  }

  // used to get the RDD transformation output and use it as the DStream transformation output
  JavaRDDLike<?, ?> getOutputRDD(PTransform<?, ?> transform) {
    return getRDD((PValue) getOutput(transform));
  }

  public JavaStreamingContext getStreamingContext() {
    return jssc;
  }

  @Override
  public void computeOutputs() {
    super.computeOutputs(); // in case the pipeline contains bounded branches as well.
    for (DStreamHolder<?> streamHolder : leafStreams) {
      computeOutput(streamHolder);
    } // force a DStream action
  }

  private static <T> void computeOutput(DStreamHolder<T> streamHolder) {
    JavaDStream<WindowedValue<T>> dStream = streamHolder.getDStream();
    // cache in DStream level not RDD
    // because there could be a difference in StorageLevel if the DStream is windowed.
    dStream.dstream().cache();
    dStream.foreachRDD(new VoidFunction<JavaRDD<WindowedValue<T>>>() {
      @Override
      public void call(JavaRDD<WindowedValue<T>> rdd) throws Exception {
        rdd.count();
      }
    });
  }

  @Override
  public void close(boolean gracefully) {
    if (timeout > 0) {
      jssc.awaitTerminationOrTimeout(timeout);
    } else {
      jssc.awaitTermination();
    }
    // stop streaming context gracefully, so checkpointing (and other computations) get to
    // finish before shutdown.
    jssc.stop(false, gracefully);
    state = State.DONE;
    super.close(false);
  }

  private State state = State.RUNNING;

  @Override
  public State getState() {
    return state;
  }

  @Override
  public State cancel() throws IOException {
    throw new UnsupportedOperationException(
        "Spark runner StreamingEvaluationContext does not support cancel.");
  }

  @Override
  public State waitUntilFinish() {
    throw new UnsupportedOperationException(
        "Spark runner StreamingEvaluationContext does not support waitUntilFinish.");
  }

  @Override
  public State waitUntilFinish(Duration duration) {
    throw new UnsupportedOperationException(
        "Spark runner StreamingEvaluationContext does not support waitUntilFinish.");
  }

  //---------------- override in order to expose in package
  @Override
  protected <InputT extends PInput> InputT getInput(PTransform<InputT, ?> transform) {
    return super.getInput(transform);
  }
  @Override
  protected <OutputT extends POutput> OutputT getOutput(PTransform<?, OutputT> transform) {
    return super.getOutput(transform);
  }

  @Override
  protected JavaSparkContext getSparkContext() {
    return super.getSparkContext();
  }

  @Override
  protected SparkRuntimeContext getRuntimeContext() {
    return super.getRuntimeContext();
  }

  @Override
  public void setCurrentTransform(AppliedPTransform<?, ?, ?> transform) {
    super.setCurrentTransform(transform);
  }

  @Override
  protected AppliedPTransform<?, ?, ?> getCurrentTransform() {
    return super.getCurrentTransform();
  }

  @Override
  protected <T> void setOutputRDD(PTransform<?, ?> transform,
      JavaRDDLike<WindowedValue<T>, ?> rdd) {
    super.setOutputRDD(transform, rdd);
  }

  @Override
  protected <T> void setOutputRDDFromValues(PTransform<?, ?> transform, Iterable<T> values,
      Coder<T> coder) {
    super.setOutputRDDFromValues(transform, values, coder);
  }

  @Override
  protected boolean hasOutputRDD(PTransform<? extends PInput, ?> transform) {
    return super.hasOutputRDD(transform);
  }
}
