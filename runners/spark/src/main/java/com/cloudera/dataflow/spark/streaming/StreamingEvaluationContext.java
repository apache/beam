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


import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.transforms.AppliedPTransform;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.values.PInput;
import com.google.cloud.dataflow.sdk.values.POutput;
import com.google.cloud.dataflow.sdk.values.PValue;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaDStreamLike;
import org.apache.spark.streaming.api.java.JavaStreamingContext;


import com.cloudera.dataflow.spark.EvaluationContext;
import com.cloudera.dataflow.spark.SparkRuntimeContext;

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

    public DStreamHolder(Iterable<Iterable<T>> values, Coder<T> coder) {
      this.values = values;
      this.coder = coder;
    }

    public DStreamHolder(JavaDStream<WindowedValue<T>> dStream) {
      this.dStream = dStream;
    }

    @SuppressWarnings("unchecked")
    public JavaDStream<WindowedValue<T>> getDStream() {
      if (dStream == null) {
        // create the DStream from values
        Queue<JavaRDD<WindowedValue<T>>> rddQueue = new LinkedBlockingQueue<>();
        for (Iterable<T> v : values) {
          setOutputRDDFromValues(currentTransform.getTransform(), v, coder);
          rddQueue.offer((JavaRDD<WindowedValue<T>>) getOutputRDD(currentTransform.getTransform()));
        }
        // create dstream from queue, one at a time, no defaults
        // mainly for unit test so no reason to have this configurable
        dStream = jssc.queueStream(rddQueue, true);
      }
      return dStream;
    }
  }

  public <T> void setDStreamFromQueue(PTransform<?, ?> transform, Iterable<Iterable<T>> values,
      Coder<T> coder) {
    pstreams.put((PValue) getOutput(transform), new DStreamHolder<>(values, coder));
  }

  public <T, R extends JavaRDDLike<WindowedValue<T>, R>>
      void setStream(PTransform<?, ?> transform, JavaDStreamLike<WindowedValue<T>, ?, R> dStream) {
    PValue pvalue = (PValue) getOutput(transform);
    @SuppressWarnings("unchecked")
    DStreamHolder<T> dStreamHolder = new DStreamHolder((JavaDStream) dStream);
    pstreams.put(pvalue, dStreamHolder);
    leafStreams.add(dStreamHolder);
  }

  boolean hasStream(PTransform<?, ?> transform) {
    PValue pvalue = (PValue) getInput(transform);
    return pstreams.containsKey(pvalue);
  }

  public JavaDStreamLike<?, ?, ?> getStream(PTransform<?, ?> transform) {
    PValue pvalue = (PValue) getInput(transform);
    DStreamHolder<?> dStreamHolder = pstreams.get(pvalue);
    JavaDStreamLike<?, ?, ?> dStream = dStreamHolder.getDStream();
    leafStreams.remove(dStreamHolder);
    return dStream;
  }

  // used to set the RDD from the DStream in the RDDHolder for transformation
  public <T> void setInputRDD(PTransform<? extends PInput, ?> transform,
      JavaRDDLike<WindowedValue<T>, ?> rdd) {
    setRDD((PValue) getInput(transform), rdd);
  }

  // used to get the RDD transformation output and use it as the DStream transformation output
  public JavaRDDLike<?, ?> getOutputRDD(PTransform<?, ?> transform) {
    return getRDD((PValue) getOutput(transform));
  }

  public JavaStreamingContext getStreamingContext() {
    return jssc;
  }

  @Override
  protected void computeOutputs() {
    for (DStreamHolder<?> streamHolder : leafStreams) {
      @SuppressWarnings("unchecked")
      JavaDStream<WindowedValue<?>> stream = (JavaDStream) streamHolder.getDStream();
      stream.foreachRDD(new Function<JavaRDD<WindowedValue<?>>, Void>() {
        @Override
        public Void call(JavaRDD<WindowedValue<?>> rdd) throws Exception {
          rdd.rdd().cache();
          rdd.count();
          return null;
        }
      }); // force a DStream action
    }
  }

  @Override
  public void close() {
    if (timeout > 0) {
      jssc.awaitTerminationOrTimeout(timeout);
    } else {
      jssc.awaitTermination();
    }
    //TODO: stop gracefully ?
    jssc.stop(false, false);
    state = State.DONE;
    super.close();
  }

  private State state = State.RUNNING;

  @Override
  public State getState() {
    return state;
  }

  //---------------- override in order to expose in package
  @Override
  protected <O extends POutput> O getOutput(PTransform<?, O> transform) {
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
  protected void setCurrentTransform(AppliedPTransform<?, ?, ?> transform) {
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
