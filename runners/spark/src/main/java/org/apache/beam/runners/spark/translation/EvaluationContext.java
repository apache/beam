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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.runners.spark.EvaluationResult;
import org.apache.beam.runners.spark.aggregators.AccumulatorSingleton;
import org.apache.beam.runners.spark.coders.CoderHelpers;
import org.apache.beam.sdk.AggregatorRetrievalException;
import org.apache.beam.sdk.AggregatorValues;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.JavaSparkContext;
import org.joda.time.Duration;


/**
 * Evaluation context allows us to define how pipeline instructions.
 */
public class EvaluationContext implements EvaluationResult {
  private final JavaSparkContext jsc;
  private final Pipeline pipeline;
  private final SparkRuntimeContext runtime;
  private final Map<PValue, RDDHolder<?>> pcollections = new LinkedHashMap<>();
  private final Set<RDDHolder<?>> leafRdds = new LinkedHashSet<>();
  private final Set<PValue> multireads = new LinkedHashSet<>();
  private final Map<PValue, Object> pobjects = new LinkedHashMap<>();
  private final Map<PValue, Iterable<? extends WindowedValue<?>>> pview = new LinkedHashMap<>();
  protected AppliedPTransform<?, ?, ?> currentTransform;

  public EvaluationContext(JavaSparkContext jsc, Pipeline pipeline) {
    this.jsc = jsc;
    this.pipeline = pipeline;
    this.runtime = new SparkRuntimeContext(pipeline, jsc);
  }

  /**
   * Holds an RDD or values for deferred conversion to an RDD if needed. PCollections are
   * sometimes created from a collection of objects (using RDD parallelize) and then
   * only used to create View objects; in which case they do not need to be
   * converted to bytes since they are not transferred across the network until they are
   * broadcast.
   */
  private class RDDHolder<T> {

    private Iterable<WindowedValue<T>> windowedValues;
    private Coder<T> coder;
    private JavaRDDLike<WindowedValue<T>, ?> rdd;

    RDDHolder(Iterable<T> values, Coder<T> coder) {
      this.windowedValues =
          Iterables.transform(values, WindowingHelpers.<T>windowValueFunction());
      this.coder = coder;
    }

    RDDHolder(JavaRDDLike<WindowedValue<T>, ?> rdd) {
      this.rdd = rdd;
    }

    JavaRDDLike<WindowedValue<T>, ?> getRDD() {
      if (rdd == null) {
        WindowedValue.ValueOnlyWindowedValueCoder<T> windowCoder =
            WindowedValue.getValueOnlyCoder(coder);
        rdd = jsc.parallelize(CoderHelpers.toByteArrays(windowedValues, windowCoder))
            .map(CoderHelpers.fromByteFunction(windowCoder));
      }
      return rdd;
    }

    Iterable<WindowedValue<T>> getValues(PCollection<T> pcollection) {
      if (windowedValues == null) {
        WindowFn<?, ?> windowFn =
                pcollection.getWindowingStrategy().getWindowFn();
        Coder<? extends BoundedWindow> windowCoder = windowFn.windowCoder();
        final WindowedValue.WindowedValueCoder<T> windowedValueCoder;
            if (windowFn instanceof GlobalWindows) {
              windowedValueCoder =
                  WindowedValue.ValueOnlyWindowedValueCoder.of(pcollection.getCoder());
            } else {
              windowedValueCoder =
                  WindowedValue.FullWindowedValueCoder.of(pcollection.getCoder(), windowCoder);
            }
        JavaRDDLike<byte[], ?> bytesRDD =
            rdd.map(CoderHelpers.toByteFunction(windowedValueCoder));
        List<byte[]> clientBytes = bytesRDD.collect();
        windowedValues = Iterables.transform(clientBytes,
            new Function<byte[], WindowedValue<T>>() {
          @Override
          public WindowedValue<T> apply(byte[] bytes) {
            return CoderHelpers.fromByteArray(bytes, windowedValueCoder);
          }
        });
      }
      return windowedValues;
    }
  }

  protected JavaSparkContext getSparkContext() {
    return jsc;
  }

  public Pipeline getPipeline() {
    return pipeline;
  }

  protected SparkRuntimeContext getRuntimeContext() {
    return runtime;
  }

  public void setCurrentTransform(AppliedPTransform<?, ?, ?> transform) {
    this.currentTransform = transform;
  }

  protected AppliedPTransform<?, ?, ?> getCurrentTransform() {
    return currentTransform;
  }

  protected <T extends PInput> T getInput(PTransform<T, ?> transform) {
    checkArgument(currentTransform != null && currentTransform.getTransform() == transform,
        "can only be called with current transform");
    @SuppressWarnings("unchecked")
    T input = (T) currentTransform.getInput();
    return input;
  }

  protected <T extends POutput> T getOutput(PTransform<?, T> transform) {
    checkArgument(currentTransform != null && currentTransform.getTransform() == transform,
        "can only be called with current transform");
    @SuppressWarnings("unchecked")
    T output = (T) currentTransform.getOutput();
    return output;
  }

  protected  <T> void setOutputRDD(PTransform<?, ?> transform,
      JavaRDDLike<WindowedValue<T>, ?> rdd) {
    setRDD((PValue) getOutput(transform), rdd);
  }

  protected  <T> void setOutputRDDFromValues(PTransform<?, ?> transform, Iterable<T> values,
      Coder<T> coder) {
    pcollections.put((PValue) getOutput(transform), new RDDHolder<>(values, coder));
  }

  public void setPView(PValue view, Iterable<? extends WindowedValue<?>> value) {
    pview.put(view, value);
  }

  protected boolean hasOutputRDD(PTransform<? extends PInput, ?> transform) {
    PValue pvalue = (PValue) getOutput(transform);
    return pcollections.containsKey(pvalue);
  }

  public JavaRDDLike<?, ?> getRDD(PValue pvalue) {
    RDDHolder<?> rddHolder = pcollections.get(pvalue);
    JavaRDDLike<?, ?> rdd = rddHolder.getRDD();
    leafRdds.remove(rddHolder);
    if (multireads.contains(pvalue)) {
      // Ensure the RDD is marked as cached
      rdd.rdd().cache();
    } else {
      multireads.add(pvalue);
    }
    return rdd;
  }

  protected <T> void setRDD(PValue pvalue, JavaRDDLike<WindowedValue<T>, ?> rdd) {
    try {
      rdd.rdd().setName(pvalue.getName());
    } catch (IllegalStateException e) {
      // name not set, ignore
    }
    RDDHolder<T> rddHolder = new RDDHolder<>(rdd);
    pcollections.put(pvalue, rddHolder);
    leafRdds.add(rddHolder);
  }

  protected JavaRDDLike<?, ?> getInputRDD(PTransform<? extends PInput, ?> transform) {
    return getRDD((PValue) getInput(transform));
  }


  <T> Iterable<? extends WindowedValue<?>> getPCollectionView(PCollectionView<T> view) {
    return pview.get(view);
  }

  /**
   * Computes the outputs for all RDDs that are leaves in the DAG and do not have any
   * actions (like saving to a file) registered on them (i.e. they are performed for side
   * effects).
   */
  public void computeOutputs() {
    for (RDDHolder<?> rddHolder : leafRdds) {
      JavaRDDLike<?, ?> rdd = rddHolder.getRDD();
      rdd.rdd().cache(); // cache so that any subsequent get() is cheap
      rdd.count(); // force the RDD to be computed
    }
  }

  @Override
  public <T> T get(PValue value) {
    if (pobjects.containsKey(value)) {
      @SuppressWarnings("unchecked")
      T result = (T) pobjects.get(value);
      return result;
    }
    if (pcollections.containsKey(value)) {
      JavaRDDLike<?, ?> rdd = pcollections.get(value).getRDD();
      @SuppressWarnings("unchecked")
      T res = (T) Iterables.getOnlyElement(rdd.collect());
      pobjects.put(value, res);
      return res;
    }
    throw new IllegalStateException("Cannot resolve un-known PObject: " + value);
  }

  @Override
  public <T> T getAggregatorValue(String named, Class<T> resultType) {
    return runtime.getAggregatorValue(AccumulatorSingleton.getInstance(jsc), named, resultType);
  }

  @Override
  public <T> AggregatorValues<T> getAggregatorValues(Aggregator<?, T> aggregator)
      throws AggregatorRetrievalException {
    return runtime.getAggregatorValues(AccumulatorSingleton.getInstance(jsc), aggregator);
  }

  @Override
  public <T> Iterable<T> get(PCollection<T> pcollection) {
    @SuppressWarnings("unchecked")
    RDDHolder<T> rddHolder = (RDDHolder<T>) pcollections.get(pcollection);
    Iterable<WindowedValue<T>> windowedValues = rddHolder.getValues(pcollection);
    return Iterables.transform(windowedValues, WindowingHelpers.<T>unwindowValueFunction());
  }

  <T> Iterable<WindowedValue<T>> getWindowedValues(PCollection<T> pcollection) {
    @SuppressWarnings("unchecked")
    RDDHolder<T> rddHolder = (RDDHolder<T>) pcollections.get(pcollection);
    return rddHolder.getValues(pcollection);
  }

  @Override
  public void close() {
    SparkContextFactory.stopSparkContext(jsc);
  }

  /** The runner is blocking. */
  @Override
  public State getState() {
    return State.DONE;
  }

  @Override
  public State cancel() throws IOException {
    throw new UnsupportedOperationException(
        "Spark runner EvaluationContext does not support cancel.");
  }

  @Override
  public State waitUntilFinish()
      throws IOException, InterruptedException {
    return waitUntilFinish(Duration.millis(-1));
  }

  @Override
  public State waitUntilFinish(Duration duration)
      throws IOException, InterruptedException {
    throw new UnsupportedOperationException(
        "Spark runner EvaluationContext does not support waitUntilFinish.");
  }
}
