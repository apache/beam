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

import static com.google.common.base.Preconditions.checkArgument;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderRegistry;
import com.google.cloud.dataflow.sdk.runners.AggregatorRetrievalException;
import com.google.cloud.dataflow.sdk.runners.AggregatorValues;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.AppliedPTransform;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.PInput;
import com.google.cloud.dataflow.sdk.values.POutput;
import com.google.cloud.dataflow.sdk.values.PValue;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.JavaSparkContext;


/**
 * Evaluation context allows us to define how pipeline instructions.
 */
public class EvaluationContext implements EvaluationResult {
  private final JavaSparkContext jsc;
  private final Pipeline pipeline;
  private final SparkRuntimeContext runtime;
  private final CoderRegistry registry;
  private final Map<PValue, RDDHolder<?>> pcollections = new LinkedHashMap<>();
  private final Set<RDDHolder<?>> leafRdds = new LinkedHashSet<>();
  private final Set<PValue> multireads = new LinkedHashSet<>();
  private final Map<PValue, Object> pobjects = new LinkedHashMap<>();
  private final Map<PValue, Iterable<? extends WindowedValue<?>>> pview = new LinkedHashMap<>();
  protected AppliedPTransform<?, ?, ?> currentTransform;

  public EvaluationContext(JavaSparkContext jsc, Pipeline pipeline) {
    this.jsc = jsc;
    this.pipeline = pipeline;
    this.registry = pipeline.getCoderRegistry();
    this.runtime = new SparkRuntimeContext(jsc, pipeline);
  }

  /**
   * Holds an RDD or values for deferred conversion to an RDD if needed. PCollections are
   * sometimes created from a collection of objects (using RDD parallelize) and then
   * only used to create View objects; in which case they do not need to be
   * converted to bytes since they are not transferred across the network until they are
   * broadcast.
   */
  private class RDDHolder<T> {

    private Iterable<T> values;
    private Coder<T> coder;
    private JavaRDDLike<WindowedValue<T>, ?> rdd;

    RDDHolder(Iterable<T> values, Coder<T> coder) {
      this.values = values;
      this.coder = coder;
    }

    RDDHolder(JavaRDDLike<WindowedValue<T>, ?> rdd) {
      this.rdd = rdd;
    }

    public JavaRDDLike<WindowedValue<T>, ?> getRDD() {
      if (rdd == null) {
        Iterable<WindowedValue<T>> windowedValues = Iterables.transform(values,
            new Function<T, WindowedValue<T>>() {
            @Override
            public WindowedValue<T> apply(T t) {
             // TODO: this is wrong if T is a TimestampedValue
              return WindowedValue.valueInEmptyWindows(t);
            }
        });
        WindowedValue.ValueOnlyWindowedValueCoder<T> windowCoder =
            WindowedValue.getValueOnlyCoder(coder);
        rdd = jsc.parallelize(CoderHelpers.toByteArrays(windowedValues, windowCoder))
            .map(CoderHelpers.fromByteFunction(windowCoder));
      }
      return rdd;
    }

    public Iterable<T> getValues(PCollection<T> pcollection) {
      if (values == null) {
        coder = pcollection.getCoder();
        JavaRDDLike<byte[], ?> bytesRDD = rdd.map(WindowingHelpers.<T>unwindowFunction())
            .map(CoderHelpers.toByteFunction(coder));
        List<byte[]> clientBytes = bytesRDD.collect();
        values = Iterables.transform(clientBytes, new Function<byte[], T>() {
          @Override
          public T apply(byte[] bytes) {
            return CoderHelpers.fromByteArray(bytes, coder);
          }
        });
      }
      return values;
    }

    public Iterable<WindowedValue<T>> getWindowedValues(PCollection<T> pcollection) {
      return Iterables.transform(get(pcollection), new Function<T, WindowedValue<T>>() {
        @Override
        public WindowedValue<T> apply(T t) {
          return WindowedValue.valueInEmptyWindows(t); // TODO: not the right place?
        }
      });
    }
  }

  protected JavaSparkContext getSparkContext() {
    return jsc;
  }

  protected Pipeline getPipeline() {
    return pipeline;
  }

  protected SparkRuntimeContext getRuntimeContext() {
    return runtime;
  }

  protected void setCurrentTransform(AppliedPTransform<?, ?, ?> transform) {
    this.currentTransform = transform;
  }

  protected AppliedPTransform<?, ?, ?> getCurrentTransform() {
    return currentTransform;
  }

  protected <I extends PInput> I getInput(PTransform<I, ?> transform) {
    checkArgument(currentTransform != null && currentTransform.getTransform() == transform,
        "can only be called with current transform");
    @SuppressWarnings("unchecked")
    I input = (I) currentTransform.getInput();
    return input;
  }

  protected <O extends POutput> O getOutput(PTransform<?, O> transform) {
    checkArgument(currentTransform != null && currentTransform.getTransform() == transform,
        "can only be called with current transform");
    @SuppressWarnings("unchecked")
    O output = (O) currentTransform.getOutput();
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

  void setPView(PValue view, Iterable<? extends WindowedValue<?>> value) {
    pview.put(view, value);
  }

  protected boolean hasOutputRDD(PTransform<? extends PInput, ?> transform) {
    PValue pvalue = (PValue) getOutput(transform);
    return pcollections.containsKey(pvalue);
  }

  protected JavaRDDLike<?, ?> getRDD(PValue pvalue) {
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

  JavaRDDLike<?, ?> getInputRDD(PTransform<? extends PInput, ?> transform) {
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
  protected void computeOutputs() {
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
    return runtime.getAggregatorValue(named, resultType);
  }

  @Override
  public <T> AggregatorValues<T> getAggregatorValues(Aggregator<?, T> aggregator)
      throws AggregatorRetrievalException {
    return runtime.getAggregatorValues(aggregator);
  }

  @Override
  public <T> Iterable<T> get(PCollection<T> pcollection) {
    @SuppressWarnings("unchecked")
    RDDHolder<T> rddHolder = (RDDHolder<T>) pcollections.get(pcollection);
    return rddHolder.getValues(pcollection);
  }

  <T> Iterable<WindowedValue<T>> getWindowedValues(PCollection<T> pcollection) {
    @SuppressWarnings("unchecked")
    RDDHolder<T> rddHolder = (RDDHolder<T>) pcollections.get(pcollection);
    return rddHolder.getWindowedValues(pcollection);
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
}
