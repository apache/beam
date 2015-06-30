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
  private final Map<PValue, JavaRDDLike<?, ?>> rdds = new LinkedHashMap<>();
  private final Set<JavaRDDLike<?, ?>> leafRdds = new LinkedHashSet<>();
  private final Set<PValue> multireads = new LinkedHashSet<>();
  private final Map<PValue, Object> pobjects = new LinkedHashMap<>();
  private final Map<PValue, Iterable<WindowedValue<?>>> pview = new LinkedHashMap<>();
  private AppliedPTransform<?, ?, ?> currentTransform;

  public EvaluationContext(JavaSparkContext jsc, Pipeline pipeline) {
    this.jsc = jsc;
    this.pipeline = pipeline;
    this.registry = pipeline.getCoderRegistry();
    this.runtime = new SparkRuntimeContext(jsc, pipeline);
  }

  JavaSparkContext getSparkContext() {
    return jsc;
  }

  Pipeline getPipeline() {
    return pipeline;
  }

  SparkRuntimeContext getRuntimeContext() {
    return runtime;
  }

  void setCurrentTransform(AppliedPTransform<?, ?, ?> transform) {
    this.currentTransform = transform;
  }

  <I extends PInput> I getInput(PTransform<I, ?> transform) {
    checkArgument(currentTransform != null && currentTransform.getTransform() == transform,
        "can only be called with current transform");
    @SuppressWarnings("unchecked")
    I input = (I) currentTransform.getInput();
    return input;
  }

  <O extends POutput> O getOutput(PTransform<?, O> transform) {
    checkArgument(currentTransform != null && currentTransform.getTransform() == transform,
        "can only be called with current transform");
    @SuppressWarnings("unchecked")
    O output = (O) currentTransform.getOutput();
    return output;
  }

  void setOutputRDD(PTransform<?, ?> transform, JavaRDDLike<?, ?> rdd) {
    setRDD((PValue) getOutput(transform), rdd);
  }

  void setPView(PValue view, Iterable<WindowedValue<?>> value) {
    pview.put(view, value);
  }

  JavaRDDLike<?, ?> getRDD(PValue pvalue) {
    JavaRDDLike<?, ?> rdd = rdds.get(pvalue);
    leafRdds.remove(rdd);
    if (multireads.contains(pvalue)) {
      // Ensure the RDD is marked as cached
      rdd.rdd().cache();
    } else {
      multireads.add(pvalue);
    }
    return rdd;
  }

  void setRDD(PValue pvalue, JavaRDDLike<?, ?> rdd) {
    rdds.put(pvalue, rdd);
    leafRdds.add(rdd);
  }

  JavaRDDLike<?, ?> getInputRDD(PTransform transform) {
    return getRDD((PValue) getInput(transform));
  }


  <T> Iterable<WindowedValue<?>> getPCollectionView(PCollectionView<T> view) {
    return pview.get(view);
  }

  /**
   * Computes the outputs for all RDDs that are leaves in the DAG and do not have any
   * actions (like saving to a file) registered on them (i.e. they are performed for side
   * effects).
   */
  void computeOutputs() {
    for (JavaRDDLike<?, ?> rdd : leafRdds) {
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
    if (rdds.containsKey(value)) {
      JavaRDDLike<?, ?> rdd = rdds.get(value);
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
    JavaRDDLike<T, ?> rdd = (JavaRDDLike<T, ?>) getRDD(pcollection);
    // Use a coder to convert the objects in the PCollection to byte arrays, so they
    // can be transferred over the network.
    final Coder<T> coder = pcollection.getCoder();
    JavaRDDLike<byte[], ?> bytesRDD = rdd.map(CoderHelpers.toByteFunction(coder));
    List<byte[]> clientBytes = bytesRDD.collect();
    return Iterables.transform(clientBytes, new Function<byte[], T>() {
      @Override
      public T apply(byte[] bytes) {
        return CoderHelpers.fromByteArray(bytes, coder);
      }
    });
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
