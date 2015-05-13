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

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.PipelineResult;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderRegistry;
import com.google.cloud.dataflow.sdk.coders.IterableCoder;
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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Evaluation context allows us to define how pipeline instructions
 */
public class EvaluationContext implements EvaluationResult {
  private final JavaSparkContext jsc;
  private final Pipeline pipeline;
  private final SparkRuntimeContext runtime;
  private final CoderRegistry registry;
  private final Map<PValue, JavaRDDLike<?, ?>> rdds = new HashMap<>();
  private final Set<PValue> multireads = new HashSet<>();
  private final Map<PValue, Object> pobjects = new HashMap<>();
  private final Map<PValue, Iterable<WindowedValue<?>>> pview = new HashMap<>();

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

  <T> Coder<T> getDefaultCoder(T example) {
    Coder<T> defaultCoder = registry.getDefaultCoder(example);
    if (defaultCoder == null) {
      if (example instanceof Iterable) {
        Object first = ((Iterable<?>) example).iterator().next();
        @SuppressWarnings("unchecked")
        Coder<T> coder = (Coder<T>) IterableCoder.of(getDefaultCoder(first));
        return coder;
      } else {
        throw new IllegalStateException(String.format("Couldn't determine the default coder for " +
            "an example  of class [%s]", example.getClass()));
      }
    }
    return defaultCoder;
  }

  /**
   * Coder<Iterable<Object>> getDefaultIterableCoder(Iterables iter) {
   * <p/>
   * }
   */

  <I extends PInput> I getInput(PTransform<I, ?> transform) {
    @SuppressWarnings("unchecked")
    I input = (I) pipeline.getInput(transform);
    return input;
  }

  <O extends POutput> O getOutput(PTransform<?, O> transform) {
    @SuppressWarnings("unchecked")
    O output = (O) pipeline.getOutput(transform);
    return output;
  }

  void setOutputRDD(PTransform<?, ?> transform, JavaRDDLike<?, ?> rdd) {
    rdds.put((PValue) getOutput(transform), rdd);
  }

  void setPView(PValue view, Iterable<WindowedValue<?>> value) {
    pview.put(view, value);
  }

  JavaRDDLike<?, ?> getRDD(PValue pvalue) {
    JavaRDDLike<?, ?> rdd = rdds.get(pvalue);
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
  }

  JavaRDDLike<?, ?> getInputRDD(PTransform transform) {
    return getRDD((PValue) pipeline.getInput(transform));
  }


  <T> Iterable<WindowedValue<?>> getPCollectionView(PCollectionView<T> view) {
    Iterable<WindowedValue<?>> value = pview.get(view);
    return value;
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
  public <T> Iterable<T> get(PCollection<T> pcollection) {
    @SuppressWarnings("unchecked")
    JavaRDDLike<T, ?> rdd = (JavaRDDLike<T, ?>) getRDD(pcollection);
    final Coder<T> coder = pcollection.getCoder();
    JavaRDDLike<byte[], ?> bytesRDD = rdd.map(CoderHelpers.toByteFunction(coder));
    List<byte[]> clientBytes = bytesRDD.collect();
    return Iterables.transform(clientBytes, new Function<byte[], T>() {
      @Override
      public T apply(byte[] bytes) {
        return (T) CoderHelpers.fromByteArray(bytes, coder);
      }
    });
  }

  @Override
  public void close() {
    jsc.stop();
  }

  @Override
  public State getState() {
    return State.UNKNOWN;
  }
}
