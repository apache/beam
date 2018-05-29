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

import com.google.common.collect.Iterables;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.core.construction.TransformInputs;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.coders.CoderHelpers;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * The EvaluationContext allows us to define pipeline instructions and translate between
 * {@code PObject<T>}s or {@code PCollection<T>}s and Ts or DStreams/RDDs of Ts.
 */
public class EvaluationContext {
  private final JavaSparkContext jsc;
  private JavaStreamingContext jssc;
  private final Pipeline pipeline;
  private final Map<PValue, Dataset> datasets = new LinkedHashMap<>();
  private final Map<PValue, Dataset> pcollections = new LinkedHashMap<>();
  private final Set<Dataset> leaves = new LinkedHashSet<>();
  private final Map<PValue, Object> pobjects = new LinkedHashMap<>();
  private AppliedPTransform<?, ?, ?> currentTransform;
  private final SparkPCollectionView pviews = new SparkPCollectionView();
  private final Map<PCollection, Long> cacheCandidates = new HashMap<>();
  private final PipelineOptions options;
  private final SerializablePipelineOptions serializableOptions;

  public EvaluationContext(JavaSparkContext jsc, Pipeline pipeline, PipelineOptions options) {
    this.jsc = jsc;
    this.pipeline = pipeline;
    this.options = options;
    this.serializableOptions = new SerializablePipelineOptions(options);
  }

  public EvaluationContext(
      JavaSparkContext jsc, Pipeline pipeline, PipelineOptions options, JavaStreamingContext jssc) {
    this(jsc, pipeline, options);
    this.jssc = jssc;
  }

  public JavaSparkContext getSparkContext() {
    return jsc;
  }

  public JavaStreamingContext getStreamingContext() {
    return jssc;
  }

  public Pipeline getPipeline() {
    return pipeline;
  }

  public PipelineOptions getOptions() {
    return options;
  }

  public SerializablePipelineOptions getSerializableOptions() {
    return serializableOptions;
  }

  public void setCurrentTransform(AppliedPTransform<?, ?, ?> transform) {
    this.currentTransform = transform;
  }

  public AppliedPTransform<?, ?, ?> getCurrentTransform() {
    return currentTransform;
  }

  public <T extends PValue> T getInput(PTransform<T, ?> transform) {
    @SuppressWarnings("unchecked")
    T input =
        (T) Iterables.getOnlyElement(TransformInputs.nonAdditionalInputs(getCurrentTransform()));
    return input;
  }

  public <T> Map<TupleTag<?>, PValue> getInputs(PTransform<?, ?> transform) {
    checkArgument(currentTransform != null && currentTransform.getTransform() == transform,
        "can only be called with current transform");
    return currentTransform.getInputs();
  }

  public <T extends PValue> T getOutput(PTransform<?, T> transform) {
    @SuppressWarnings("unchecked")
    T output = (T) Iterables.getOnlyElement(getOutputs(transform).values());
    return output;
  }

  public Map<TupleTag<?>, PValue> getOutputs(PTransform<?, ?> transform) {
    checkArgument(currentTransform != null && currentTransform.getTransform() == transform,
        "can only be called with current transform");
    return currentTransform.getOutputs();
  }

  private boolean shouldCache(PValue pvalue) {
    if ((pvalue instanceof PCollection)
        && cacheCandidates.containsKey(pvalue)
        && cacheCandidates.get(pvalue) > 1) {
      return true;
    }
    return false;
  }

  public void putDataset(PTransform<?, ? extends PValue> transform, Dataset dataset,
      boolean forceCache) {
    putDataset(getOutput(transform), dataset, forceCache);
  }


  public void putDataset(PTransform<?, ? extends PValue> transform, Dataset dataset) {
    putDataset(transform, dataset,  false);
  }

  public void putDataset(PValue pvalue, Dataset dataset, boolean forceCache) {
    try {
      dataset.setName(pvalue.getName());
    } catch (IllegalStateException e) {
      // name not set, ignore
    }
    if ((forceCache || shouldCache(pvalue)) && pvalue instanceof PCollection) {
      // we cache only PCollection
      Coder<?> coder = ((PCollection<?>) pvalue).getCoder();
      Coder<? extends BoundedWindow> wCoder =
          ((PCollection<?>) pvalue).getWindowingStrategy().getWindowFn().windowCoder();
      dataset.cache(storageLevel(), WindowedValue.getFullCoder(coder, wCoder));
    }
    datasets.put(pvalue, dataset);
    leaves.add(dataset);
  }

  <T> void putBoundedDatasetFromValues(
      PTransform<?, ? extends PValue> transform, Iterable<T> values, Coder<T> coder) {
    PValue output = getOutput(transform);
    if (shouldCache(output)) {
      // eagerly create the RDD, as it will be reused.
      Iterable<WindowedValue<T>> elems =
          Iterables.transform(values, WindowingHelpers.windowValueFunction());
      WindowedValue.ValueOnlyWindowedValueCoder<T> windowCoder =
          WindowedValue.getValueOnlyCoder(coder);
      JavaRDD<WindowedValue<T>> rdd =
          getSparkContext().parallelize(CoderHelpers.toByteArrays(elems, windowCoder))
          .map(CoderHelpers.fromByteFunction(windowCoder));
      putDataset(transform, new BoundedDataset<>(rdd));
    } else {
      // create a BoundedDataset that would create a RDD on demand
      datasets.put(getOutput(transform), new BoundedDataset<>(values, jsc, coder));
    }
  }

  public Dataset borrowDataset(PTransform<? extends PValue, ?> transform) {
    return borrowDataset(getInput(transform));
  }

  public Dataset borrowDataset(PValue pvalue) {
    Dataset dataset = datasets.get(pvalue);
    leaves.remove(dataset);
    return dataset;
  }

  /**
   * Computes the outputs for all RDDs that are leaves in the DAG and do not have any actions (like
   * saving to a file) registered on them (i.e. they are performed for side effects).
   */
  public void computeOutputs() {
    for (Dataset dataset : leaves) {
      dataset.action(); // force computation.
    }
  }

  /**
   * Retrieve an object of Type T associated with the PValue passed in.
   *
   * @param value PValue to retrieve associated data for.
   * @param <T>  Type of object to return.
   * @return Native object.
   */
  @SuppressWarnings("unchecked")
  public <T> T get(PValue value) {
    if (pobjects.containsKey(value)) {
      T result = (T) pobjects.get(value);
      return result;
    }
    if (pcollections.containsKey(value)) {
      JavaRDD<?> rdd = ((BoundedDataset) pcollections.get(value)).getRDD();
      T res = (T) Iterables.getOnlyElement(rdd.collect());
      pobjects.put(value, res);
      return res;
    }
    throw new IllegalStateException("Cannot resolve un-known PObject: " + value);
  }

  /**
   * Retrun the current views creates in the pipepline.
   *
   * @return SparkPCollectionView
   */
  public SparkPCollectionView getPViews() {
    return pviews;
  }

  /**
   * Adds/Replaces a view to the current views creates in the pipepline.
   *
   * @param view - Identifier of the view
   * @param value - Actual value of the view
   * @param coder - Coder of the value
   */
  public void putPView(
      PCollectionView<?> view,
      Iterable<WindowedValue<?>> value,
      Coder<Iterable<WindowedValue<?>>> coder) {
    pviews.putPView(view, value, coder);
  }

  /**
   * Get the map of cache candidates hold by the evaluation context.
   *
   * @return The current {@link Map} of cache candidates.
   */
  public Map<PCollection, Long> getCacheCandidates() {
    return this.cacheCandidates;
  }

  <T> Iterable<WindowedValue<T>> getWindowedValues(PCollection<T> pcollection) {
    @SuppressWarnings("unchecked")
    BoundedDataset<T> boundedDataset = (BoundedDataset<T>) datasets.get(pcollection);
    leaves.remove(boundedDataset);
    return boundedDataset.getValues(pcollection);
  }

  public String storageLevel() {
    return serializableOptions.get().as(SparkPipelineOptions.class).getStorageLevel();
  }

}
