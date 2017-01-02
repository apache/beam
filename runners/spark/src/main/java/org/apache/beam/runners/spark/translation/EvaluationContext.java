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
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.translation.streaming.UnboundedDataset;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
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
  private final SparkRuntimeContext runtime;
  private final Pipeline pipeline;
  private final Map<PValue, Dataset> datasets = new LinkedHashMap<>();
  private final Map<PValue, Dataset> pcollections = new LinkedHashMap<>();
  private final Set<Dataset> leaves = new LinkedHashSet<>();
  private final Set<PValue> multiReads = new LinkedHashSet<>();
  private final Map<PValue, Object> pobjects = new LinkedHashMap<>();
  private AppliedPTransform<?, ?, ?> currentTransform;
  private final SparkPCollectionView pviews = new SparkPCollectionView();

  public EvaluationContext(JavaSparkContext jsc, Pipeline pipeline) {
    this.jsc = jsc;
    this.pipeline = pipeline;
    this.runtime = new SparkRuntimeContext(pipeline);
  }

  public EvaluationContext(JavaSparkContext jsc, Pipeline pipeline, JavaStreamingContext jssc) {
    this(jsc, pipeline);
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

  public SparkRuntimeContext getRuntimeContext() {
    return runtime;
  }

  public void setCurrentTransform(AppliedPTransform<?, ?, ?> transform) {
    this.currentTransform = transform;
  }

  public <T extends PInput> T getInput(PTransform<T, ?> transform) {
    checkArgument(currentTransform != null && currentTransform.getTransform() == transform,
        "can only be called with current transform");
    @SuppressWarnings("unchecked")
    T input = (T) currentTransform.getInput();
    return input;
  }

  public <T extends POutput> T getOutput(PTransform<?, T> transform) {
    checkArgument(currentTransform != null && currentTransform.getTransform() == transform,
        "can only be called with current transform");
    @SuppressWarnings("unchecked")
    T output = (T) currentTransform.getOutput();
    return output;
  }

  public void putDataset(PTransform<?, ?> transform, Dataset dataset) {
    putDataset((PValue) getOutput(transform), dataset);
  }

  public void putDataset(PValue pvalue, Dataset dataset) {
    try {
      dataset.setName(pvalue.getName());
    } catch (IllegalStateException e) {
      // name not set, ignore
    }
    datasets.put(pvalue, dataset);
    leaves.add(dataset);
  }

  <T> void putBoundedDatasetFromValues(PTransform<?, ?> transform, Iterable<T> values,
                                       Coder<T> coder) {
    datasets.put((PValue) getOutput(transform), new BoundedDataset<>(values, jsc, coder));
  }

  public <T> void putUnboundedDatasetFromQueue(
      PTransform<?, ?> transform, Iterable<Iterable<T>> values, Coder<T> coder) {
    datasets.put((PValue) getOutput(transform), new UnboundedDataset<>(values, jssc, coder));
  }

  public Dataset borrowDataset(PTransform<?, ?> transform) {
    return borrowDataset((PValue) getInput(transform));
  }

  public Dataset borrowDataset(PValue pvalue) {
    Dataset dataset = datasets.get(pvalue);
    leaves.remove(dataset);
    if (multiReads.contains(pvalue)) {
      // Ensure the RDD is marked as cached
      dataset.cache(storageLevel());
    } else {
      multiReads.add(pvalue);
    }
    return dataset;
  }

  /**
   * Computes the outputs for all RDDs that are leaves in the DAG and do not have any actions (like
   * saving to a file) registered on them (i.e. they are performed for side effects).
   */
  public void computeOutputs() {
    for (Dataset dataset : leaves) {
      // cache so that any subsequent get() is cheap.
      dataset.cache(storageLevel());
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
   * Retrieves an iterable of results associated with the PCollection passed in.
   *
   * @param pcollection Collection we wish to translate.
   * @param <T>         Type of elements contained in collection.
   * @return Natively types result associated with collection.
   */
  public <T> Iterable<T> get(PCollection<T> pcollection) {
    @SuppressWarnings("unchecked")
    BoundedDataset<T> boundedDataset = (BoundedDataset<T>) datasets.get(pcollection);
    Iterable<WindowedValue<T>> windowedValues = boundedDataset.getValues(pcollection);
    return Iterables.transform(windowedValues, WindowingHelpers.<T>unwindowValueFunction());
  }

  /**
   * Retruns the current views creates in the pipepline.
   * @return SparkPCollectionView
   */
  public SparkPCollectionView getPviews() {
    return pviews;
  }

  /**
   * Adds/Replaces a view to the current views creates in the pipepline.
   * @param view - Identifier of the view
   * @param value - Actual value of the view
   * @param coder - Coder of the value
   */
  public void putPView(PCollectionView<?> view,
      Iterable<WindowedValue<?>> value,
      Coder<Iterable<WindowedValue<?>>> coder) {
    pviews.putPView(view, value, coder, jsc);
  }

  <T> Iterable<WindowedValue<T>> getWindowedValues(PCollection<T> pcollection) {
    @SuppressWarnings("unchecked")
    BoundedDataset<T> boundedDataset = (BoundedDataset<T>) datasets.get(pcollection);
    return boundedDataset.getValues(pcollection);
  }

  private String storageLevel() {
    return runtime.getPipelineOptions().as(SparkPipelineOptions.class).getStorageLevel();
  }

}
