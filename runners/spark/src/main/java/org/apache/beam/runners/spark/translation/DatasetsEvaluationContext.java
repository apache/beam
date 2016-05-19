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

import org.apache.beam.runners.spark.coders.EncoderHelpers;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.runners.AggregatorRetrievalException;
import org.apache.beam.sdk.runners.AggregatorValues;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;

import com.google.api.client.util.Lists;
import com.google.common.collect.Iterables;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * Evaluation context for batch using {@link Dataset}s.
 */
public class DatasetsEvaluationContext implements EvaluationContext {
  private final JavaSparkContext jsc;
  private final SQLContext sqc;
  private final Pipeline pipeline;
  private final SparkRuntimeContext runtime;
  private final Map<PValue, DatasetHolder<?>> pcollections = new LinkedHashMap<>();
  private final Set<DatasetHolder<?>> leafs = new LinkedHashSet<>();
  private final Set<PValue> multireads = new LinkedHashSet<>();
  private final Map<PValue, Object> pobjects = new LinkedHashMap<>();
  private final Map<PValue, Iterable<? extends WindowedValue<?>>> pview = new LinkedHashMap<>();
  protected AppliedPTransform<?, ?, ?> currentTransform;

  public DatasetsEvaluationContext(JavaSparkContext jsc, Pipeline pipeline) {
    this.jsc = jsc;
    this.sqc = new SQLContext(jsc);
    this.pipeline = pipeline;
    this.runtime = new SparkRuntimeContext(jsc, pipeline);
  }

  /**
   * Holds an Dataset or values for deferred conversion to an Dataset if needed. PCollections are
   * sometimes created from a collection of objects and then only used to create View objects;
   * in which case they do not need to be converted to bytes since they are not transferred across
   * the network until they are broadcast.
   */
  private class DatasetHolder<T> {

    private List<WindowedValue<T>> windowedValues;
    private Coder<T> coder;
    private Dataset<WindowedValue<T>> dataset;

    DatasetHolder(Iterable<T> values, Coder<T> coder) {
      this.windowedValues =
          Lists.newArrayList(Iterables.transform(values,
          WindowingHelpers.<T>windowValueFunction()));
      this.coder = coder;
    }

    DatasetHolder(Dataset<WindowedValue<T>> dataset) {
      this.dataset = dataset;
    }

    Dataset<WindowedValue<T>> getDataset() {
      if (dataset == null) {
        //TODO: optimize by sending bytes + coder and transforming on the node ?
        dataset = sqc.createDataset(windowedValues, EncoderHelpers.<WindowedValue<T>>encoder());
      }
      return dataset;
    }

    Iterable<WindowedValue<T>> getValues(PCollection<T> pcollection) {
      if (windowedValues == null) {
        //TODO: optimize by transforming to bytes, collecting and transforming back ?
        windowedValues = dataset.collectAsList();
      }
      return windowedValues;
    }
  }

  protected JavaSparkContext getSparkContext() {
    return jsc;
  }

  public SQLContext getSqc() {
    return sqc;
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

  public AppliedPTransform<?, ?, ?> getCurrentTransform() {
    return currentTransform;
  }

  public  <T extends PInput> T getInput(PTransform<T, ?> transform) {
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

  protected <T> void setOutputDataset(PTransform<?, ?> transform,
                                      Dataset<WindowedValue<T>> dataset) {
    setDataset((PValue) getOutput(transform), dataset);
  }

  protected  <T> void setOutputDatasetFromValues(PTransform<?, ?> transform,
                                                 Iterable<T> values,
                                                 Coder<T> coder) {
    pcollections.put((PValue) getOutput(transform), new DatasetHolder<>(values, coder));
  }

  void setPView(PValue view, Iterable<? extends WindowedValue<?>> value) {
    pview.put(view, value);
  }

  protected boolean hasOutputDataset(PTransform<? extends PInput, ?> transform) {
    PValue pvalue = (PValue) getOutput(transform);
    return pcollections.containsKey(pvalue);
  }

  protected Dataset<?> getDataset(PValue pvalue) {
    DatasetHolder<?> datasetHolder = pcollections.get(pvalue);
    Dataset<?> dataset = datasetHolder.getDataset();
    leafs.remove(datasetHolder);
    if (multireads.contains(pvalue)) {
      // Ensure the Dataset is marked as cached
      dataset.cache();
    } else {
      multireads.add(pvalue);
    }
    return dataset;
  }

  protected <T> void setDataset(PValue pvalue, Dataset<WindowedValue<T>> dataset) {
    try {
      dataset.as(pvalue.getName());
    } catch (IllegalStateException e) {
      // name not set, ignore
    }
    DatasetHolder<T> datasetHolder = new DatasetHolder<>(dataset);
    pcollections.put(pvalue, datasetHolder);
    leafs.add(datasetHolder);
  }

  Dataset<?> getInputDataset(PTransform<? extends PInput, ?> transform) {
    return getDataset((PValue) getInput(transform));
  }


  <T> Iterable<? extends WindowedValue<?>> getPCollectionView(PCollectionView<T> view) {
    return pview.get(view);
  }

  /**
   * Computes the outputs for all Datasets that are leaves in the DAG and do not have any
   * actions (like saving to a file) registered on them (i.e. they are performed for side
   * effects).
   */
  public void computeOutputs() {
    for (DatasetHolder<?> datasetHolder : leafs) {
      Dataset<?> dataset = datasetHolder.getDataset();
      dataset.cache(); // cache so that any subsequent get() is cheap
      dataset.count(); // force the Dataset to be computed
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
      Dataset<?> dataset = pcollections.get(value).getDataset();
      @SuppressWarnings("unchecked")
      T res = (T) Iterables.getOnlyElement(dataset.collectAsList());
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
    DatasetHolder<T> datasetHolder = (DatasetHolder<T>) pcollections.get(pcollection);
    Iterable<WindowedValue<T>> windowedValues = datasetHolder.getValues(pcollection);
    return Iterables.transform(windowedValues, WindowingHelpers.<T>unwindowValueFunction());
  }

  <T> Iterable<WindowedValue<T>> getWindowedValues(PCollection<T> pcollection) {
    @SuppressWarnings("unchecked")
    DatasetHolder<T> datasetHolder = (DatasetHolder<T>) pcollections.get(pcollection);
    return datasetHolder.getValues(pcollection);
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
