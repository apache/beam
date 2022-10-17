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
package org.apache.beam.runners.spark.structuredstreaming.translation;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.spark.structuredstreaming.SparkStructuredStreamingPipelineOptions;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PValue;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class that gives a context for {@link PTransform} translation: keeping track of the
 * datasets, the {@link SparkSession}, the current transform being translated.
 */
public class TranslationContext {

  private static final Logger LOG = LoggerFactory.getLogger(TranslationContext.class);

  /** All the datasets of the DAG. */
  private final Map<PValue, Dataset<?>> datasets;
  /** datasets that are not used as input to other datasets (leaves of the DAG). */
  private final Set<Dataset<?>> leaves;

  private final SerializablePipelineOptions serializablePipelineOptions;

  private final SparkSession sparkSession;

  private final Map<PCollectionView<?>, Dataset<?>> broadcastDataSets;

  private final Map<Coder<?>, ExpressionEncoder<?>> encoders;

  public TranslationContext(SparkStructuredStreamingPipelineOptions options) {
    this.sparkSession = SparkSessionFactory.getOrCreateSession(options);
    this.serializablePipelineOptions = new SerializablePipelineOptions(options);
    this.datasets = new HashMap<>();
    this.leaves = new HashSet<>();
    this.broadcastDataSets = new HashMap<>();
    this.encoders = new HashMap<>();
  }

  public SparkSession getSparkSession() {
    return sparkSession;
  }

  public SerializablePipelineOptions getSerializableOptions() {
    return serializablePipelineOptions;
  }

  public <T> Encoder<T> encoderOf(Coder<T> coder, Function<Coder<T>, Encoder<T>> loadFn) {
    return (Encoder<T>) encoders.computeIfAbsent(coder, (Function) loadFn);
  }

  @SuppressWarnings("unchecked") // can't be avoided
  public <T> Dataset<WindowedValue<T>> getDataset(PCollection<T> pCollection) {
    Dataset<?> dataset = Preconditions.checkStateNotNull(datasets.get(pCollection));
    // assume that the Dataset is used as an input if retrieved here. So it is not a leaf anymore
    leaves.remove(dataset);
    return (Dataset<WindowedValue<T>>) dataset;
  }

  public <T> void putDataset(PCollection<T> pCollection, Dataset<WindowedValue<T>> dataset) {
    if (!datasets.containsKey(pCollection)) {
      datasets.put(pCollection, dataset);
      leaves.add(dataset);
    }
  }

  public <ViewT, ElemT> void setSideInputDataset(
      PCollectionView<ViewT> value, Dataset<WindowedValue<ElemT>> set) {
    if (!broadcastDataSets.containsKey(value)) {
      broadcastDataSets.put(value, set);
    }
  }

  @SuppressWarnings("unchecked") // can't be avoided
  public <T> Dataset<T> getSideInputDataSet(PCollectionView<?> value) {
    return (Dataset<T>) Preconditions.checkStateNotNull(broadcastDataSets.get(value));
  }

  /**
   * Starts the batch pipeline, streaming is not supported.
   *
   * @see org.apache.beam.runners.spark.structuredstreaming.SparkStructuredStreamingRunner
   */
  public void startPipeline() {
    encoders.clear();

    SparkStructuredStreamingPipelineOptions options =
        serializablePipelineOptions.get().as(SparkStructuredStreamingPipelineOptions.class);
    int datasetIndex = 0;
    for (Dataset<?> dataset : leaves) {
      if (options.getTestMode()) {
        LOG.debug("**** dataset {} catalyst execution plans ****", ++datasetIndex);
        dataset.explain(true);
      }
      // force evaluation using a dummy foreach action
      dataset.foreach((ForeachFunction) t -> {});
    }
  }

  public static <T> void printDatasetContent(Dataset<WindowedValue<T>> dataset) {
    // cannot use dataset.show because dataset schema is binary so it will print binary
    // code.
    List<WindowedValue<T>> windowedValues = dataset.collectAsList();
    for (WindowedValue<?> windowedValue : windowedValues) {
      System.out.println(windowedValue);
    }
  }
}
