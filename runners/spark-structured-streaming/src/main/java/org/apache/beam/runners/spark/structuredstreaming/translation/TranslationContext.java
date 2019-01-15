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

import com.google.common.collect.Iterables;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.runners.core.construction.TransformInputs;
import org.apache.beam.runners.spark.structuredstreaming.SparkPipelineOptions;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;

/**
 * Base class that gives a context for {@link PTransform} translation: keeping track of the
 * datasets, the {@link SparkSession}, the current transform being translated.
 */
public class TranslationContext {

  /** All the datasets of the DAG. */
  private final Map<PValue, Dataset<?>> datasets;
  /** datasets that are not used as input to other datasets (leaves of the DAG). */
  private final Set<Dataset<?>> leaves;

  private final SparkPipelineOptions options;

  @SuppressFBWarnings("URF_UNREAD_FIELD") // make findbug happy
  private AppliedPTransform<?, ?, ?> currentTransform;

  @SuppressFBWarnings("URF_UNREAD_FIELD") // make findbug happy
  private SparkSession sparkSession;

  public TranslationContext(SparkPipelineOptions options) {
    SparkConf sparkConf = new SparkConf();
    sparkConf.setMaster(options.getSparkMaster());
    sparkConf.setAppName(options.getAppName());
    if (options.getFilesToStage() != null && !options.getFilesToStage().isEmpty()) {
      sparkConf.setJars(options.getFilesToStage().toArray(new String[0]));
    }

    this.sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
    this.options = options;
    this.datasets = new HashMap<>();
    this.leaves = new HashSet<>();
  }

  public SparkSession getSparkSession() {
    return sparkSession;
  }

  public SparkPipelineOptions getOptions() {
    return options;
  }

  // --------------------------------------------------------------------------------------------
  //  Transforms methods
  // --------------------------------------------------------------------------------------------
  public void setCurrentTransform(AppliedPTransform<?, ?, ?> currentTransform) {
    this.currentTransform = currentTransform;
  }

  public AppliedPTransform<?, ?, ?> getCurrentTransform() {
    return currentTransform;
  }

  // --------------------------------------------------------------------------------------------
  //  Datasets methods
  // --------------------------------------------------------------------------------------------
  @SuppressWarnings("unchecked")
  public <T> Dataset<T> emptyDataset() {
    return (Dataset<T>) sparkSession.emptyDataset(Encoders.bean(Void.class));
  }

  @SuppressWarnings("unchecked")
  public <T> Dataset<WindowedValue<T>> getDataset(PValue value) {
    Dataset<?> dataset = datasets.get(value);
    // assume that the Dataset is used as an input if retrieved here. So it is not a leaf anymore
    leaves.remove(dataset);
    return (Dataset<WindowedValue<T>>) dataset;
  }

  public void putDatasetWildcard(PValue value, Dataset<WindowedValue<?>> dataset) {
    if (!datasets.containsKey(value)) {
      datasets.put(value, dataset);
      leaves.add(dataset);
    }
  }

  public <T> void putDataset(PValue value, Dataset<WindowedValue<T>> dataset) {
    if (!datasets.containsKey(value)) {
      datasets.put(value, dataset);
      leaves.add(dataset);
    }
  }

    public void putDatasetRaw(PValue value, Dataset<WindowedValue> dataset) {
      if (!datasets.containsKey(value)) {
        datasets.put(value, dataset);
        leaves.add(dataset);
      }
    }

  // --------------------------------------------------------------------------------------------
  //  PCollections methods
  // --------------------------------------------------------------------------------------------
  @SuppressWarnings("unchecked")
  public PValue getInput() {
    return Iterables.getOnlyElement(TransformInputs.nonAdditionalInputs(currentTransform));
  }

  @SuppressWarnings("unchecked")
  public <T extends PValue> T getInput(PTransform<T, ?> transform) {
    return (T) Iterables.getOnlyElement(TransformInputs.nonAdditionalInputs(currentTransform));
  }

  @SuppressWarnings("unchecked")
  public Map<TupleTag<?>, PValue> getInputs() {
    return currentTransform.getInputs();
  }

  @SuppressWarnings("unchecked")
  public PValue getOutput() {
    return Iterables.getOnlyElement(currentTransform.getOutputs().values());
  }

  @SuppressWarnings("unchecked")
  public Map<TupleTag<?>, PValue> getOutputs() {
    return currentTransform.getOutputs();
  }

  @SuppressWarnings("unchecked")
  public Map<TupleTag<?>, Coder<?>> getOutputCoders() {
    return currentTransform
        .getOutputs()
        .entrySet()
        .stream()
        .filter(e -> e.getValue() instanceof PCollection)
        .collect(Collectors.toMap(e -> e.getKey(), e -> ((PCollection) e.getValue()).getCoder()));
  }

  // --------------------------------------------------------------------------------------------
  //  Pipeline methods
  // --------------------------------------------------------------------------------------------

  public void startPipeline(boolean testMode) {
    try {
      // to start a pipeline we need a DatastreamWriter to start
      for (Dataset<?> dataset : leaves) {

        if (options.isStreaming()) {
          dataset.writeStream().foreach(new NoOpForeachWriter<>()).start().awaitTermination();
        } else {
          if (testMode){
            // cannot use dataset.show because dataset schema is binary so it will print binary code.
            List<WindowedValue> windowedValues = ((Dataset<WindowedValue>)dataset).collectAsList();
            for (WindowedValue windowedValue : windowedValues){
              System.out.println(windowedValue);
            }
          } else {
            // apply a dummy fn just to apply forech action that will trigger the pipeline run in spark
            dataset.foreachPartition(t -> {
            });
          }
        }
      }
    } catch (StreamingQueryException e) {
      throw new RuntimeException("Pipeline execution failed: " + e);
    }
  }

  private static class NoOpForeachWriter<T> extends ForeachWriter<T> {

    @Override
    public boolean open(long partitionId, long epochId) {
      return false;
    }

    @Override
    public void process(T value) {
      // do nothing
    }

    @Override
    public void close(Throwable errorOrNull) {
      // do nothing
    }
  }
}
