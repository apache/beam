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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.core.construction.TransformInputs;
import org.apache.beam.runners.spark.structuredstreaming.SparkStructuredStreamingPipelineOptions;
import org.apache.beam.runners.spark.structuredstreaming.translation.helpers.EncoderHelpers;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQueryException;
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

  @SuppressFBWarnings("URF_UNREAD_FIELD") // make spotbugs happy
  private AppliedPTransform<?, ?, ?> currentTransform;

  @SuppressFBWarnings("URF_UNREAD_FIELD") // make spotbugs happy
  private final SparkSession sparkSession;

  private final Map<PCollectionView<?>, Dataset<?>> broadcastDataSets;

  public TranslationContext(SparkStructuredStreamingPipelineOptions options) {
    SparkConf sparkConf = new SparkConf();
    sparkConf.setMaster(options.getSparkMaster());
    sparkConf.setAppName(options.getAppName());
    if (options.getFilesToStage() != null && !options.getFilesToStage().isEmpty()) {
      sparkConf.setJars(options.getFilesToStage().toArray(new String[0]));
    }

    // By default, Spark defines 200 as a number of sql partitions. This seems too much for local
    // mode, so try to align with value of "sparkMaster" option in this case.
    // We should not overwrite this value (or any user-defined spark configuration value) if the
    // user has already configured it.
    String sparkMaster = options.getSparkMaster();
    if (sparkMaster != null
        && sparkMaster.startsWith("local[")
        && System.getProperty("spark.sql.shuffle.partitions") == null) {
      int numPartitions =
          Integer.parseInt(sparkMaster.substring("local[".length(), sparkMaster.length() - 1));
      if (numPartitions > 0) {
        sparkConf.set("spark.sql.shuffle.partitions", String.valueOf(numPartitions));
      }
    }

    this.sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
    this.serializablePipelineOptions = new SerializablePipelineOptions(options);
    this.datasets = new HashMap<>();
    this.leaves = new HashSet<>();
    this.broadcastDataSets = new HashMap<>();
  }

  public SparkSession getSparkSession() {
    return sparkSession;
  }

  public SerializablePipelineOptions getSerializableOptions() {
    return serializablePipelineOptions;
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
    return (Dataset<T>) sparkSession.emptyDataset(EncoderHelpers.fromBeamCoder(VoidCoder.of()));
  }

  @SuppressWarnings("unchecked")
  public <T> Dataset<WindowedValue<T>> getDataset(PValue value) {
    Dataset<?> dataset = datasets.get(value);
    // assume that the Dataset is used as an input if retrieved here. So it is not a leaf anymore
    leaves.remove(dataset);
    return (Dataset<WindowedValue<T>>) dataset;
  }

  /**
   * TODO: All these 3 methods (putDataset*) are temporary and they are used only for generics type
   * checking. We should unify them in the future.
   */
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

  public <ViewT, ElemT> void setSideInputDataset(
      PCollectionView<ViewT> value, Dataset<WindowedValue<ElemT>> set) {
    if (!broadcastDataSets.containsKey(value)) {
      broadcastDataSets.put(value, set);
    }
  }

  @SuppressWarnings("unchecked")
  public <T> Dataset<T> getSideInputDataSet(PCollectionView<?> value) {
    return (Dataset<T>) broadcastDataSets.get(value);
  }

  // --------------------------------------------------------------------------------------------
  //  PCollections methods
  // --------------------------------------------------------------------------------------------
  public PValue getInput() {
    return Iterables.getOnlyElement(TransformInputs.nonAdditionalInputs(currentTransform));
  }

  public Map<TupleTag<?>, PValue> getInputs() {
    return currentTransform.getInputs();
  }

  public PValue getOutput() {
    return Iterables.getOnlyElement(currentTransform.getOutputs().values());
  }

  public Map<TupleTag<?>, PValue> getOutputs() {
    return currentTransform.getOutputs();
  }

  @SuppressWarnings("unchecked")
  public Map<TupleTag<?>, Coder<?>> getOutputCoders() {
    return currentTransform.getOutputs().entrySet().stream()
        .filter(e -> e.getValue() instanceof PCollection)
        .collect(Collectors.toMap(Map.Entry::getKey, e -> ((PCollection) e.getValue()).getCoder()));
  }

  // --------------------------------------------------------------------------------------------
  //  Pipeline methods
  // --------------------------------------------------------------------------------------------

  /** Starts the pipeline. */
  public void startPipeline() {
    try {
      SparkStructuredStreamingPipelineOptions options =
          serializablePipelineOptions.get().as(SparkStructuredStreamingPipelineOptions.class);
      int datasetIndex = 0;
      for (Dataset<?> dataset : leaves) {
        if (options.isStreaming()) {
          // TODO: deal with Beam Discarding, Accumulating and Accumulating & Retracting	outputmodes
          // with DatastreamWriter.outputMode
          DataStreamWriter<?> dataStreamWriter = dataset.writeStream();
          // spark sets a default checkpoint dir if not set.
          if (options.getCheckpointDir() != null) {
            dataStreamWriter =
                dataStreamWriter.option("checkpointLocation", options.getCheckpointDir());
          }
          // TODO: Do not await termination here.
          dataStreamWriter.foreach(new NoOpForeachWriter<>()).start().awaitTermination();
        } else {
          if (options.getTestMode()) {
            LOG.debug("**** dataset {} catalyst execution plans ****", ++datasetIndex);
            dataset.explain(true);
          }
          // apply a dummy fn just to apply foreach action that will trigger the pipeline run in
          // spark
          dataset.foreach((ForeachFunction) t -> {});
        }
      }
    } catch (StreamingQueryException e) {
      throw new RuntimeException("Pipeline execution failed: " + e);
    }
  }

  public static void printDatasetContent(Dataset<WindowedValue> dataset) {
    // cannot use dataset.show because dataset schema is binary so it will print binary
    // code.
    List<WindowedValue> windowedValues = dataset.collectAsList();
    for (WindowedValue windowedValue : windowedValues) {
      LOG.debug("**** dataset content {} ****", windowedValue.toString());
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
