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
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import org.apache.beam.runners.spark.EvaluationResult;
import org.apache.beam.runners.spark.aggregators.AccumulatorSingleton;
import org.apache.beam.runners.spark.translation.streaming.UnboundedDataset;
import org.apache.beam.sdk.AggregatorRetrievalException;
import org.apache.beam.sdk.AggregatorValues;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.transforms.Aggregator;
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
import org.apache.spark.streaming.StreamingContextState;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.joda.time.Duration;


/**
 * Evaluation context allows us to define how pipeline instructions.
 */
public class EvaluationContext implements EvaluationResult {
  private final JavaSparkContext jsc;
  private JavaStreamingContext jssc;
  private final SparkRuntimeContext runtime;
  private final Pipeline pipeline;
  private final Map<PValue, Dataset> datasets = new LinkedHashMap<>();
  private final Map<PValue, Dataset> pcollections = new LinkedHashMap<>();
  private final Set<Dataset> leaves = new LinkedHashSet<>();
  private final Set<PValue> multiReads = new LinkedHashSet<>();
  private final Map<PValue, Object> pobjects = new LinkedHashMap<>();
  private final Map<PValue, Iterable<? extends WindowedValue<?>>> pview = new LinkedHashMap<>();
  private AppliedPTransform<?, ?, ?> currentTransform;
  private State state;

  public EvaluationContext(JavaSparkContext jsc, Pipeline pipeline) {
    this.jsc = jsc;
    this.pipeline = pipeline;
    this.runtime = new SparkRuntimeContext(pipeline, jsc);
    // A batch pipeline is blocking by nature
    this.state = State.DONE;
  }

  public EvaluationContext(JavaSparkContext jsc, Pipeline pipeline,
                           JavaStreamingContext jssc) {
    this(jsc, pipeline);
    this.jssc = jssc;
    this.state = State.RUNNING;
  }

  JavaSparkContext getSparkContext() {
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

  void putPView(PValue view, Iterable<? extends WindowedValue<?>> value) {
    pview.put(view, value);
  }

  public Dataset borrowDataset(PTransform<?, ?> transform) {
    return borrowDataset((PValue) getInput(transform));
  }

  public Dataset borrowDataset(PValue pvalue) {
    Dataset dataset = datasets.get(pvalue);
    leaves.remove(dataset);
    if (multiReads.contains(pvalue)) {
      // Ensure the RDD is marked as cached
      dataset.cache();
    } else {
      multiReads.add(pvalue);
    }
    return dataset;
  }

  <T> Iterable<? extends WindowedValue<?>> getPCollectionView(PCollectionView<T> view) {
    return pview.get(view);
  }

  /**
   * Computes the outputs for all RDDs that are leaves in the DAG and do not have any actions (like
   * saving to a file) registered on them (i.e. they are performed for side effects).
   */
  public void computeOutputs() {
    for (Dataset dataset : leaves) {
      dataset.cache(); // cache so that any subsequent get() is cheap.
      dataset.action(); // force computation.
    }
  }

  @SuppressWarnings("unchecked")
  @Override
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
  public MetricResults metrics() {
    throw new UnsupportedOperationException("The SparkRunner does not currently support metrics.");
  }

  @Override
  public <T> Iterable<T> get(PCollection<T> pcollection) {
    @SuppressWarnings("unchecked")
    BoundedDataset<T> boundedDataset = (BoundedDataset<T>) datasets.get(pcollection);
    Iterable<WindowedValue<T>> windowedValues = boundedDataset.getValues(pcollection);
    return Iterables.transform(windowedValues, WindowingHelpers.<T>unwindowValueFunction());
  }

  <T> Iterable<WindowedValue<T>> getWindowedValues(PCollection<T> pcollection) {
    @SuppressWarnings("unchecked")
    BoundedDataset<T> boundedDataset = (BoundedDataset<T>) datasets.get(pcollection);
    return boundedDataset.getValues(pcollection);
  }

  @Override
  public void close(boolean gracefully) {
    // Stopping streaming job if running
    if (isStreamingPipeline() && !state.isTerminal()) {
      try {
        cancel(gracefully);
      } catch (IOException e) {
        throw new RuntimeException("Failed to cancel streaming job", e);
      }
    }
    SparkContextFactory.stopSparkContext(jsc);
  }

  @Override
  public State getState() {
    return state;
  }

  @Override
  public State cancel() throws IOException {
    return cancel(true);
  }

  private State cancel(boolean gracefully) throws IOException {
    if (isStreamingPipeline()) {
      if (!state.isTerminal()) {
        jssc.stop(false, gracefully);
        state = State.CANCELLED;
      }
      return state;
    } else {
      // Batch is currently blocking so
      // there is no way to cancel a batch job
      // will be handled at BEAM-1000
      throw new UnsupportedOperationException(
          "Spark runner EvaluationContext does not support cancel.");
    }
  }

  @Override
  public State waitUntilFinish() {
    return waitUntilFinish(Duration.ZERO);
  }

  @Override
  public State waitUntilFinish(Duration duration) {
    if (isStreamingPipeline()) {
      // According to PipelineResult: Provide a value less than 1 ms for an infinite wait
      if (duration.getMillis() < 1L) {
        jssc.awaitTermination();
        state = State.DONE;
      } else {
        jssc.awaitTermination(duration.getMillis());
        // According to PipelineResult: The final state of the pipeline or null on timeout
        if (jssc.getState().equals(StreamingContextState.STOPPED)) {
          state = State.DONE;
        } else {
          return null;
        }
      }
      return state;
    } else {
      // This is no-op, since Spark runner in batch is blocking.
      // It needs to be updated once SparkRunner supports non-blocking execution:
      // https://issues.apache.org/jira/browse/BEAM-595
      return State.DONE;
    }
  }

  private boolean isStreamingPipeline() {
    return jssc != null;
  }
}
