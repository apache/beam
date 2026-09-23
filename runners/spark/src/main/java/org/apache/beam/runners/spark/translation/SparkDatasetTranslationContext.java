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

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.fnexecution.translation.PipelineTranslatorUtils;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.structuredstreaming.translation.EvaluationContext;
import org.apache.beam.runners.spark.structuredstreaming.translation.helpers.EncoderHelpers;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

/**
 * Translation context of the Dataset-based portable backend. Keeps one {@link Dataset} per
 * translated PCollection and evaluates the ones no transform consumed in {@link #computeOutputs()}.
 */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "unchecked",
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class SparkDatasetTranslationContext extends SparkTranslationContext {
  private final SparkSession session;
  private final StorageLevel storageLevel;
  private final boolean cacheDisabled;
  private final Map<String, Integer> consumers = new HashMap<>();
  private final Map<String, Dataset> datasets = new HashMap<>();
  private final Set<String> leaves = new LinkedHashSet<>();
  private final Map<Coder<?>, Encoder<?>> encoders = new HashMap<>();

  public SparkDatasetTranslationContext(
      JavaSparkContext jsc, SparkPipelineOptions options, JobInfo jobInfo) {
    super(jsc, options, jobInfo);
    // The builder attaches to the SparkContext that SparkContextFactory already created.
    this.session = SparkSession.builder().getOrCreate();
    this.storageLevel = StorageLevel.fromString(options.getStorageLevel());
    this.cacheDisabled = options.isCacheDisabled();
  }

  public SparkSession getSparkSession() {
    return session;
  }

  public StorageLevel getStorageLevel() {
    return storageLevel;
  }

  /** Records one more transform reading {@code pCollectionId}. */
  void addConsumer(String pCollectionId) {
    consumers.merge(pCollectionId, 1, Integer::sum);
  }

  /** Registers the Dataset of a PCollection. Datasets read by several transforms are persisted. */
  public <T> void putDataset(String pCollectionId, Dataset<WindowedValue<T>> dataset) {
    putDataset(pCollectionId, dataset, true);
  }

  /**
   * Registers the Dataset of a PCollection. Pass {@code cache} as false for a Dataset that is a
   * projection of an already persisted one, so the same rows are not cached twice.
   */
  public <T> void putDataset(
      String pCollectionId, Dataset<WindowedValue<T>> dataset, boolean cache) {
    if (cache && !cacheDisabled && consumers.getOrDefault(pCollectionId, 0) > 1) {
      dataset = dataset.persist(storageLevel);
    }
    datasets.put(pCollectionId, dataset);
    leaves.add(pCollectionId);
  }

  /** Returns the Dataset of a PCollection and marks it as consumed. */
  public <T> Dataset<WindowedValue<T>> getDataset(String pCollectionId) {
    leaves.remove(pCollectionId);
    return datasets.get(pCollectionId);
  }

  /** Encoder of the windowed values of a PCollection, derived from its wire coder. */
  public <T> Encoder<WindowedValue<T>> windowedEncoder(
      String pCollectionId, RunnerApi.Components components) {
    Coder<T> valueCoder =
        PipelineTranslatorUtils.<T>getWindowedValueCoder(pCollectionId, components).getValueCoder();
    Coder<? extends BoundedWindow> windowCoder =
        PipelineTranslatorUtils.getWindowingStrategy(pCollectionId, components)
            .getWindowFn()
            .windowCoder();
    return EncoderHelpers.windowedValueEncoder(encoderOf(valueCoder), encoderOf(windowCoder));
  }

  private <T> Encoder<T> encoderOf(Coder<T> coder) {
    return (Encoder<T>) encoders.computeIfAbsent(coder, c -> EncoderHelpers.encoderFor(coder));
  }

  /** Evaluates every Dataset no transform consumed. */
  @Override
  public void computeOutputs() {
    for (String leaf : leaves) {
      EvaluationContext.evaluate(leaf, datasets.get(leaf));
    }
  }
}
