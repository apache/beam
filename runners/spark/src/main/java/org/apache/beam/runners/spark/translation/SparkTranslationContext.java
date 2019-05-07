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

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Translation context used to lazily store Spark data sets during portable pipeline translation and
 * compute them after translation.
 */
public class SparkTranslationContext {
  private final JavaSparkContext jsc;
  final JobInfo jobInfo;
  private final Map<String, Dataset> datasets = new LinkedHashMap<>();
  private final Set<Dataset> leaves = new LinkedHashSet<>();
  final SerializablePipelineOptions serializablePipelineOptions;

  public SparkTranslationContext(JavaSparkContext jsc, PipelineOptions options, JobInfo jobInfo) {
    this.jsc = jsc;
    this.serializablePipelineOptions = new SerializablePipelineOptions(options);
    this.jobInfo = jobInfo;
  }

  public JavaSparkContext getSparkContext() {
    return jsc;
  }

  /** Add output of transform to context. */
  public void pushDataset(String pCollectionId, Dataset dataset) {
    dataset.setName(pCollectionId);
    // TODO cache
    datasets.put(pCollectionId, dataset);
    leaves.add(dataset);
  }

  /** Retrieve the dataset for the pCollection id and remove it from the DAG's leaves. */
  public Dataset popDataset(String pCollectionId) {
    Dataset dataset = datasets.get(pCollectionId);
    leaves.remove(dataset);
    return dataset;
  }

  /** Compute the outputs for all RDDs that are leaves in the DAG. */
  public void computeOutputs() {
    for (Dataset dataset : leaves) {
      dataset.action(); // force computation.
    }
  }
}
