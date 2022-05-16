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
package org.apache.beam.runners.spark;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.api.java.JavaStreamingListener;

/**
 * A custom {@link PipelineOptions} to work with properties related to {@link JavaSparkContext}.
 *
 * <p>This can only be used programmatically (as opposed to passing command line arguments), since
 * the properties here are context-aware and should not be propagated to workers.
 *
 * <p>Separating this from {@link SparkPipelineOptions} is needed so the context-aware properties,
 * which link to Spark dependencies, won't be scanned by {@link PipelineOptions} reflective
 * instantiation. Note that {@link SparkContextOptions} is not registered with {@link
 * SparkRunnerRegistrar}.
 *
 * <p>Note: It's recommended to use {@link
 * org.apache.beam.runners.spark.translation.SparkContextFactory#setProvidedSparkContext(JavaSparkContext)}
 * instead of {@link SparkContextOptions#setProvidedSparkContext(JavaSparkContext)} for testing.
 * When using @{@link org.apache.beam.sdk.testing.TestPipeline} any provided {@link
 * JavaSparkContext} via {@link SparkContextOptions} is dropped.
 */
public interface SparkContextOptions extends SparkPipelineOptions {

  @Description("Provided Java Spark Context")
  @JsonIgnore
  JavaSparkContext getProvidedSparkContext();

  void setProvidedSparkContext(JavaSparkContext jsc);

  @Description("Spark streaming listeners")
  @Default.InstanceFactory(EmptyListenersList.class)
  @JsonIgnore
  List<JavaStreamingListener> getListeners();

  void setListeners(List<JavaStreamingListener> listeners);

  /** Returns an empty list, to avoid handling null. */
  class EmptyListenersList implements DefaultValueFactory<List<JavaStreamingListener>> {
    @Override
    public List<JavaStreamingListener> create(PipelineOptions options) {
      return new ArrayList<>();
    }
  }
}
