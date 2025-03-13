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

import java.util.Set;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Interface for portable Spark translators. This allows for a uniform invocation pattern for
 * pipeline translation between streaming and batch runners.
 */
public interface SparkPortablePipelineTranslator<T extends SparkTranslationContext> {

  Set<String> knownUrns();

  /** Translates the given pipeline. */
  void translate(RunnerApi.Pipeline pipeline, T context);

  T createTranslationContext(JavaSparkContext jsc, SparkPipelineOptions options, JobInfo jobInfo);
}
