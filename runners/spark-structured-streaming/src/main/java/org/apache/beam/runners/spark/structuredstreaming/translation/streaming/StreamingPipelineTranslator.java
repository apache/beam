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
package org.apache.beam.runners.spark.structuredstreaming.translation.streaming;

import org.apache.beam.runners.spark.structuredstreaming.SparkPipelineOptions;
import org.apache.beam.runners.spark.structuredstreaming.translation.PipelineTranslator;
import org.apache.beam.runners.spark.structuredstreaming.translation.TransformTranslator;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.TransformHierarchy;

/**
 * {@link PipelineTranslator} for executing a {@link Pipeline} in Spark in streaming mode. This
 * contains only the components specific to streaming: {@link StreamingTranslationContext}, registry
 * of batch {@link TransformTranslator} and registry lookup code.
 */
public class StreamingPipelineTranslator extends PipelineTranslator {

  public StreamingPipelineTranslator(SparkPipelineOptions options) {}

  @Override
  protected TransformTranslator<?> getTransformTranslator(TransformHierarchy.Node node) {
    return null;
  }
}
