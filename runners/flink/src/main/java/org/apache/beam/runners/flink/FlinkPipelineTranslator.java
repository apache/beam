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
package org.apache.beam.runners.flink;

import org.apache.beam.sdk.Pipeline;

/**
 * The role of this class is to translate the Beam operators to their Flink counterparts. If we have
 * a streaming job, this is instantiated as a {@link FlinkStreamingPipelineTranslator}. In other
 * case, i.e. for a batch job, a {@link FlinkBatchPipelineTranslator} is created. Correspondingly,
 * the {@link org.apache.beam.sdk.values.PCollection}-based user-provided job is translated into a
 * {@link org.apache.flink.streaming.api.datastream.DataStream} (for streaming) or a {@link
 * org.apache.flink.api.java.DataSet} (for batch) one.
 */
abstract class FlinkPipelineTranslator extends Pipeline.PipelineVisitor.Defaults {

  /**
   * Translates the pipeline by passing this class as a visitor.
   *
   * @param pipeline The pipeline to be translated
   */
  public void translate(Pipeline pipeline) {
    pipeline.traverseTopologically(this);
  }

  /**
   * Utility formatting method.
   *
   * @param n number of spaces to generate
   * @return String with "|" followed by n spaces
   */
  protected static String genSpaces(int n) {
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < n; i++) {
      builder.append("|   ");
    }
    return builder.toString();
  }
}
