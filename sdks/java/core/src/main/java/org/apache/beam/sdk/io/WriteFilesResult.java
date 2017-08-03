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

package org.apache.beam.sdk.io;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;

/** The result of a {@link WriteFiles} transform. */
public class WriteFilesResult implements POutput {
  private final Pipeline pipeline;
  private final TupleTag<String> outputFilenamesTag;
  private final PCollection<String> outputFilenames;

  private WriteFilesResult(
      Pipeline pipeline, TupleTag<String> outputFilenamesTag, PCollection<String> outputFilenames) {
    this.pipeline = pipeline;
    this.outputFilenamesTag = outputFilenamesTag;
    this.outputFilenames = outputFilenames;
  }

  static WriteFilesResult in(
      Pipeline pipeline, TupleTag<String> outputFilenamesTag, PCollection<String> outputFilenames) {
    return new WriteFilesResult(pipeline, outputFilenamesTag, outputFilenames);
  }

  @Override
  public Map<TupleTag<?>, PValue> expand() {
    return ImmutableMap.<TupleTag<?>, PValue>of(outputFilenamesTag, outputFilenames);
  }

  @Override
  public Pipeline getPipeline() {
    return pipeline;
  }

  @Override
  public void finishSpecifyingOutput(
      String transformName, PInput input, PTransform<?, ?> transform) {}

  public PCollection<String> getOutputFilenames() {
    return outputFilenames;
  }
}
