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
package org.apache.beam.sdk.io.csv;

import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

/**
 * The {@link T} and {@link CsvIOParseError} {@link PCollection} results of parsing CSV records. Use
 * {@link #getOutput()} and {@link #getErrors()} to apply these results in a pipeline.
 */
public class CsvIOParseResult<T> implements POutput {

  static <T> CsvIOParseResult<T> of(
      TupleTag<T> outputTag,
      Coder<T> outputCoder,
      TupleTag<CsvIOParseError> errorTag,
      PCollectionTuple pct) {
    return new CsvIOParseResult<>(outputTag, outputCoder, errorTag, pct);
  }

  static <T> CsvIOParseResult<T> empty(Pipeline pipeline, Coder<T> outputCoder) {
    return new CsvIOParseResult<>(
        new TupleTag<T>() {},
        outputCoder,
        new TupleTag<CsvIOParseError>() {},
        PCollectionTuple.empty(pipeline));
  }

  private final Pipeline pipeline;
  private final TupleTag<T> outputTag;
  private final PCollection<T> output;
  private final TupleTag<CsvIOParseError> errorTag;
  private final PCollection<CsvIOParseError> errors;

  private CsvIOParseResult(
      TupleTag<T> outputTag,
      Coder<T> outputCoder,
      TupleTag<CsvIOParseError> errorTag,
      PCollectionTuple pct) {
    this.outputTag = outputTag;
    this.errorTag = errorTag;
    this.pipeline = pct.getPipeline();
    this.output = pct.get(outputTag).setCoder(outputCoder);
    this.errors = pct.get(errorTag).setCoder(CsvIOParseError.CODER);
  }

  /** The {@link T} {@link PCollection} as a result of successfully parsing CSV records. */
  public PCollection<T> getOutput() {
    return output;
  }

  /**
   * The {@link CsvIOParseError} {@link PCollection} as a result of errors associated with parsing
   * CSV records.
   */
  public PCollection<CsvIOParseError> getErrors() {
    return errors;
  }

  @Override
  public Pipeline getPipeline() {
    return pipeline;
  }

  @Override
  public Map<TupleTag<?>, PValue> expand() {
    return ImmutableMap.of(
        outputTag, output,
        errorTag, errors);
  }

  @Override
  public void finishSpecifyingOutput(
      String transformName, PInput input, PTransform<?, ?> transform) {}
}
