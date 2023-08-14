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
package org.apache.beam.testinfra.pipelines.conversions;

import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

/** Convenience class for bundling {@link Row} conversion successes and failures. */
@Internal
public class RowConversionResult<SourceT, FailureT> implements POutput {

  private final Pipeline pipeline;

  private final TupleTag<Row> successTag;
  private final PCollection<Row> success;

  private final TupleTag<FailureT> failureTag;
  private final PCollection<FailureT> failure;

  RowConversionResult(
      Schema successRowSchema,
      TupleTag<Row> successTag,
      TupleTag<FailureT> failureTag,
      PCollectionTuple pct) {
    this.pipeline = pct.getPipeline();
    this.successTag = successTag;
    this.success = pct.get(successTag).setRowSchema(successRowSchema);
    this.failureTag = failureTag;
    this.failure = pct.get(failureTag);
  }

  public PCollection<Row> getSuccess() {
    return success;
  }

  public PCollection<FailureT> getFailure() {
    return failure;
  }

  @Override
  public Pipeline getPipeline() {
    return pipeline;
  }

  @Override
  public Map<TupleTag<?>, PValue> expand() {
    return ImmutableMap.of(
        successTag, success,
        failureTag, failure);
  }

  @Override
  public void finishSpecifyingOutput(
      String transformName, PInput input, PTransform<?, ?> transform) {}
}
