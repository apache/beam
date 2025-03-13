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
package org.apache.beam.io.requestresponse;

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
 * The {@link Result} of processing request {@link PCollection} into response {@link PCollection}.
 */
public class Result<ResponseT> implements POutput {

  /**
   * Instantiates a {@link Result}. Package private as the design goals of {@link Result} are to be
   * a convenience read-only wrapper around a {@link PCollectionTuple}.
   */
  static <ResponseT> Result<ResponseT> of(
      Coder<ResponseT> responseTCoder,
      TupleTag<ResponseT> responseTag,
      TupleTag<ApiIOError> failureTag,
      PCollectionTuple pct) {
    return new Result<>(responseTCoder, responseTag, pct, failureTag);
  }

  private final Pipeline pipeline;
  private final TupleTag<ResponseT> responseTag;
  private final TupleTag<ApiIOError> failureTag;
  private final PCollection<ResponseT> responses;
  private final PCollection<ApiIOError> failures;

  private Result(
      Coder<ResponseT> responseTCoder,
      TupleTag<ResponseT> responseTag,
      PCollectionTuple pct,
      TupleTag<ApiIOError> failureTag) {
    this.pipeline = pct.getPipeline();
    this.responseTag = responseTag;
    this.failureTag = failureTag;
    this.responses = pct.get(responseTag).setCoder(responseTCoder);
    this.failures = pct.get(this.failureTag);
  }

  public PCollection<ResponseT> getResponses() {
    return responses;
  }

  public PCollection<ApiIOError> getFailures() {
    return failures;
  }

  @Override
  public Pipeline getPipeline() {
    return this.pipeline;
  }

  @Override
  public Map<TupleTag<?>, PValue> expand() {
    return ImmutableMap.of(
        responseTag, responses,
        failureTag, failures);
  }

  @Override
  public void finishSpecifyingOutput(
      String transformName, PInput input, PTransform<?, ?> transform) {}
}
