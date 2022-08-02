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
package org.apache.beam.sdk.transforms;

import org.apache.beam.sdk.transforms.Contextful.Fn;
import org.apache.beam.sdk.transforms.WithFailures.ExceptionElement;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A {@code PTransform} that adds exception handling to {@link MapKeys} and {@link MapValues} using
 * {@link MapElements.MapWithFailures}.
 */
class SimpleMapWithFailures<InputT, OutputT, FailureT>
    extends PTransform<PCollection<InputT>, WithFailures.Result<PCollection<OutputT>, FailureT>> {

  private final transient TypeDescriptor<OutputT> outputType;
  private final Contextful<Fn<InputT, OutputT>> fn;
  private final transient TypeDescriptor<FailureT> failureType;
  private final @Nullable ProcessFunction<ExceptionElement<InputT>, FailureT> exceptionHandler;
  private final String transformName;

  SimpleMapWithFailures(
      String transformName,
      Contextful<Fn<InputT, OutputT>> fn,
      TypeDescriptor<OutputT> outputType,
      @Nullable ProcessFunction<ExceptionElement<InputT>, FailureT> exceptionHandler,
      TypeDescriptor<FailureT> failureType) {
    this.transformName = transformName;
    this.fn = fn;
    this.outputType = outputType;
    this.exceptionHandler = exceptionHandler;
    this.failureType = failureType;
  }

  @Override
  public WithFailures.Result<PCollection<OutputT>, FailureT> expand(PCollection<InputT> input) {
    if (exceptionHandler == null) {
      throw new NullPointerException(".exceptionsVia() is required");
    }
    return input.apply(
        transformName,
        MapElements.into(outputType)
            .via(fn)
            .exceptionsInto(failureType)
            .exceptionsVia(exceptionHandler));
  }

  /**
   * Returns a {@code PTransform} that catches exceptions raised while mapping elements, passing the
   * raised exception instance and the input element being processed through the given {@code
   * exceptionHandler} and emitting the result to a failure collection.
   */
  public SimpleMapWithFailures<InputT, OutputT, FailureT> exceptionsVia(
      ProcessFunction<ExceptionElement<InputT>, FailureT> exceptionHandler) {
    return new SimpleMapWithFailures<>(
        transformName, fn, outputType, exceptionHandler, failureType);
  }
}
