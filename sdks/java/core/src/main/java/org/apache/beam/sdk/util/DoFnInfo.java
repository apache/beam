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
package org.apache.beam.sdk.util;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;

/**
 * Wrapper class holding the necessary information to serialize a {@link DoFn}.
 *
 * @param <InputT> the type of the (main) input elements of the {@link DoFn}
 * @param <OutputT> the type of the (main) output elements of the {@link DoFn}
 */
@Internal
public class DoFnInfo<InputT, OutputT> implements Serializable {
  private final DoFn<InputT, OutputT> doFn;
  private final WindowingStrategy<?, ?> windowingStrategy;
  private final Iterable<PCollectionView<?>> sideInputViews;
  private final Coder<InputT> inputCoder;
  Map<TupleTag<?>, Coder<?>> outputCoders;
  private final TupleTag<OutputT> mainOutput;
  private final DoFnSchemaInformation doFnSchemaInformation;
  private final Map<String, PCollectionView<?>> sideInputMapping;

  /**
   * Creates a {@link DoFnInfo} for the given {@link DoFn}.
   *
   * <p>This method exists for backwards compatibility with the Dataflow runner. Once the Dataflow
   * runner has been updated to use the new constructor, remove this one.
   */
  public static <InputT, OutputT> DoFnInfo<InputT, OutputT> forFn(
      DoFn<InputT, OutputT> doFn,
      WindowingStrategy<?, ?> windowingStrategy,
      Iterable<PCollectionView<?>> sideInputViews,
      Coder<InputT> inputCoder,
      TupleTag<OutputT> mainOutput,
      DoFnSchemaInformation doFnSchemaInformation,
      Map<String, PCollectionView<?>> sideInputMapping) {
    return new DoFnInfo<>(
        doFn,
        windowingStrategy,
        sideInputViews,
        inputCoder,
        Collections.emptyMap(),
        mainOutput,
        doFnSchemaInformation,
        sideInputMapping);
  }

  /** Creates a {@link DoFnInfo} for the given {@link DoFn}. */
  public static <InputT, OutputT> DoFnInfo<InputT, OutputT> forFn(
      DoFn<InputT, OutputT> doFn,
      WindowingStrategy<?, ?> windowingStrategy,
      Iterable<PCollectionView<?>> sideInputViews,
      Coder<InputT> inputCoder,
      Map<TupleTag<?>, Coder<?>> outputCoders,
      TupleTag<OutputT> mainOutput,
      DoFnSchemaInformation doFnSchemaInformation,
      Map<String, PCollectionView<?>> sideInputMapping) {
    return new DoFnInfo<>(
        doFn,
        windowingStrategy,
        sideInputViews,
        inputCoder,
        outputCoders,
        mainOutput,
        doFnSchemaInformation,
        sideInputMapping);
  }

  public DoFnInfo<InputT, OutputT> withFn(DoFn<InputT, OutputT> newFn) {
    return DoFnInfo.forFn(
        newFn,
        windowingStrategy,
        sideInputViews,
        inputCoder,
        outputCoders,
        mainOutput,
        doFnSchemaInformation,
        sideInputMapping);
  }

  private DoFnInfo(
      DoFn<InputT, OutputT> doFn,
      WindowingStrategy<?, ?> windowingStrategy,
      Iterable<PCollectionView<?>> sideInputViews,
      Coder<InputT> inputCoder,
      Map<TupleTag<?>, Coder<?>> outputCoders,
      TupleTag<OutputT> mainOutput,
      DoFnSchemaInformation doFnSchemaInformation,
      Map<String, PCollectionView<?>> sideInputMapping) {
    this.doFn = doFn;
    this.windowingStrategy = windowingStrategy;
    this.sideInputViews = sideInputViews;
    this.inputCoder = inputCoder;
    this.outputCoders = outputCoders;
    this.mainOutput = mainOutput;
    this.doFnSchemaInformation = doFnSchemaInformation;
    this.sideInputMapping = sideInputMapping;
  }

  /** Returns the embedded function. */
  public DoFn<InputT, OutputT> getDoFn() {
    return doFn;
  }

  public WindowingStrategy<?, ?> getWindowingStrategy() {
    return windowingStrategy;
  }

  public Iterable<PCollectionView<?>> getSideInputViews() {
    return sideInputViews;
  }

  public Coder<InputT> getInputCoder() {
    return inputCoder;
  }

  public Map<TupleTag<?>, Coder<?>> getOutputCoders() {
    return outputCoders;
  }

  public TupleTag<OutputT> getMainOutput() {
    return mainOutput;
  }

  public DoFnSchemaInformation getDoFnSchemaInformation() {
    return doFnSchemaInformation;
  }

  public Map<String, PCollectionView<?>> getSideInputMapping() {
    return sideInputMapping;
  }
}
