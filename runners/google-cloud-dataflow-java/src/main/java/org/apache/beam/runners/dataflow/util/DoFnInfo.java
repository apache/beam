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
package org.apache.beam.runners.dataflow.util;

import static com.google.common.base.Preconditions.checkState;

import java.io.Serializable;
import java.util.Map;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.OldDoFn;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;

/**
 * Wrapper class holding the necessary information to serialize a {@link OldDoFn}
 * or {@link DoFn}.
 *
 * @param <InputT> the type of the (main) input elements of the {@link OldDoFn}
 * @param <OutputT> the type of the (main) output elements of the {@link OldDoFn}
 */
public class DoFnInfo<InputT, OutputT> implements Serializable {
  private final Serializable doFn;
  private final WindowingStrategy<?, ?> windowingStrategy;
  private final Iterable<PCollectionView<?>> sideInputViews;
  private final Coder<InputT> inputCoder;
  private final long mainOutput;
  private final Map<Long, TupleTag<?>> outputMap;

  /**
   * Creates a {@link DoFnInfo} for the given {@link Serializable} object, which is expected to be a
   * {@link DoFn} or {@link OldDoFn} or other context-appropriate UDF blob.
   */
  public static <InputT, OutputT> DoFnInfo<InputT, OutputT> forFn(
      Serializable doFn,
      WindowingStrategy<?, ?> windowingStrategy,
      Iterable<PCollectionView<?>> sideInputViews,
      Coder<InputT> inputCoder,
      long mainOutput,
      Map<Long, TupleTag<?>> outputMap) {
    return new DoFnInfo(doFn, windowingStrategy, sideInputViews, inputCoder, mainOutput, outputMap);
  }

  private DoFnInfo(
      Serializable doFn,
      WindowingStrategy<?, ?> windowingStrategy,
      Iterable<PCollectionView<?>> sideInputViews,
      Coder<InputT> inputCoder,
      long mainOutput,
      Map<Long, TupleTag<?>> outputMap) {
    this.doFn = doFn;
    this.windowingStrategy = windowingStrategy;
    this.sideInputViews = sideInputViews;
    this.inputCoder = inputCoder;
    this.mainOutput = mainOutput;
    this.outputMap = outputMap;
  }

  /**
   * @deprecated use {@link #forFn}.
   */
  @Deprecated
  public DoFnInfo(
      OldDoFn doFn,
      WindowingStrategy<?, ?> windowingStrategy,
      Iterable<PCollectionView<?>> sideInputViews,
      Coder<InputT> inputCoder,
      long mainOutput,
      Map<Long, TupleTag<?>> outputMap) {
    this((Serializable) doFn, windowingStrategy, sideInputViews, inputCoder, mainOutput, outputMap);
  }

  /** Returns the embedded serialized function. It may be a {@code DoFn} or {@code OldDoFn}. */
  public Serializable getFn() {
    return doFn;
  }

  /** @deprecated use {@link #getFn()} */
  @Deprecated
  public OldDoFn getDoFn() {
    checkState(
        doFn instanceof OldDoFn,
        "Deprecated %s.getDoFn() called when the payload was actually a new %s",
        DoFnInfo.class.getSimpleName(),
        DoFn.class.getSimpleName());
    return (OldDoFn) doFn;
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

  public long getMainOutput() {
    return mainOutput;
  }

  public Map<Long, TupleTag<?>> getOutputMap() {
    return outputMap;
  }
}
