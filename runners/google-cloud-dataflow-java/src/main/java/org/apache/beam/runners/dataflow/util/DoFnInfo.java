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

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.OldDoFn;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.PCollectionView;

import java.io.Serializable;

/**
 * Wrapper class holding the necessary information to serialize a OldDoFn.
 *
 * @param <InputT> the type of the (main) input elements of the OldDoFn
 * @param <OutputT> the type of the (main) output elements of the OldDoFn
 */
public class DoFnInfo<InputT, OutputT> implements Serializable {
  private final OldDoFn<InputT, OutputT> doFn;
  private final WindowingStrategy<?, ?> windowingStrategy;
  private final Iterable<PCollectionView<?>> sideInputViews;
  private final Coder<InputT> inputCoder;

  public DoFnInfo(OldDoFn<InputT, OutputT> doFn, WindowingStrategy<?, ?> windowingStrategy) {
    this.doFn = doFn;
    this.windowingStrategy = windowingStrategy;
    this.sideInputViews = null;
    this.inputCoder = null;
  }

  public DoFnInfo(OldDoFn<InputT, OutputT> doFn, WindowingStrategy<?, ?> windowingStrategy,
                  Iterable<PCollectionView<?>> sideInputViews, Coder<InputT> inputCoder) {
    this.doFn = doFn;
    this.windowingStrategy = windowingStrategy;
    this.sideInputViews = sideInputViews;
    this.inputCoder = inputCoder;
  }

  public OldDoFn<InputT, OutputT> getDoFn() {
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
}
