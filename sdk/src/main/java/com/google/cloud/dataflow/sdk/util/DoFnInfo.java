/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.util;

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn;

import java.io.Serializable;

/**
 * Wrapper class holding the necessary information to serialize a DoFn.
 *
 * @param <I> the type of the (main) input elements of the DoFn
 * @param <O> the type of the (main) output elements of the DoFn
 */
public class DoFnInfo<I, O> implements Serializable {
  private static final long serialVersionUID = 0;
  private DoFn<I, O> doFn;
  private WindowFn<?, ?> windowFn;

  public DoFnInfo(DoFn<I, O> doFn, WindowFn<?, ?> windowFn) {
    this.doFn = doFn;
    this.windowFn = windowFn;
  }

  public DoFn<I, O> getDoFn() {
    return doFn;
  }

  public WindowFn<?, ?> getWindowFn() {
    return windowFn;
  }
}
