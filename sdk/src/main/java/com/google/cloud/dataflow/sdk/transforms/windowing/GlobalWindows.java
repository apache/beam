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

package com.google.cloud.dataflow.sdk.transforms.windowing;

import com.google.cloud.dataflow.sdk.coders.Coder;

import java.util.Arrays;
import java.util.Collection;

/**
 * Default {@link WindowFn} where all data is in the same bucket.
 */
@SuppressWarnings("serial")
public class GlobalWindows
    extends NonMergingWindowFn<Object, GlobalWindow> {
  @Override
  public Collection<GlobalWindow> assignWindows(AssignContext c) {
    return Arrays.asList(GlobalWindow.INSTANCE);
  }

  @Override
  public boolean isCompatible(WindowFn o) {
    return o instanceof GlobalWindows;
  }

  @Override
  public Coder<GlobalWindow> windowCoder() {
    return GlobalWindow.Coder.INSTANCE;
  }
}
