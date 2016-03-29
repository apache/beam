/*
 * Copyright (C) 2015 Google Inc.
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

import org.joda.time.Instant;

import java.util.Collection;
import java.util.Collections;

/**
 * Default {@link WindowFn} that assigns all data to the same window.
 */
public class GlobalWindows extends NonMergingWindowFn<Object, GlobalWindow> {

  private static final Collection<GlobalWindow> GLOBAL_WINDOWS =
      Collections.singletonList(GlobalWindow.INSTANCE);

  @Override
  public Collection<GlobalWindow> assignWindows(AssignContext c) {
    return GLOBAL_WINDOWS;
  }

  @Override
  public boolean isCompatible(WindowFn<?, ?> o) {
    return o instanceof GlobalWindows;
  }

  @Override
  public Coder<GlobalWindow> windowCoder() {
    return GlobalWindow.Coder.INSTANCE;
  }

  @Override
  public GlobalWindow getSideInputWindow(BoundedWindow window) {
    return GlobalWindow.INSTANCE;
  }

  @Override
  public boolean assignsToSingleWindow() {
    return true;
  }

  @Override
  public Instant getOutputTime(Instant inputTimestamp, GlobalWindow window) {
    return inputTimestamp;
  }
}
