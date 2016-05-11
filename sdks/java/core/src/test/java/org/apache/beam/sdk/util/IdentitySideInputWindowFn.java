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

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.NonMergingWindowFn;
import org.apache.beam.sdk.transforms.windowing.WindowFn;

import java.util.Collection;

/**
 * A {@link WindowFn} for use during tests that returns the input window for calls to
 * {@link #getSideInputWindow(BoundedWindow)}.
 */
public class IdentitySideInputWindowFn extends NonMergingWindowFn<Integer, BoundedWindow> {
  @Override
  public Collection<BoundedWindow> assignWindows(WindowFn<Integer, BoundedWindow>.AssignContext c)
      throws Exception {
    return (Collection<BoundedWindow>) c.windows();
  }

  @Override
  public boolean isCompatible(WindowFn<?, ?> other) {
    return true;
  }

  @Override
  public Coder<BoundedWindow> windowCoder() {
    // not used
    return (Coder) GlobalWindow.Coder.INSTANCE;
  }

  @Override
  public BoundedWindow getSideInputWindow(BoundedWindow window) {
    return window;
  }
}
