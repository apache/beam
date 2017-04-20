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
package org.apache.beam.sdk.transforms.windowing;

import java.util.Collection;
import java.util.Collections;
import org.apache.beam.sdk.coders.Coder;
import org.joda.time.Instant;

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
  public WindowMappingFn<GlobalWindow> getDefaultWindowMappingFn() {
    return new GlobalWindowMappingFn();
  }

  static class GlobalWindowMappingFn extends WindowMappingFn<GlobalWindow> {
    @Override
    public GlobalWindow getSideInputWindow(BoundedWindow mainWindow) {
      return GlobalWindow.INSTANCE;
    }
  }

  @Override
  public Instant getOutputTime(Instant inputTimestamp, GlobalWindow window) {
    return inputTimestamp;
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof GlobalWindows;
  }

  @Override
  public String toString() {
    return getClass().getCanonicalName();
  }
}
