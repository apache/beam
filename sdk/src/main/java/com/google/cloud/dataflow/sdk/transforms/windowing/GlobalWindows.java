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

import com.google.cloud.dataflow.sdk.coders.AtomicCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;

import org.joda.time.Instant;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collection;

/**
 * Default {@link WindowFn} where all data is in the same bucket.
 */
@SuppressWarnings("serial")
public class GlobalWindows
    extends NonMergingWindowFn<Object, GlobalWindows.GlobalWindow> {
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

  /**
   * The default window into which all data is placed.
   */
  public static class GlobalWindow extends BoundedWindow {
    public static final GlobalWindow INSTANCE = new GlobalWindow();

    @Override
    public Instant maxTimestamp() {
      return new Instant(Long.MAX_VALUE);
    }

    private GlobalWindow() {}

    /**
     * {@link Coder} for encoding and decoding {@code Window}s.
     */
    public static class Coder extends AtomicCoder<GlobalWindow> {
      public static final Coder INSTANCE = new Coder();

      @Override
      public void encode(GlobalWindow window, OutputStream outStream, Context context) {}

      @Override
      public GlobalWindow decode(InputStream inStream, Context context) {
        return GlobalWindow.INSTANCE;
      }

      @Override
      public boolean isDeterministic() {
        return true;
      }

      private Coder() {}
    }
  }
}
