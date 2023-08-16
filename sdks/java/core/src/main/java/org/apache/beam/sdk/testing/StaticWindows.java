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
package org.apache.beam.sdk.testing;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IncompatibleWindowException;
import org.apache.beam.sdk.transforms.windowing.NonMergingWindowFn;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.transforms.windowing.WindowMappingFn;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Supplier;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.joda.time.Duration;

/**
 * A {@link WindowFn} that assigns all elements to a static collection of {@link BoundedWindow
 * BoundedWindows}. Side inputs windowed into static windows only support main input windows in the
 * provided collection of windows.
 */
final class StaticWindows extends NonMergingWindowFn<Object, BoundedWindow> {
  private final Supplier<Collection<BoundedWindow>> windows;
  private final Coder<BoundedWindow> coder;

  private final boolean onlyExisting;

  private StaticWindows(
      Supplier<Collection<BoundedWindow>> windows,
      Coder<BoundedWindow> coder,
      boolean onlyExisting) {
    this.windows = windows;
    this.coder = coder;
    this.onlyExisting = onlyExisting;
  }

  public static <W extends BoundedWindow> StaticWindows of(Coder<W> coder, Iterable<W> windows) {
    checkArgument(!Iterables.isEmpty(windows), "Input windows to StaticWindows may not be empty");
    @SuppressWarnings("unchecked")
    StaticWindows windowFn =
        new StaticWindows(
            WindowSupplier.of((Coder<BoundedWindow>) coder, (Iterable<BoundedWindow>) windows),
            (Coder<BoundedWindow>) coder,
            false);
    return windowFn;
  }

  public static <W extends BoundedWindow> StaticWindows of(Coder<W> coder, W window) {
    return of(coder, Collections.singleton(window));
  }

  public StaticWindows intoOnlyExisting() {
    return new StaticWindows(windows, coder, true);
  }

  public Collection<BoundedWindow> getWindows() {
    return windows.get();
  }

  @Override
  public Collection<BoundedWindow> assignWindows(AssignContext c) throws Exception {
    if (onlyExisting) {
      checkArgument(
          windows.get().contains(c.window()),
          "Tried to assign windows to an element that is not already windowed into a provided "
              + "window when onlyExisting is set to true");
      return Collections.singleton(c.window());
    } else {
      return getWindows();
    }
  }

  @Override
  public boolean isCompatible(WindowFn<?, ?> other) {
    if (!(other instanceof StaticWindows)) {
      return false;
    }
    StaticWindows that = (StaticWindows) other;
    return Objects.equals(this.windows.get(), that.windows.get());
  }

  @Override
  public void verifyCompatibility(WindowFn<?, ?> other) throws IncompatibleWindowException {
    if (!this.isCompatible(other)) {
      throw new IncompatibleWindowException(
          other,
          String.format(
              "Only %s objects with the same window supplier are compatible.",
              StaticWindows.class.getSimpleName()));
    }
  }

  @Override
  public Coder<BoundedWindow> windowCoder() {
    return coder;
  }

  @Override
  public WindowMappingFn<BoundedWindow> getDefaultWindowMappingFn() {
    return new WindowMappingFn<BoundedWindow>(Duration.millis(Long.MAX_VALUE)) {
      @Override
      public BoundedWindow getSideInputWindow(BoundedWindow mainWindow) {
        checkArgument(
            windows.get().contains(mainWindow),
            "%s only supports side input windows for main input windows that it contains",
            StaticWindows.class.getSimpleName());
        return mainWindow;
      }
    };
  }

  @Override
  public boolean assignsToOneWindow() {
    return true;
  }
}
