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

package org.apache.beam.runners.spark.translation;

import javax.annotation.Nonnull;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.spark.api.java.function.Function;

/**
 * Helper functions for working with windows.
 */
public final class WindowingHelpers {
  private WindowingHelpers() {
  }

  /**
   * A Spark function for converting a value to a {@link WindowedValue}. The resulting
   * {@link WindowedValue} will be in a global windows, and will have the default timestamp == MIN
   * and pane.
   *
   * @param <T>   The type of the object.
   * @return A function that accepts an object and returns its {@link WindowedValue}.
   */
  public static <T> Function<T, WindowedValue<T>> windowFunction() {
    return new Function<T, WindowedValue<T>>() {
      @Override
      public WindowedValue<T> call(T t) {
        return WindowedValue.valueInGlobalWindow(t);
      }
    };
  }

  /**
   * A Spark function for extracting the value from a {@link WindowedValue}.
   *
   * @param <T>   The type of the object.
   * @return A function that accepts a {@link WindowedValue} and returns its value.
   */
  public static <T> Function<WindowedValue<T>, T> unwindowFunction() {
    return new Function<WindowedValue<T>, T>() {
      @Override
      public T call(WindowedValue<T> t) {
        return t.getValue();
      }
    };
  }

  /**
   * Same as windowFunction but for non-RDD values - not an RDD transformation!
   *
   * @param <T>   The type of the object.
   * @return A function that accepts an object and returns its {@link WindowedValue}.
   */
  public static <T> com.google.common.base.Function<T, WindowedValue<T>> windowValueFunction() {
    return new com.google.common.base.Function<T, WindowedValue<T>>() {
      @Override
      public WindowedValue<T> apply(T t) {
        return WindowedValue.valueInGlobalWindow(t);
      }
    };
  }

  /**
   * Same as unwindowFunction but for non-RDD values - not an RDD transformation!
   *
   * @param <T>   The type of the object.
   * @return A function that accepts an object and returns its {@link WindowedValue}.
   */
  public static <T> com.google.common.base.Function<WindowedValue<T>, T> unwindowValueFunction() {
    return new com.google.common.base.Function<WindowedValue<T>, T>() {
      @Override
      public T apply(@Nonnull WindowedValue<T> t) {
        return t.getValue();
      }
    };
  }
}
