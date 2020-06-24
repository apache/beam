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
package org.apache.beam.fn.harness.state;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.ViewFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.WindowMappingFn;

/**
 * A specification for side inputs containing the access pattern, a value {@link Coder}, the window
 * {@link Coder}, the {@link ViewFn}, and the {@link WindowMappingFn}.
 *
 * @param <W>
 */
@AutoValue
public abstract class SideInputSpec<W extends BoundedWindow> {
  public static <W extends BoundedWindow> SideInputSpec create(
      String accessPattern,
      Coder<?> coder,
      Coder<W> windowCoder,
      ViewFn<?, ?> viewFn,
      WindowMappingFn<W> windowMappingFn) {
    return new AutoValue_SideInputSpec<>(
        accessPattern, coder, windowCoder, viewFn, windowMappingFn);
  }

  abstract String getAccessPattern();

  abstract Coder<?> getCoder();

  abstract Coder<W> getWindowCoder();

  abstract ViewFn<?, ?> getViewFn();

  abstract WindowMappingFn<W> getWindowMappingFn();
}
