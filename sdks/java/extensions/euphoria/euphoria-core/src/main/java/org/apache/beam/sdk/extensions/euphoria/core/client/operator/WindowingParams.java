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
package org.apache.beam.sdk.extensions.euphoria.core.client.operator;

import javax.annotation.Nullable;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.windowing.Windowing;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.windowing.WindowingDesc;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.WindowingStrategy;

class WindowingParams<W extends BoundedWindow>
implements WindowAware<Object, W>{

  WindowFn<Object, W> windowFn;
  //TODO do we always need to set following fields explicitly ? No default values ?
  Trigger trigger;
  WindowingStrategy.AccumulationMode accumulationMode;

  // left for backward compatibility
  Windowing euphoriaWindowing;

  @Nullable
  public WindowingDesc<Object, W> getWindowing() {
    if (windowFn == null || trigger == null || accumulationMode == null) {
      return null;
    }

    return new WindowingDesc<>(windowFn, trigger, accumulationMode);
  }

  @Override
  public Windowing getEuphoriaWindowing() {
    return euphoriaWindowing;
  }

}
