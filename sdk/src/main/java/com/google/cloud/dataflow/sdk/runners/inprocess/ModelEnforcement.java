/*
 * Copyright (C) 2016 Google Inc.
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
package com.google.cloud.dataflow.sdk.runners.inprocess;

import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.CommittedBundle;
import com.google.cloud.dataflow.sdk.util.WindowedValue;

/**
 * Enforcement tools that verify that executing code conforms to the model.
 */
public interface ModelEnforcement<T> {
  /**
   * Called before an element is processed. Returns the element that should be used by the
   * {@link TransformEvaluator}.
   */
  void beforeElement(WindowedValue<T> element);

  /**
   * Called after an element is processed.
   */
  void afterElement(WindowedValue<T> element);

  /**
   * Called after a bundle has been completed and finishBundle has been called. Provides the
   * committed outputs.
   */
  void afterFinish(
      CommittedBundle<T> input,
      InProcessTransformResult result,
      Iterable<? extends CommittedBundle<?>> outputs);
}
