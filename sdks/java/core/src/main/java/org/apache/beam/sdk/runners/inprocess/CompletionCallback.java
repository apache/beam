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
package org.apache.beam.sdk.runners.inprocess;

import org.apache.beam.sdk.runners.inprocess.InProcessPipelineRunner.CommittedBundle;
import org.apache.beam.sdk.transforms.AppliedPTransform;

import java.util.Collection;
import java.util.Map;

/**
 * A callback for completing a bundle of input.
 */
interface CompletionCallback {
  /**
   * Handle a successful result, returning the committed outputs of the result and the transforms
   * that should consume those outputs.
   */
  Map<? extends CommittedBundle<?>, Collection<AppliedPTransform<?, ?, ?>>> handleResult(
      CommittedBundle<?> inputBundle, InProcessTransformResult result);

  /**
   * Handle a result that terminated abnormally due to the provided {@link Throwable}.
   */
  void handleThrowable(CommittedBundle<?> inputBundle, Throwable t);
}
