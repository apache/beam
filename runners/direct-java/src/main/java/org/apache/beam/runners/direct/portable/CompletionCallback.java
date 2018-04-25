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
package org.apache.beam.runners.direct.portable;

import org.apache.beam.sdk.runners.AppliedPTransform;

/**
 * A callback for completing a bundle of input.
 */
interface CompletionCallback {
  /**
   * Handle a successful result, returning the committed outputs of the result.
   */
  CommittedResult handleResult(
      CommittedBundle<?> inputBundle, TransformResult<?> result);

  /**
   * Handle an input bundle that did not require processing.
   *
   * <p>This occurs when a Source has no splits that can currently produce outputs.
   */
  void handleEmpty(AppliedPTransform<?, ?, ?> transform);

  /**
   * Handle a result that terminated abnormally due to the provided {@link Exception}.
   */
  void handleException(CommittedBundle<?> inputBundle, Exception t);

  /**
   * Handle a result that terminated abnormally due to the provided {@link Error}. The pipeline
   * should be shut down, and the Error propagated.
  */
  void handleError(Error err);
}
