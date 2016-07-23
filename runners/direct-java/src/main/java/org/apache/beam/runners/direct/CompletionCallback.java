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
package org.apache.beam.runners.direct;

import org.apache.beam.runners.direct.DirectRunner.CommittedBundle;

/**
 * A callback for completing a bundle of input.
 */
interface CompletionCallback {
  /**
   * Handle a successful result, returning the committed outputs of the result.
   */
  CommittedResult handleResult(
      CommittedBundle<?> inputBundle, TransformResult result);

  /**
   * Handle an input bundle that did not require processing.
   */
  void handleEmpty(CommittedBundle<?> inputBundle);

  /**
   * Handle a result that terminated abnormally due to the provided {@link Throwable}.
   */
  void handleThrowable(CommittedBundle<?> inputBundle, Throwable t);
}
