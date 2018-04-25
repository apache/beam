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

import org.apache.beam.runners.local.Bundle;

/**
 * An executor that is capable of processing some bundle of input over some executable stage or
 * step.
 */
interface BundleProcessor<
    CollectionT, BundleT extends Bundle<?, ? extends CollectionT>, ExecutableT> {
  /**
   * Execute the provided bundle using the provided Executable, calling back to the {@link
   * CompletionCallback} when execution completes.
   */
  void process(BundleT bundle, ExecutableT consumer, CompletionCallback onComplete);
}
