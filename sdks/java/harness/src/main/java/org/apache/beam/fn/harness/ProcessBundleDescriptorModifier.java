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
package org.apache.beam.fn.harness;

import org.apache.beam.model.fnexecution.v1.BeamFnApi;

public interface ProcessBundleDescriptorModifier {
  class GraphModificationException extends Exception {
    public GraphModificationException() {}

    public GraphModificationException(String message) {
      super(message);
    }

    public GraphModificationException(String message, Throwable throwable) {
      super(throwable);
    }
  }

  /**
   * Modifies the given ProcessBundleDescriptor in-place. Throws a `GraphModificationException` on
   * failure. Can be used to instrument functionality onto an existing ProcessBundleDescriptor. For
   * instance, this is used to add DataSampling PTransforms to a graph to sample in-flight elements.
   */
  BeamFnApi.ProcessBundleDescriptor modifyProcessBundleDescriptor(
      BeamFnApi.ProcessBundleDescriptor pbd) throws GraphModificationException;
}
