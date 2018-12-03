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
package org.apache.beam.fn.harness.control;

import java.util.List;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.BundleApplication;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.DelayedBundleApplication;

/**
 * Allows for PTransforms to notify when they split the execution of an element into a primary and
 * residual root.
 *
 * <p>See <a href="https://s.apache.org/beam-breaking-fusion">Breaking the Fusion Barrier</a> for a
 * discussion of the design.
 */
public interface BundleExecutionController {

  /**
   * Informs the PTransform runner to buffer input instead of processing it.
   *
   * <p>Note this method allows the processing thread to be taken over by the framework allowing the
   * framework to ask transforms to checkpoint.
   */
  // TODO: Consider an explicit callback pause/resume mechanism that allows for a local variable
  // to store and control whether buffering is being performed instead of incurring the cost
  // of a volatile read.
  boolean shouldBuffer() throws Exception;

  /**
   * Signals that the current PTransform has split the current application into a completed primary
   * root and unstarted residual roots.
   */
  void checkpoint(BundleApplication primaryRoot, List<DelayedBundleApplication> residualRoots);

  /**
   * Signals that the current PTransform has buffered the following input instead of processing it.
   */
  void buffer(BundleApplication buffer);

  // TODO: Consider moving checkpoint registration to only exist during PTransform construction
  // instead of being contained within the BundleExecutionController.
  void registerCheckpointListener(CheckpointListener checkpointListener);

  @FunctionalInterface
  interface CheckpointListener {
    /** Informs the runner to checkpoint the current application. */
    void checkpoint();
  }
}
