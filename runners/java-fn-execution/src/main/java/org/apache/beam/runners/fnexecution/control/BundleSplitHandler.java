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
package org.apache.beam.runners.fnexecution.control;

import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleSplitResponse;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;

/** A handler for bundle splits and checkpoints. */
// TODO: Consider making this only handle SDK initiated checkpoints and make RemoteBundle#split
// a blocking call.
@Experimental(Kind.SPLITTABLE_DO_FN)
public interface BundleSplitHandler {
  /** Handles a split from the bundle while it is executing. */
  void onSplit(ProcessBundleSplitResponse splitResponse);

  /** Handles the bundle's checkpoint. */
  void onCheckpoint(ProcessBundleResponse checkpointResponse);

  /** Returns a handler that fails if a split or checkpoint is requested. */
  static BundleSplitHandler unsupported() {
    return new BundleSplitHandler() {
      @Override
      public void onSplit(ProcessBundleSplitResponse response) {
        throw new UnsupportedOperationException(
            String.format("This runner does not support this type of split %s", response));
      }

      @Override
      public void onCheckpoint(ProcessBundleResponse response) {
        throw new UnsupportedOperationException(
            String.format("This runner does not support this type of checkpoint %s", response));
      }
    };
  }
}
