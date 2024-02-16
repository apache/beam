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

import org.apache.beam.runners.fnexecution.state.StateRequestHandler;

/**
 * A bundle factory scoped to a particular {@link
 * org.apache.beam.sdk.util.construction.graph.ExecutableStage}, which has all of the resources it
 * needs to provide new {@link RemoteBundle RemoteBundles}.
 *
 * <p>Closing a StageBundleFactory signals that the stage has completed and any resources bound to
 * its lifetime can be cleaned up.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public interface StageBundleFactory extends AutoCloseable {
  /** Get a new {@link RemoteBundle bundle} for processing the data in an executable stage. */
  default RemoteBundle getBundle(
      OutputReceiverFactory outputReceiverFactory,
      StateRequestHandler stateRequestHandler,
      BundleProgressHandler progressHandler)
      throws Exception {
    return getBundle(outputReceiverFactory, null, stateRequestHandler, progressHandler);
  }

  default RemoteBundle getBundle(
      OutputReceiverFactory outputReceiverFactory,
      StateRequestHandler stateRequestHandler,
      BundleProgressHandler progressHandler,
      BundleFinalizationHandler finalizationHandler)
      throws Exception {
    return getBundle(
        outputReceiverFactory, null, stateRequestHandler, progressHandler, finalizationHandler);
  }

  default RemoteBundle getBundle(
      OutputReceiverFactory outputReceiverFactory,
      StateRequestHandler stateRequestHandler,
      BundleProgressHandler progressHandler,
      BundleFinalizationHandler finalizationHandler,
      BundleCheckpointHandler checkpointHandler)
      throws Exception {
    return getBundle(
        outputReceiverFactory,
        null,
        stateRequestHandler,
        progressHandler,
        finalizationHandler,
        checkpointHandler);
  }

  /** Get a new {@link RemoteBundle bundle} for processing the data in an executable stage. */
  default RemoteBundle getBundle(
      OutputReceiverFactory outputReceiverFactory,
      TimerReceiverFactory timerReceiverFactory,
      StateRequestHandler stateRequestHandler,
      BundleProgressHandler progressHandler)
      throws Exception {
    return getBundle(
        outputReceiverFactory, timerReceiverFactory, stateRequestHandler, progressHandler, null);
  }

  default RemoteBundle getBundle(
      OutputReceiverFactory outputReceiverFactory,
      TimerReceiverFactory timerReceiverFactory,
      StateRequestHandler stateRequestHandler,
      BundleProgressHandler progressHandler,
      BundleFinalizationHandler finalizationHandler)
      throws Exception {
    return getBundle(
        outputReceiverFactory,
        timerReceiverFactory,
        stateRequestHandler,
        progressHandler,
        finalizationHandler,
        null);
  }

  RemoteBundle getBundle(
      OutputReceiverFactory outputReceiverFactory,
      TimerReceiverFactory timerReceiverFactory,
      StateRequestHandler stateRequestHandler,
      BundleProgressHandler progressHandler,
      BundleFinalizationHandler finalizationHandler,
      BundleCheckpointHandler checkpointHandler)
      throws Exception;

  ProcessBundleDescriptors.ExecutableProcessBundleDescriptor getProcessBundleDescriptor();

  InstructionRequestHandler getInstructionRequestHandler();
}
