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
package org.apache.beam.runners.samza.runtime;

import org.joda.time.Instant;

/**
 * Bundle management for the {@link DoFnOp} that handles lifecycle of a bundle. It also serves as a
 * proxy for the {@link DoFnOp} to process watermark and decides to 1. Hold watermark if there is at
 * least one bundle in progress. 2. Propagates the watermark to downstream DAG, if all the previous
 * bundles have completed.
 *
 * <p>A bundle is considered complete only when the outputs corresponding to each element in the
 * bundle have been resolved and the watermark associated with the bundle(if any) is propagated
 * downstream. The output of an element is considered resolved based on the nature of the ParDoFn 1.
 * In case of synchronous ParDo, outputs of the element is resolved immediately after the
 * processElement returns. 2. In case of asynchronous ParDo, outputs of the element is resolved when
 * all the future emitted by the processElement is resolved.
 *
 * @param <OutT> output type of the {@link DoFnOp}
 */
public interface BundleManager<OutT> {
  /** Starts a new bundle if not already started, then adds an element to the existing bundle. */
  void tryStartBundle();

  /**
   * Signals a watermark event arrived. The BundleManager will decide if the watermark needs to be
   * processed, and notify the listener if needed.
   *
   * @param watermark
   * @param emitter
   */
  void processWatermark(Instant watermark, OpEmitter<OutT> emitter);

  /**
   * Signals the BundleManager that a timer is up.
   *
   * @param keyedTimerData
   * @param emitter
   */
  void processTimer(KeyedTimerData<Void> keyedTimerData, OpEmitter<OutT> emitter);

  /**
   * Fails the current bundle, throws away the pending output, and resets the bundle to an empty
   * state.
   *
   * @param t the throwable that caused the failure.
   */
  void signalFailure(Throwable t);

  /**
   * Tries to close the bundle, and reset the bundle to an empty state.
   *
   * @param emitter
   */
  void tryFinishBundle(OpEmitter<OutT> emitter);

  /**
   * A listener used to track the lifecycle of a bundle. Typically, the lifecycle of a bundle
   * consists of 1. Start bundle - Invoked when the bundle is started 2. Finish bundle - Invoked
   * when the bundle is complete. Refer to the docs under {@link BundleManager} for definition on
   * when a bundle is considered complete. 3. onWatermark - Invoked when watermark is ready to be
   * propagated to downstream DAG. Refer to the docs under {@link BundleManager} on when watermark
   * is held vs propagated.
   *
   * @param <OutT>
   */
  interface BundleProgressListener<OutT> {
    void onBundleStarted();

    void onBundleFinished(OpEmitter<OutT> emitter);

    void onWatermark(Instant watermark, OpEmitter<OutT> emitter);
  }
}
