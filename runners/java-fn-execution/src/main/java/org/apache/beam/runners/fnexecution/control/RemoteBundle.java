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

import java.util.Map;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.util.construction.Timer;
import org.apache.beam.sdk.values.KV;

/**
 * A bundle capable of handling input data elements for a {@link
 * org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleDescriptor bundle descriptor} by
 * forwarding them to a remote environment for processing.
 *
 * <p>When a RemoteBundle is closed, it will block until bundle processing is finished on remote
 * resources, and throw an exception if bundle processing has failed.
 */
@SuppressWarnings({
  "rawtypes" // TODO(https://github.com/apache/beam/issues/20447)
})
public interface RemoteBundle extends AutoCloseable {
  /** Get an id used to represent this bundle. */
  String getId();

  /**
   * Get a map of PCollection ids to {@link FnDataReceiver receiver}s which consume input elements,
   * forwarding them to the remote environment.
   */
  Map<String, FnDataReceiver> getInputReceivers();

  /**
   * Get a map of (transform id, timer id) to {@link FnDataReceiver receiver}s which consume timers,
   * forwarding them to the remote environment.
   */
  Map<KV<String, String>, FnDataReceiver<Timer>> getTimerReceivers();

  /**
   * Ask the remote bundle for progress.
   *
   * <p>This method is a no-op if the bundle is complete otherwise it will return after the request
   * has been issued. Any progress reports will be forwarded to the {@link BundleProgressHandler}.
   *
   * <p>All {@link BundleProgressHandler#onProgress} calls are guaranteed to be called before any
   * {@link BundleProgressHandler#onCompleted}.
   */
  void requestProgress();

  /**
   * Ask the remote bundle to split its current processing based upon its knowledge of remaining
   * work. A fraction of 0, is equivalent to asking the SDK to checkpoint.
   *
   * <p>This method is a no-op if the bundle is complete otherwise it will return after the request
   * has been issued. Any splits will be forwarded to the {@link BundleSplitHandler}.
   *
   * <p>All {@link BundleSplitHandler#split} calls are guaranteed to be called before any {@link
   * BundleCheckpointHandler#onCheckpoint}.
   */
  void split(double fractionOfRemainder);

  /**
   * Closes this bundle. This causes the input {@link FnDataReceiver} to be closed (future calls to
   * that {@link FnDataReceiver} will throw an exception), and causes the {@link RemoteBundle} to
   * produce any buffered outputs. The call to {@link #close()} will block until all of the outputs
   * produced by this bundle have been received and all outstanding progress and split requests have
   * been handled.
   */
  @Override
  void close() throws Exception;
}
