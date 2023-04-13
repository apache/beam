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

public interface BundleManager<OutT> {
  void tryStartBundle();

  public void processWatermark(Instant watermark, OpEmitter<OutT> emitter);

  public void processTimer(KeyedTimerData<Void> keyedTimerData, OpEmitter<OutT> emitter);

  public void signalFailure(Throwable t);

  public void tryFinishBundle(OpEmitter<OutT> emitter);

  /**
   * A listener used to track the lifecycle of a bundle. Typically, the lifecycle of a bundle
   * consists of 1. Start bundle - Invoked when the bundle is started 2. Finish bundle - Invoked
   * when the bundle is complete. Refer to the docs under {@link ClassicBundleManager} for
   * definition on when a bundle is considered complete. 3. onWatermark - Invoked when watermark is
   * ready to be propagated to downstream DAG. Refer to the docs under {@link ClassicBundleManager}
   * on when watermark is held vs propagated.
   *
   * @param <OutT>
   */
  public interface BundleProgressListener<OutT> {
    void onBundleStarted();

    void onBundleFinished(OpEmitter<OutT> emitter);

    void onWatermark(Instant watermark, OpEmitter<OutT> emitter);
  }
}
