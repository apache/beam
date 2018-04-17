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

import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.util.WindowedValue;

/**
 * A bundle capable of handling input data elements for a
 * {@link org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleDescriptor bundle descriptor}
 * by forwarding them to a remote environment for processing.
 *
 * <p>When a RemoteBundle is closed, it will block until bundle processing is finished on remote
 * resources, and throw an exception if bundle processing has failed.
 */
public interface RemoteBundle<InputT> extends AutoCloseable {
  /**
   * Get an id used to represent this bundle.
   */
  String getId();

  /**
   * Get a {@link FnDataReceiver receiver} which consumes input elements, forwarding them to the
   * remote environment.
   */
  FnDataReceiver<WindowedValue<InputT>> getInputReceiver();
}
