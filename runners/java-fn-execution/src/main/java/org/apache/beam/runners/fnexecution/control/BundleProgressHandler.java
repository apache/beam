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

import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleProgressResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleResponse;

/**
 * A handler for bundle progress messages, both during bundle execution and on its completion.
 *
 * <p>Methods on this interface will only be called serially, and after {@link #onCompleted} no more
 * calls will be made.
 */
public interface BundleProgressHandler {
  /** Handles a progress report from the bundle while it is executing. */
  void onProgress(ProcessBundleProgressResponse progress);

  /** Handles the bundle's completion report. */
  void onCompleted(ProcessBundleResponse response);

  /** Returns a handler that ignores metrics. */
  static BundleProgressHandler ignored() {
    return new BundleProgressHandler() {
      @Override
      public void onProgress(ProcessBundleProgressResponse progress) {}

      @Override
      public void onCompleted(ProcessBundleResponse response) {}
    };
  }
}
