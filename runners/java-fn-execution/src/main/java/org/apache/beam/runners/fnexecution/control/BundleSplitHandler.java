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

import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleSplitResponse;

/**
 * A handler which is invoked whenever an active bundle is split. The active bundle will continue to
 * keep processing until it is complete.
 *
 * <p>The returned split response contains a description of work that has been performed containing
 * a {@code primary} portion that the SDK is responsible for processing and a {@code residual} which
 * the runner is responsible for scheduling for future processing. See <a
 * href="https://s.apache.org/beam-breaking-fusion">breaking the fusion barrier</a> for further
 * details.
 */
public interface BundleSplitHandler {
  void split(ProcessBundleSplitResponse splitResponse);

  /** Returns a bundle split handler that throws on any split response. */
  static BundleSplitHandler unsupported() {
    return new BundleSplitHandler() {
      @Override
      public void split(ProcessBundleSplitResponse splitResponse) {
        throw new UnsupportedOperationException(
            String.format(
                "%s does not support splitting.", BundleSplitHandler.class.getSimpleName()));
      }
    };
  };
}
