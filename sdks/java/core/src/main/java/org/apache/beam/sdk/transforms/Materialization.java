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
package org.apache.beam.sdk.transforms;

import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.annotations.Internal;

/**
 * <b><i>For internal use only; no backwards-compatibility guarantees.</i></b>
 *
 * <p>How a view should be physically materialized by a {@link PipelineRunner}.
 *
 * <p>A {@link PipelineRunner} will support some set of materializations, and will reject {@link
 * ViewFn ViewFns} that require materializations it does not support. See {@link Materializations}
 * for known implementations.
 */
@Internal
public interface Materialization<T> {
  /**
   * Gets the URN describing this {@link Materialization}. This is a stable, SDK-independent URN
   * understood by a {@link PipelineRunner}.
   */
  String getUrn();
}
