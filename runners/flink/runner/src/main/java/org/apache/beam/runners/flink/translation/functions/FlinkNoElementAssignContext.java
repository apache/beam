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
package org.apache.beam.runners.flink.translation.functions;

import org.apache.beam.runners.core.OldDoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.joda.time.Instant;

/**
 * {@link WindowFn.AssignContext} for calling a {@link WindowFn} for elements emitted from
 * {@link OldDoFn#startBundle(OldDoFn.Context)}
 * or {@link OldDoFn#finishBundle(OldDoFn.Context)}.
 *
 * <p>In those cases the {@code WindowFn} is not allowed to access any element information.
 */
class FlinkNoElementAssignContext<InputT, W extends BoundedWindow>
    extends WindowFn<InputT, W>.AssignContext {

  private final InputT element;
  private final Instant timestamp;

  FlinkNoElementAssignContext(
      WindowFn<InputT, W> fn,
      InputT element,
      Instant timestamp) {
    fn.super();

    this.element = element;
    // the timestamp can be null, in that case output is called
    // without a timestamp
    this.timestamp = timestamp;
  }

  @Override
  public InputT element() {
    return element;
  }

  @Override
  public Instant timestamp() {
    if (timestamp != null) {
      return timestamp;
    } else {
      throw new UnsupportedOperationException("No timestamp available.");
    }
  }

  @Override
  public BoundedWindow window() {
    throw new UnsupportedOperationException("No window available.");
  }
}
