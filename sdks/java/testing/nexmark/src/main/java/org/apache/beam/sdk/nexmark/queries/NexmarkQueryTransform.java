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
package org.apache.beam.sdk.nexmark.queries;

import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.nexmark.model.KnownSize;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Base class for 'NEXMark' query transforms.
 *
 * <p>A query transform maps an event stream to some known size element stream.
 *
 * <p>A query transform may request a faked side input to join with ids.
 */
public abstract class NexmarkQueryTransform<T extends KnownSize>
    extends PTransform<PCollection<Event>, PCollection<T>> {

  private transient PCollection<KV<Long, String>> sideInput = null;

  protected NexmarkQueryTransform(String name) {
    super(name);
  }

  /** Whether this query expects a side input to be populated. Defaults to {@code false}. */
  public boolean needsSideInput() {
    return false;
  }

  /**
   * Set the side input for the query.
   *
   * <p>Note that due to the nature of side inputs, this instance of the query is now fixed and can
   * only be safely applied in the pipeline where the side input was created.
   */
  public void setSideInput(PCollection<KV<Long, String>> sideInput) {
    this.sideInput = sideInput;
  }

  /** Get the side input, if any. */
  public @Nullable PCollection<KV<Long, String>> getSideInput() {
    return sideInput;
  }
}
