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
package org.apache.beam.sdk.transforms.windowing;

import java.util.List;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.joda.time.Instant;

/**
 * <b>For internal use only; no backwards compatibility guarantees.</b>
 *
 * <p>The trigger used with {@link Reshuffle} which triggers on every element and never buffers
 * state.
 *
 * @param <W> The kind of window that is being reshuffled.
 */
@Internal
public class ReshuffleTrigger<W extends BoundedWindow> extends Trigger {

  public ReshuffleTrigger() {
    super();
  }

  @Override
  protected Trigger getContinuationTrigger(List<Trigger> continuationTriggers) {
    return this;
  }

  @Override
  public Instant getWatermarkThatGuaranteesFiring(BoundedWindow window) {
    throw new UnsupportedOperationException(
        "ReshuffleTrigger should not be used outside of Reshuffle");
  }

  @Override
  public boolean mayFinish() {
    return false;
  }

  @Override
  public String toString() {
    return "ReshuffleTrigger()";
  }
}
