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
package org.apache.beam.runners.core.triggers;

import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * {@code AfterProcessingTime} triggers fire based on the current processing time. They operate in
 * the real-time domain.
 *
 * <p>The time at which to fire the timer can be adjusted via the methods in {@link
 * AfterDelayFromFirstElementStateMachine}, such as {@link
 * AfterDelayFromFirstElementStateMachine#plusDelayOf} or {@link
 * AfterDelayFromFirstElementStateMachine#alignedTo}.
 */
// The superclass should be inlined here, its only real use
// https://issues.apache.org/jira/browse/BEAM-1486
public class AfterProcessingTimeStateMachine extends AfterDelayFromFirstElementStateMachine {

  @Override
  public @Nullable Instant getCurrentTime(TriggerStateMachine.TriggerContext context) {
    return context.currentProcessingTime();
  }

  private AfterProcessingTimeStateMachine(List<SerializableFunction<Instant, Instant>> transforms) {
    super(TimeDomain.PROCESSING_TIME, transforms);
  }

  /**
   * Creates a trigger that fires when the current processing time passes the processing time at
   * which this trigger saw the first element in a pane.
   */
  public static AfterProcessingTimeStateMachine pastFirstElementInPane() {
    return new AfterProcessingTimeStateMachine(IDENTITY);
  }

  @Override
  protected AfterProcessingTimeStateMachine newWith(
      List<SerializableFunction<Instant, Instant>> transforms) {
    return new AfterProcessingTimeStateMachine(transforms);
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder("AfterProcessingTime.pastFirstElementInPane()");
    for (SerializableFunction<Instant, Instant> delayFn : timestampMappers) {
      builder.append(".plusDelayOf(").append(delayFn).append(")");
    }

    return builder.toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof AfterProcessingTimeStateMachine)) {
      return false;
    }
    AfterProcessingTimeStateMachine that = (AfterProcessingTimeStateMachine) obj;
    return Objects.equals(this.timestampMappers, that.timestampMappers);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), this.timestampMappers);
  }
}
