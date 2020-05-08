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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.transforms.windowing.Trigger.OnceTrigger;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Joiner;
import org.joda.time.Instant;

/**
 * A composite {@link Trigger} that fires once after at least one of its sub-triggers have fired.
 */
@Experimental(Kind.TRIGGER)
public class AfterFirst extends OnceTrigger {

  AfterFirst(List<Trigger> subTriggers) {
    super(subTriggers);
    checkArgument(subTriggers.size() > 1);
  }

  /** Returns an {@code AfterFirst} {@code Trigger} with the given subtriggers. */
  public static AfterFirst of(OnceTrigger... triggers) {
    return new AfterFirst(Arrays.asList(triggers));
  }

  /** Returns an {@code AfterFirst} {@code Trigger} with the given subtriggers. */
  public static AfterFirst of(List<Trigger> triggers) {
    return new AfterFirst(triggers);
  }

  @Override
  public Instant getWatermarkThatGuaranteesFiring(BoundedWindow window) {
    // This trigger will fire after the earliest of its sub-triggers.
    Instant deadline = BoundedWindow.TIMESTAMP_MAX_VALUE;
    for (Trigger subTrigger : subTriggers) {
      Instant subDeadline = subTrigger.getWatermarkThatGuaranteesFiring(window);
      if (deadline.isAfter(subDeadline)) {
        deadline = subDeadline;
      }
    }
    return deadline;
  }

  @Override
  protected OnceTrigger getContinuationTrigger(List<Trigger> continuationTriggers) {
    return new AfterFirst(continuationTriggers);
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder("AfterFirst.of(");
    Joiner.on(", ").appendTo(builder, subTriggers);
    builder.append(")");

    return builder.toString();
  }
}
