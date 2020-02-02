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

import java.util.Arrays;
import java.util.List;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.joda.time.Instant;

/**
 * A {@link Trigger} that executes according to its main trigger until its "finally" trigger fires.
 *
 * <p>Uniquely among triggers, the "finally" trigger's predicate is applied to all input seen so
 * far, not input since the last firing.
 */
public class OrFinallyTrigger extends Trigger {

  private static final int ACTUAL = 0;
  private static final int UNTIL = 1;

  @VisibleForTesting
  OrFinallyTrigger(Trigger actual, Trigger.OnceTrigger until) {
    super(Arrays.asList(actual, until));
  }

  /**
   * The main trigger, which will continue firing until the "until" trigger fires. See {@link
   * #getUntilTrigger()}
   */
  public Trigger getMainTrigger() {
    return subTriggers().get(ACTUAL);
  }

  /** The trigger that signals termination of this trigger. */
  public OnceTrigger getUntilTrigger() {
    return (OnceTrigger) subTriggers().get(UNTIL);
  }

  @Override
  public Instant getWatermarkThatGuaranteesFiring(BoundedWindow window) {
    // This trigger fires once either the trigger or the until trigger fires.
    Instant actualDeadline = subTriggers.get(ACTUAL).getWatermarkThatGuaranteesFiring(window);
    Instant untilDeadline = subTriggers.get(UNTIL).getWatermarkThatGuaranteesFiring(window);
    return actualDeadline.isBefore(untilDeadline) ? actualDeadline : untilDeadline;
  }

  @Override
  public boolean mayFinish() {
    return subTriggers.get(ACTUAL).mayFinish() || subTriggers.get(UNTIL).mayFinish();
  }

  @Override
  protected Trigger getContinuationTrigger(List<Trigger> continuationTriggers) {
    // Use OrFinallyTrigger instead of AfterFirst because the continuation of ACTUAL
    // may not be a OnceTrigger.
    return Repeatedly.forever(
        new OrFinallyTrigger(
            continuationTriggers.get(ACTUAL),
            (Trigger.OnceTrigger) continuationTriggers.get(UNTIL)));
  }

  @Override
  public String toString() {
    return String.format("%s.orFinally(%s)", subTriggers.get(ACTUAL), subTriggers.get(UNTIL));
  }
}
