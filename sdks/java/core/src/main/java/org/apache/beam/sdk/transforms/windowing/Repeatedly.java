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
import org.joda.time.Instant;

/**
 * A {@link Trigger} that fires according to its subtrigger forever.
 *
 * <p>For example, to fire after the end of the window, and every time late data arrives:
 *
 * <pre>{@code
 * Repeatedly.forever(AfterWatermark.pastEndOfWindow());
 * }</pre>
 *
 * <p>{@code Repeatedly.forever(someTrigger)} behaves like an infinite {@code
 * AfterEach.inOrder(someTrigger, someTrigger, someTrigger, ...)}.
 *
 * <p>You can use {@link #orFinally(OnceTrigger)} to let another trigger interrupt the repetition.
 */
public class Repeatedly extends Trigger {

  private static final int REPEATED = 0;

  /**
   * Create a composite trigger that repeatedly executes the trigger {@code repeated}, firing each
   * time it fires and ignoring any indications to finish.
   *
   * <p>Unless used with {@link Trigger#orFinally} the composite trigger will never finish.
   *
   * @param repeated the trigger to execute repeatedly.
   */
  public static Repeatedly forever(Trigger repeated) {
    return new Repeatedly(repeated);
  }

  private Trigger repeatedTrigger;

  private Repeatedly(Trigger repeatedTrigger) {
    super(Arrays.asList(repeatedTrigger));
    this.repeatedTrigger = repeatedTrigger;
  }

  public Trigger getRepeatedTrigger() {
    return repeatedTrigger;
  }

  @Override
  public Instant getWatermarkThatGuaranteesFiring(BoundedWindow window) {
    // This trigger fires once the repeated trigger fires.
    return subTriggers.get(REPEATED).getWatermarkThatGuaranteesFiring(window);
  }

  @Override
  public boolean mayFinish() {
    return false;
  }

  @Override
  protected Trigger getContinuationTrigger(List<Trigger> continuationTriggers) {
    return new Repeatedly(continuationTriggers.get(REPEATED));
  }

  @Override
  public String toString() {
    return String.format("Repeatedly.forever(%s)", subTriggers.get(REPEATED));
  }
}
