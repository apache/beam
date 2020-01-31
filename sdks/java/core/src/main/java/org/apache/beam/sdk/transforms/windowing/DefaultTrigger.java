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
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.joda.time.Instant;

/**
 * A trigger that is equivalent to {@code Repeatedly.forever(AfterWatermark.pastEndOfWindow())}. See
 * {@link Repeatedly#forever} and {@link AfterWatermark#pastEndOfWindow} for more details.
 *
 * <p>This is a distinguished class to make it easy for runners to optimize for this common case.
 */
@Experimental(Kind.TRIGGER)
public class DefaultTrigger extends Trigger {

  private DefaultTrigger() {
    super();
  }

  /** Returns the default trigger. */
  public static DefaultTrigger of() {
    return new DefaultTrigger();
  }

  @Override
  public Instant getWatermarkThatGuaranteesFiring(BoundedWindow window) {
    return window.maxTimestamp();
  }

  /** @return false; the default trigger never finishes */
  @Override
  public boolean mayFinish() {
    return false;
  }

  @Override
  public boolean isCompatible(Trigger other) {
    // Semantically, all default triggers are identical
    return other instanceof DefaultTrigger;
  }

  @Override
  protected Trigger getContinuationTrigger(List<Trigger> continuationTriggers) {
    return this;
  }
}
