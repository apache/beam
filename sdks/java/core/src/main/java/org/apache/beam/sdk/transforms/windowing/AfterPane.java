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
import java.util.Objects;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.transforms.windowing.Trigger.OnceTrigger;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * A {@link Trigger} that fires at some point after a specified number of input elements have
 * arrived.
 */
@Experimental(Kind.TRIGGER)
public class AfterPane extends OnceTrigger {

  private final int countElems;

  private AfterPane(int countElems) {
    super(null);
    this.countElems = countElems;
  }

  /** The number of elements after which this trigger may fire. */
  public int getElementCount() {
    return countElems;
  }

  /** Creates a trigger that fires when the pane contains at least {@code countElems} elements. */
  public static AfterPane elementCountAtLeast(int countElems) {
    return new AfterPane(countElems);
  }

  @Override
  public boolean isCompatible(Trigger other) {
    return this.equals(other);
  }

  @Override
  public Instant getWatermarkThatGuaranteesFiring(BoundedWindow window) {
    return BoundedWindow.TIMESTAMP_MAX_VALUE;
  }

  @Override
  protected OnceTrigger getContinuationTrigger(List<Trigger> continuationTriggers) {
    return AfterPane.elementCountAtLeast(1);
  }

  @Override
  public String toString() {
    return "AfterPane.elementCountAtLeast(" + countElems + ")";
  }

  @Override
  public boolean equals(@Nullable Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof AfterPane)) {
      return false;
    }
    AfterPane that = (AfterPane) obj;
    return this.countElems == that.countElems;
  }

  @Override
  public int hashCode() {
    return Objects.hash(countElems);
  }
}
