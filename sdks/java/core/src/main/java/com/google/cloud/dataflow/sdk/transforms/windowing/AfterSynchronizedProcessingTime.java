/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataflow.sdk.transforms.windowing;

import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.util.TimeDomain;
import com.google.common.base.Objects;

import org.joda.time.Instant;

import java.util.Collections;
import java.util.List;

import javax.annotation.Nullable;

class AfterSynchronizedProcessingTime<W extends BoundedWindow>
    extends AfterDelayFromFirstElement<W> {

  @Override
  @Nullable
  public Instant getCurrentTime(Trigger<W>.TriggerContext context) {
    return context.currentSynchronizedProcessingTime();
  }

  public AfterSynchronizedProcessingTime() {
    super(TimeDomain.SYNCHRONIZED_PROCESSING_TIME,
        Collections.<SerializableFunction<Instant, Instant>>emptyList());
  }

  @Override
  public Instant getWatermarkThatGuaranteesFiring(W window) {
    return BoundedWindow.TIMESTAMP_MAX_VALUE;
  }

  @Override
  protected Trigger<W> getContinuationTrigger(List<Trigger<W>> continuationTriggers) {
    return this;
  }

  @Override
  public String toString() {
    return "AfterSynchronizedProcessingTime.pastFirstElementInPane()";
  }

  @Override
  public boolean equals(Object obj) {
    return this == obj || obj instanceof AfterSynchronizedProcessingTime;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(AfterSynchronizedProcessingTime.class);
  }

  @Override
  protected AfterSynchronizedProcessingTime<W>
      newWith(List<SerializableFunction<Instant, Instant>> transforms) {
    // ignore transforms
    return this;
  }

}
