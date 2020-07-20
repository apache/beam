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
import org.apache.beam.sdk.transforms.windowing.Trigger.OnceTrigger;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Objects;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * <i><b>FOR INTERNAL USE ONLY</b></i>. A trigger that fires after synchronized processing time has
 * reached the processing time of the first element's arrival.
 *
 * <p>This is for internal, primarily as a "continuation trigger" for {@link AfterProcessingTime}
 * triggers. In that use, this trigger is ready as soon as all upstream workers processing time
 * clocks have caught up to the moment that input arrived.
 */
public class AfterSynchronizedProcessingTime extends OnceTrigger {

  public static AfterSynchronizedProcessingTime ofFirstElement() {
    return new AfterSynchronizedProcessingTime();
  }

  private AfterSynchronizedProcessingTime() {
    super(null);
  }

  @Override
  public Instant getWatermarkThatGuaranteesFiring(BoundedWindow window) {
    return BoundedWindow.TIMESTAMP_MAX_VALUE;
  }

  @Override
  protected Trigger getContinuationTrigger(List<Trigger> continuationTriggers) {
    return this;
  }

  @Override
  public String toString() {
    return "AfterSynchronizedProcessingTime.pastFirstElementInPane()";
  }

  @Override
  public boolean equals(@Nullable Object obj) {
    return this == obj || obj instanceof AfterSynchronizedProcessingTime;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(AfterSynchronizedProcessingTime.class);
  }
}
