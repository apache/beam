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
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.joda.time.Instant;

/** No-op {@link OnceTrigger} implementation for testing. */
abstract class StubTrigger extends Trigger.OnceTrigger {
  /**
   * Create a stub {@link Trigger} instance which returns the specified name on {@link #toString()}.
   */
  static StubTrigger named(final String name) {
    return new StubTrigger() {
      @Override
      public String toString() {
        return name;
      }
    };
  }

  protected StubTrigger() {
    super(Lists.newArrayList());
  }

  @Override
  protected Trigger getContinuationTrigger(List<Trigger> continuationTriggers) {
    return null;
  }

  @Override
  public Instant getWatermarkThatGuaranteesFiring(BoundedWindow window) {
    return null;
  }
}
