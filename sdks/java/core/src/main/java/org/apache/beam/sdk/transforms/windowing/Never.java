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
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.windowing.Trigger.OnceTrigger;
import org.joda.time.Instant;

/**
 * A {@link Trigger} which never fires.
 *
 * <p>Using this trigger will only produce output when the watermark passes the end of the {@link
 * BoundedWindow window} plus the {@link Window#withAllowedLateness allowed lateness}.
 */
public final class Never {
  /**
   * Returns a trigger which never fires. Output will be produced from the using {@link GroupByKey}
   * when the {@link BoundedWindow} closes.
   */
  public static NeverTrigger ever() {
    // NeverTrigger ignores all inputs and is Window-type independent.
    return new NeverTrigger();
  }

  /** The actual trigger class for {@link Never} triggers. */
  public static class NeverTrigger extends OnceTrigger {
    private NeverTrigger() {
      super(null);
    }

    @Override
    protected Trigger getContinuationTrigger(List<Trigger> continuationTriggers) {
      return this;
    }

    @Override
    public Instant getWatermarkThatGuaranteesFiring(BoundedWindow window) {
      return BoundedWindow.TIMESTAMP_MAX_VALUE;
    }
  }
}
