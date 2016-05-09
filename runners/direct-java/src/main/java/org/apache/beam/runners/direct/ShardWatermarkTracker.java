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

package org.apache.beam.runners.direct;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import org.apache.beam.sdk.transforms.windowing.BoundedWindow;

import org.joda.time.Instant;

import java.util.HashMap;
import java.util.Map;

import autovalue.shaded.com.google.common.common.collect.Iterables;

/**
 * Tracks the value of a watermark across multiple shards of a TransformEvaluator.
 */
public class ShardWatermarkTracker {
  public static ShardWatermarkTracker create() {
    return new ShardWatermarkTracker();
  }

  private final Map<TransformEvaluator<?>, Instant> evaluatorWatermarks;

  private ShardWatermarkTracker() {
    evaluatorWatermarks = new HashMap<>();
  }

  public synchronized void setInitialShards(Iterable<? extends TransformEvaluator<?>> shards) {
    checkArgument(!Iterables.isEmpty(shards),
        "ShardWatermarkTracker must contain at least one shard");
    checkArgument(evaluatorWatermarks.isEmpty(),
        "ShardWatermarkTracker cannot be initialized multiple times");
    for (TransformEvaluator<?> evaluator : shards) {
      evaluatorWatermarks.put(evaluator, BoundedWindow.TIMESTAMP_MIN_VALUE);
    }
  }

  public synchronized void updateWatermark(TransformEvaluator<?> evaluator, Instant newWatermark) {
    evaluatorWatermarks.put(evaluator, newWatermark);
  }

  public synchronized Instant getWatermark() {
    Instant minwm = null;
    for (Instant wm : evaluatorWatermarks.values()) {
      if (minwm == null || wm.isBefore(minwm)) {
        minwm = wm;
      }
    }
    checkState(minwm != null, "Cannot use a ShardWatermarkTracker before it is initalized");
    return minwm;
  }
}
