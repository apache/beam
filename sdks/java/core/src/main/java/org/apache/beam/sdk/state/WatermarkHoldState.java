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
package org.apache.beam.sdk.state;

import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.joda.time.Instant;

/**
 * <b><i>For internal use only; no backwards-compatibility guarantees.</i></b>
 *
 * <p>A {@link State} accepting and aggregating timestamps according to a {@link TimestampCombiner},
 * holding the output watermark to the resulting timestamp.
 */
@Internal
public interface WatermarkHoldState extends GroupingState<Instant, Instant> {
  /**
   * Return the {@link TimestampCombiner} which will be used to determine a watermark hold time
   * given an element timestamp, and to combine watermarks from windows which are about to be
   * merged.
   */
  TimestampCombiner getTimestampCombiner();

  @Override
  WatermarkHoldState readLater();
}
