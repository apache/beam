/**
 * Copyright 2016-2017 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.flink.streaming;

import cz.seznam.euphoria.core.client.functional.ExtractEventTime;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;

import java.time.Duration;
import java.util.Objects;

class EventTimeAssigner
        extends BoundedOutOfOrdernessTimestampExtractor<StreamingElement>
{
  private final ExtractEventTime eventTimeFn;

  EventTimeAssigner(Duration allowedLateness, ExtractEventTime eventTimeFn) {
    super(millisTime(allowedLateness.toMillis()));
    this.eventTimeFn = Objects.requireNonNull(eventTimeFn);
  }

  @SuppressWarnings("unchecked")
  @Override
  public long extractTimestamp(StreamingElement element) {
    return eventTimeFn.extractTimestamp(element.getElement());
  }

  private static org.apache.flink.streaming.api.windowing.time.Time
  millisTime(long millis) {
    return org.apache.flink.streaming.api.windowing.time.Time.milliseconds(millis);
  }
}
