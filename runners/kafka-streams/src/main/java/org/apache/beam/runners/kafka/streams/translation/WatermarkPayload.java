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
package org.apache.beam.runners.kafka.streams.translation;

/**
 * The watermark-only view of a {@link KStreamsPayload}, obtained via {@link
 * KStreamsPayload#asWatermark()}. Keeping the watermark accessors on this interface — rather than
 * on {@link KStreamsPayload} itself — means they are only reachable after the caller has checked
 * {@link KStreamsPayload#isWatermark()} and narrowed the payload, so there is no kind check to do
 * on each accessor.
 *
 * <p>A watermark report is the in-band coordination message a downstream watermark aggregator
 * consumes: the watermark value, which transform produced it, which of that transform's partitions
 * reported it, and how many partitions that transform has in total. The producer stamps its own
 * identity without regard to who consumes the report; a consumer with several upstream transforms
 * (e.g. Flatten) aggregates per producing transform.
 */
public interface WatermarkPayload {

  /** The reported watermark, in event-time milliseconds. */
  long getWatermarkMillis();

  /** Globally unique id of the transform that produced this report. */
  String getTransformId();

  /**
   * Which partition (physical instance) of the producing transform this report is for, in {@code
   * [0, getTotalSourcePartitions())}.
   */
  int getSourcePartition();

  /** How many partitions (physical instances) the producing transform has in total. */
  int getTotalSourcePartitions();
}
