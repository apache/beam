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
 * <p>A watermark report is the in-band coordination message a downstream stage's {@link
 * WatermarkManager} consumes: the watermark value plus which source partition reported it and how
 * many source partitions feed the stage in total.
 */
public interface WatermarkPayload {

  /** The reported watermark, in event-time milliseconds. */
  long getWatermarkMillis();

  /** The source partition this report is for, in {@code [0, getTotalSourcePartitions())}. */
  int getSourcePartition();

  /** The total number of source partitions feeding the downstream stage. */
  int getTotalSourcePartitions();
}
