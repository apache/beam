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
 * The flush-only view of a {@link KStreamsPayload}, obtained via {@link KStreamsPayload#asFlush()}.
 * As with {@link WatermarkPayload}, the accessors live here so they are only reachable once the
 * caller has checked the kind and narrowed the payload.
 *
 * <p>A flush marker asks the stage that receives it to close its open bundle and flush the output,
 * which is how a bundle is bounded in time. It arrives as an ordinary record, so the bundle is
 * closed from {@code process()} rather than from a punctuator: transactions are committed by the
 * Kafka Streams runtime in the background and are not exposed, so a bundle cannot be aligned with
 * one, and trying to do it from a punctuator duplicated output against a real broker
 * (https://github.com/apache/beam/issues/39633).
 *
 * <p>The partition fields exist so the marker can be targeted rather than broadcast. Broadcasting
 * would give a downstream partition one flush per upstream partition, so N times more flushes than
 * the interval asks for. Instead the producing partition addresses a slice of the downstream
 * partitions and the slices tile the range, so each downstream partition gets exactly one flush per
 * interval. Unlike a watermark, a flush needs no aggregation on arrival: there is nothing to hold
 * and nothing to combine, because only one arrives.
 */
public interface FlushPayload {

  /** Which partition of the producing transform emitted this marker. */
  int getSourcePartition();

  /** How many partitions the producing transform has in total. */
  int getTotalSourcePartitions();
}
