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

package org.apache.beam.integration.nexmark;

import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

/**
 * Query "12", 'Processing time windows' (Not in original suite.)
 * <p>
 * <p>Group bids by the same user into processing time windows of windowSize. Emit the count
 * of bids per window.
 */
class Query12 extends NexmarkQuery {
  public Query12(NexmarkConfiguration configuration) {
    super(configuration, "Query12");
  }

  private PCollection<BidsPerSession> applyTyped(PCollection<Event> events) {
    return events
        .apply(JUST_BIDS)
        .apply(
            ParDo.named(name + ".Rekey")
                 .of(new DoFn<Bid, KV<Long, Void>>() {
                   @Override
                   public void processElement(ProcessContext c) {
                     Bid bid = c.element();
                     c.output(KV.of(bid.bidder, (Void) null));
                   }
                 }))
        .apply(Window.<KV<Long, Void>>into(new GlobalWindows())
            .triggering(
                Repeatedly.forever(
                    AfterProcessingTime.pastFirstElementInPane()
                                       .plusDelayOf(
                                           Duration.standardSeconds(configuration.windowSizeSec))))
            .discardingFiredPanes()
            .withAllowedLateness(Duration.ZERO))
        .apply(Count.<Long, Void>perKey())
        .apply(
            ParDo.named(name + ".ToResult")
                 .of(new DoFn<KV<Long, Long>, BidsPerSession>() {
                   @Override
                   public void processElement(ProcessContext c) {
                     c.output(
                         new BidsPerSession(c.element().getKey(), c.element().getValue()));
                   }
                 }));
  }

  @Override
  protected PCollection<KnownSize> applyPrim(PCollection<Event> events) {
    return NexmarkUtils.castToKnownSize(name, applyTyped(events));
  }
}
