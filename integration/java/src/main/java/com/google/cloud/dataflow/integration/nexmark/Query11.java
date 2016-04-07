/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.integration.nexmark;

import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.windowing.AfterPane;
import com.google.cloud.dataflow.sdk.transforms.windowing.Repeatedly;
import com.google.cloud.dataflow.sdk.transforms.windowing.Sessions;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

import org.joda.time.Duration;

/**
 * Query "11", 'User sessions' (Not in original suite.)
 *
 * <p>Group bids by the same user into sessions with {@code windowSizeSec} max gap.
 * However limit the session to at most {@code maxLogEvents}. Emit the number of
 * bids per session.
 */
class Query11 extends NexmarkQuery {
  public Query11(NexmarkConfiguration configuration) {
    super(configuration, "Query11");
  }

  private PCollection<BidsPerSession> applyTyped(PCollection<Event> events) {
    return events.apply(JUST_BIDS)
        .apply(
            ParDo.named(name + ".Rekey")
                .of(new DoFn<Bid, KV<Long, Void>>() {
                  @Override
                  public void processElement(ProcessContext c) {
                    Bid bid = c.element();
                    c.output(KV.of(bid.bidder, (Void) null));
                  }
                }))
        .apply(Window.<KV<Long, Void>>into(
            Sessions.withGapDuration(Duration.standardSeconds(configuration.windowSizeSec)))
        .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(configuration.maxLogEvents)))
        .discardingFiredPanes()
        .withAllowedLateness(Duration.standardSeconds(configuration.occasionalDelaySec / 2)))
        .apply(Count.<Long, Void>perKey())
        .apply(
            ParDo.named(name + ".ToResult")
                .of(new DoFn<KV<Long, Long>, BidsPerSession>() {
                  @Override
                  public void processElement(ProcessContext c) {
                    c.output(new BidsPerSession(c.element().getKey(), c.element().getValue()));
                  }
                }));
  }

  @Override
  protected PCollection<KnownSize> applyPrim(PCollection<Event> events) {
    return NexmarkUtils.castToKnownSize(name, applyTyped(events));
  }
}
