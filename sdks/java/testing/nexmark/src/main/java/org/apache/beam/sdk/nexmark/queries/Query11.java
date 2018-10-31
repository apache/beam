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
package org.apache.beam.sdk.nexmark.queries;

import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.NexmarkUtils;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.beam.sdk.nexmark.model.BidsPerSession;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.nexmark.model.KnownSize;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

/**
 * Query "11", 'User sessions' (Not in original suite.)
 *
 * <p>Group bids by the same user into sessions with {@code windowSizeSec} max gap. However limit
 * the session to at most {@code maxLogEvents}. Emit the number of bids per session.
 */
public class Query11 extends NexmarkQuery {
  public Query11(NexmarkConfiguration configuration) {
    super(configuration, "Query11");
  }

  private PCollection<BidsPerSession> applyTyped(PCollection<Event> events) {
    PCollection<Long> bidders =
        events
            .apply(JUST_BIDS)
            .apply(
                name + ".Rekey",
                ParDo.of(
                    new DoFn<Bid, Long>() {

                      @ProcessElement
                      public void processElement(ProcessContext c) {
                        Bid bid = c.element();
                        c.output(bid.bidder);
                      }
                    }));

    PCollection<Long> biddersWindowed =
        bidders.apply(
            Window.<Long>into(
                    Sessions.withGapDuration(Duration.standardSeconds(configuration.windowSizeSec)))
                .triggering(
                    Repeatedly.forever(AfterPane.elementCountAtLeast(configuration.maxLogEvents)))
                .discardingFiredPanes()
                .withAllowedLateness(
                    Duration.standardSeconds(configuration.occasionalDelaySec / 2)));
    return biddersWindowed
        .apply(Count.perElement())
        .apply(
            name + ".ToResult",
            ParDo.of(
                new DoFn<KV<Long, Long>, BidsPerSession>() {

                  @ProcessElement
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
