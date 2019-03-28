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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.model.AuctionCount;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

/**
 * Query 5, 'Hot Items'. Which auctions have seen the most bids in the last hour (updated every
 * minute). In CQL syntax:
 *
 * <pre>{@code
 * SELECT Rstream(auction)
 * FROM (SELECT B1.auction, count(*) AS num
 *       FROM Bid [RANGE 60 MINUTE SLIDE 1 MINUTE] B1
 *       GROUP BY B1.auction)
 * WHERE num >= ALL (SELECT count(*)
 *                   FROM Bid [RANGE 60 MINUTE SLIDE 1 MINUTE] B2
 *                   GROUP BY B2.auction);
 * }</pre>
 *
 * <p>To make things a bit more dynamic and easier to test we use much shorter windows, and we'll
 * also preserve the bid counts.
 */
public class Query5 extends NexmarkQueryTransform<AuctionCount> {
  private final NexmarkConfiguration configuration;

  public Query5(NexmarkConfiguration configuration) {
    super("Query5");
    this.configuration = configuration;
  }

  @Override
  public PCollection<AuctionCount> expand(PCollection<Event> events) {
    return events
        // Only want the bid events.
        .apply(NexmarkQueryUtil.JUST_BIDS)
        // Window the bids into sliding windows.
        .apply(
            Window.into(
                SlidingWindows.of(Duration.standardSeconds(configuration.windowSizeSec))
                    .every(Duration.standardSeconds(configuration.windowPeriodSec))))
        // Project just the auction id.
        .apply("BidToAuction", NexmarkQueryUtil.BID_TO_AUCTION)

        // Count the number of bids per auction id.
        .apply(Count.perElement())

        // We'll want to keep all auctions with the maximal number of bids.
        // Start by lifting each into a singleton list.
        // need to do so because bellow combine returns a list of auctions in the key in case of
        // equal number of bids. Combine needs to have same input type and return type.
        .apply(
            name + ".ToSingletons",
            ParDo.of(
                new DoFn<KV<Long, Long>, KV<List<Long>, Long>>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    c.output(
                        KV.of(
                            Collections.singletonList(c.element().getKey()),
                            c.element().getValue()));
                  }
                }))

        // Keep only the auction ids with the most bids.
        .apply(
            Combine.globally(
                    new Combine.BinaryCombineFn<KV<List<Long>, Long>>() {
                      @Override
                      public KV<List<Long>, Long> apply(
                          KV<List<Long>, Long> left, KV<List<Long>, Long> right) {
                        List<Long> leftBestAuctions = left.getKey();
                        long leftCount = left.getValue();
                        List<Long> rightBestAuctions = right.getKey();
                        long rightCount = right.getValue();
                        if (leftCount > rightCount) {
                          return left;
                        } else if (leftCount < rightCount) {
                          return right;
                        } else {
                          List<Long> newBestAuctions = new ArrayList<>();
                          newBestAuctions.addAll(leftBestAuctions);
                          newBestAuctions.addAll(rightBestAuctions);
                          return KV.of(newBestAuctions, leftCount);
                        }
                      }
                    })
                .withoutDefaults()
                .withFanout(configuration.fanout))

        // Project into result.
        .apply(
            name + ".Select",
            ParDo.of(
                new DoFn<KV<List<Long>, Long>, AuctionCount>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    long count = c.element().getValue();
                    for (long auction : c.element().getKey()) {
                      c.output(new AuctionCount(auction, count));
                    }
                  }
                }));
  }
}
