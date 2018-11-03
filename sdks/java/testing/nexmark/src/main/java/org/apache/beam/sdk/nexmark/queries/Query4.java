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

import org.apache.beam.sdk.nexmark.Monitor;
import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.beam.sdk.nexmark.model.AuctionBid;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.beam.sdk.nexmark.model.CategoryPrice;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.Mean;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

/**
 * Query 4, 'Average Price for a Category'. Select the average of the wining bid prices for all
 * closed auctions in each category. In CQL syntax:
 *
 * <pre>{@code
 * SELECT Istream(AVG(Q.final))
 * FROM Category C, (SELECT Rstream(MAX(B.price) AS final, A.category)
 *                   FROM Auction A [ROWS UNBOUNDED], Bid B [ROWS UNBOUNDED]
 *                   WHERE A.id=B.auction AND B.datetime < A.expires AND A.expires < CURRENT_TIME
 *                   GROUP BY A.id, A.category) Q
 * WHERE Q.category = C.id
 * GROUP BY C.id;
 * }</pre>
 *
 * <p>For extra spiciness our implementation differs slightly from the above:
 *
 * <ul>
 *   <li>We select both the average winning price and the category.
 *   <li>We don't bother joining with a static category table, since it's contents are never used.
 *   <li>We only consider bids which are above the auction's reserve price.
 *   <li>We accept the highest-price, earliest valid bid as the winner.
 *   <li>We calculate the averages oven a sliding window of size {@code windowSizeSec} and period
 *       {@code windowPeriodSec}.
 * </ul>
 */
public class Query4 extends NexmarkQueryTransform<CategoryPrice> {
  private final Monitor<AuctionBid> winningBidsMonitor;
  private final NexmarkConfiguration configuration;

  public Query4(NexmarkConfiguration configuration) {
    super("Query4");
    this.configuration = configuration;
    winningBidsMonitor = new Monitor<>(name + ".WinningBids", "winning");
  }

  @Override
  public PCollection<CategoryPrice> expand(PCollection<Event> events) {
    PCollection<AuctionBid> winningBids =
        events
            .apply(Filter.by(new AuctionOrBid()))
            // Find the winning bid for each closed auction.
            .apply(new WinningBids(name + ".WinningBids", configuration));

    // Monitor winning bids
    winningBids =
        winningBids.apply(name + ".WinningBidsMonitor", winningBidsMonitor.getTransform());

    return winningBids
        // Key the winning bid price by the auction category.
        .apply(
            name + ".Rekey",
            ParDo.of(
                new DoFn<AuctionBid, KV<Long, Long>>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    Auction auction = c.element().auction;
                    Bid bid = c.element().bid;
                    c.output(KV.of(auction.category, bid.price));
                  }
                }))

        // Re-window so we can calculate a sliding average
        .apply(
            Window.into(
                SlidingWindows.of(Duration.standardSeconds(configuration.windowSizeSec))
                    .every(Duration.standardSeconds(configuration.windowPeriodSec))))

        // Find the average of the winning bids for each category.
        // Make sure we share the work for each category between workers.
        .apply(Mean.<Long, Long>perKey().withHotKeyFanout(configuration.fanout))

        // For testing against Query4Model, capture which results are 'final'.
        .apply(
            name + ".Project",
            ParDo.of(
                new DoFn<KV<Long, Double>, CategoryPrice>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    c.output(
                        new CategoryPrice(
                            c.element().getKey(),
                            Math.round(c.element().getValue()),
                            c.pane().isLast()));
                  }
                }));
  }
}
