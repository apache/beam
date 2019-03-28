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

import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.TreeMap;
import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.NexmarkUtils;
import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.beam.sdk.nexmark.model.AuctionBid;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.beam.sdk.nexmark.model.SellerPrice;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Instant;
import org.junit.Assert;

/** A direct implementation of {@link Query6}. */
public class Query6Model extends NexmarkQueryModel<SellerPrice> implements Serializable {
  /** Simulator for query 6. */
  private static class Simulator extends AbstractSimulator<AuctionBid, SellerPrice> {
    /** The last 10 winning bids ordered by age, indexed by seller id. */
    private final Map<Long, Queue<Bid>> winningBidsPerSeller;

    /** The cumulative total of last 10 winning bid prices, indexed by seller id. */
    private final Map<Long, Long> totalWinningBidPricesPerSeller;

    private Instant lastTimestamp;

    public Simulator(NexmarkConfiguration configuration) {
      super(new WinningBidsSimulator(configuration).results());
      winningBidsPerSeller = new TreeMap<>();
      totalWinningBidPricesPerSeller = new TreeMap<>();
      lastTimestamp = BoundedWindow.TIMESTAMP_MIN_VALUE;
    }

    /** Update the per-seller running counts/sums. */
    private void captureWinningBid(Auction auction, Bid bid, Instant timestamp) {
      NexmarkUtils.info("winning auction, bid: %s, %s", auction, bid);
      Queue<Bid> queue = winningBidsPerSeller.get(auction.seller);
      if (queue == null) {
        queue = new PriorityQueue<>(10, (Bid b1, Bid b2) -> b1.dateTime.compareTo(b2.dateTime));
      }
      Long total = totalWinningBidPricesPerSeller.get(auction.seller);
      if (total == null) {
        total = 0L;
      }
      int count = queue.size();
      if (count == 10) {
        total -= queue.remove().price;
      } else {
        count += 1;
      }
      queue.add(bid);
      total += bid.price;
      winningBidsPerSeller.put(auction.seller, queue);
      totalWinningBidPricesPerSeller.put(auction.seller, total);
      TimestampedValue<SellerPrice> intermediateResult =
          TimestampedValue.of(
              new SellerPrice(auction.seller, Math.round((double) total / count)), timestamp);
      addIntermediateResult(intermediateResult);
    }

    @Override
    protected void run() {
      TimestampedValue<AuctionBid> timestampedWinningBid = nextInput();
      if (timestampedWinningBid == null) {
        for (Map.Entry<Long, Queue<Bid>> entry : winningBidsPerSeller.entrySet()) {
          long seller = entry.getKey();
          long count = entry.getValue().size();
          long total = totalWinningBidPricesPerSeller.get(seller);
          addResult(
              TimestampedValue.of(
                  new SellerPrice(seller, Math.round((double) total / count)), lastTimestamp));
        }
        allDone();
        return;
      }

      lastTimestamp = timestampedWinningBid.getTimestamp();
      captureWinningBid(
          timestampedWinningBid.getValue().auction,
          timestampedWinningBid.getValue().bid,
          lastTimestamp);
    }
  }

  public Query6Model(NexmarkConfiguration configuration) {
    super(configuration);
  }

  @Override
  public AbstractSimulator<?, SellerPrice> simulator() {
    return new Simulator(configuration);
  }

  @Override
  protected Iterable<TimestampedValue<SellerPrice>> relevantResults(
      Iterable<TimestampedValue<SellerPrice>> results) {
    // Find the last (in processing time) reported average price for each seller.
    Map<Long, TimestampedValue<SellerPrice>> finalAverages = new TreeMap<>();
    for (TimestampedValue<SellerPrice> obj : results) {
      Assert.assertTrue("have SellerPrice", obj.getValue() instanceof SellerPrice);
      SellerPrice sellerPrice = (SellerPrice) obj.getValue();
      finalAverages.put(sellerPrice.seller, TimestampedValue.of(sellerPrice, obj.getTimestamp()));
    }
    return finalAverages.values();
  }

  @Override
  protected Collection<String> toCollection(Iterator<TimestampedValue<SellerPrice>> itr) {
    return toValue(itr);
  }
}
