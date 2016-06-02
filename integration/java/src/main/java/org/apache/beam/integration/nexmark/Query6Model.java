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

import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.TimestampedValue;

import org.joda.time.Instant;
import org.junit.Assert;

import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/**
 * A direct implementation of {@link Query6}.
 */
public class Query6Model extends NexmarkQueryModel implements Serializable {
  /**
   * Simulator for query 6.
   */
  private static class Simulator extends AbstractSimulator<AuctionBid, SellerPrice> {
    /** The cumulative count of winning bids, indexed by seller id. */
    private final Map<Long, Long> numWinningBidsPerSeller;

    /** The cumulative total of winning bid prices, indexed by seller id. */
    private final Map<Long, Long> totalWinningBidPricesPerSeller;

    private Instant lastTimestamp;

    public Simulator(NexmarkConfiguration configuration) {
      super(new WinningBidsSimulator(configuration).results());
      numWinningBidsPerSeller = new TreeMap<>();
      totalWinningBidPricesPerSeller = new TreeMap<>();
      lastTimestamp = BoundedWindow.TIMESTAMP_MIN_VALUE;
    }

    /**
     * Update the per-seller running counts/sums.
     */
    private void captureWinningBid(Auction auction, Bid bid, Instant timestamp) {
      NexmarkUtils.info("winning auction, bid: %s, %s", auction, bid);
      Long count = numWinningBidsPerSeller.get(auction.seller);
      if (count == null) {
        count = 1L;
      } else {
        count += 1;
      }
      numWinningBidsPerSeller.put(auction.seller, count);
      Long total = totalWinningBidPricesPerSeller.get(auction.seller);
      if (total == null) {
        total = bid.price;
      } else {
        total += bid.price;
      }
      totalWinningBidPricesPerSeller.put(auction.seller, total);
      TimestampedValue<SellerPrice> intermediateResult = TimestampedValue.of(
          new SellerPrice(auction.seller, Math.round((double) total / count)), timestamp);
      addIntermediateResult(intermediateResult);
    }


    @Override
    protected void run() {
      TimestampedValue<AuctionBid> timestampedWinningBid = nextInput();
      if (timestampedWinningBid == null) {
        for (long seller : numWinningBidsPerSeller.keySet()) {
          long count = numWinningBidsPerSeller.get(seller);
          long total = totalWinningBidPricesPerSeller.get(seller);
          addResult(TimestampedValue.of(
              new SellerPrice(seller, Math.round((double) total / count)), lastTimestamp));
        }
        allDone();
        return;
      }

      lastTimestamp = timestampedWinningBid.getTimestamp();
      captureWinningBid(timestampedWinningBid.getValue().auction,
          timestampedWinningBid.getValue().bid, lastTimestamp);
    }
  }

  public Query6Model(NexmarkConfiguration configuration) {
    super(configuration);
  }

  @Override
  public AbstractSimulator<?, ?> simulator() {
    return new Simulator(configuration);
  }

  @Override
  protected Iterable<TimestampedValue<KnownSize>> relevantResults(
      Iterable<TimestampedValue<KnownSize>> results) {
    // Find the last (in processing time) reported average price for each seller.
    Map<Long, TimestampedValue<KnownSize>> finalAverages = new TreeMap<>();
    for (TimestampedValue<KnownSize> obj : results) {
      Assert.assertTrue("have SellerPrice", obj.getValue() instanceof SellerPrice);
      SellerPrice sellerPrice = (SellerPrice) obj.getValue();
      finalAverages.put(
          sellerPrice.seller, TimestampedValue.of((KnownSize) sellerPrice, obj.getTimestamp()));
    }
    return finalAverages.values();
  }

  @Override
  protected <T> Collection<String> toCollection(Iterator<TimestampedValue<T>> itr) {
    return toValue(itr);
  }
}
