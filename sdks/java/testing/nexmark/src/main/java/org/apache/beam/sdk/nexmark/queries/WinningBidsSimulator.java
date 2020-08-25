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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.NexmarkUtils;
import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.beam.sdk.nexmark.model.AuctionBid;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.TimestampedValue;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/** A simulator of the {@code WinningBids} query. */
public class WinningBidsSimulator extends AbstractSimulator<Event, AuctionBid> {
  /** Auctions currently still open, indexed by auction id. */
  private final Map<Long, Auction> openAuctions;

  /** The ids of auctions known to be closed. */
  private final Set<Long> closedAuctions;

  /** Current best valid bids for open auctions, indexed by auction id. */
  private final Map<Long, Bid> bestBids;

  /** Bids for auctions we havn't seen yet. */
  private final List<Bid> bidsWithoutAuctions;

  /** Timestamp of last new auction or bid event (ms since epoch). */
  private Instant lastTimestamp;

  public WinningBidsSimulator(NexmarkConfiguration configuration) {
    super(NexmarkUtils.standardEventIterator(configuration));
    openAuctions = new TreeMap<>();
    closedAuctions = new TreeSet<>();
    bestBids = new TreeMap<>();
    bidsWithoutAuctions = new ArrayList<>();
    lastTimestamp = BoundedWindow.TIMESTAMP_MIN_VALUE;
  }

  /**
   * Try to account for {@code bid} in state. Return true if bid has now been accounted for by
   * {@code bestBids}.
   */
  private boolean captureBestBid(Bid bid, boolean shouldLog) {
    if (closedAuctions.contains(bid.auction)) {
      // Ignore bids for known, closed auctions.
      if (shouldLog) {
        NexmarkUtils.info("closed auction: %s", bid);
      }
      return true;
    }
    Auction auction = openAuctions.get(bid.auction);
    if (auction == null) {
      // We don't have an auction for this bid yet, so can't determine if it is
      // winning or not.
      if (shouldLog) {
        NexmarkUtils.info("pending auction: %s", bid);
      }
      return false;
    }
    if (bid.price < auction.reserve) {
      // Bid price is too low.
      if (shouldLog) {
        NexmarkUtils.info("below reserve: %s", bid);
      }
      return true;
    }
    Bid existingBid = bestBids.get(bid.auction);
    if (existingBid == null || Bid.PRICE_THEN_DESCENDING_TIME.compare(existingBid, bid) < 0) {
      // We've found a (new) best bid for a known auction.
      bestBids.put(bid.auction, bid);
      if (shouldLog) {
        NexmarkUtils.info("new winning bid: %s", bid);
      }
    } else {
      if (shouldLog) {
        NexmarkUtils.info("ignoring low bid: %s", bid);
      }
    }
    return true;
  }

  /** Try to match bids without auctions to auctions. */
  private void flushBidsWithoutAuctions() {
    Iterator<Bid> itr = bidsWithoutAuctions.iterator();
    while (itr.hasNext()) {
      Bid bid = itr.next();
      if (captureBestBid(bid, false)) {
        NexmarkUtils.info("bid now accounted for: %s", bid);
        itr.remove();
      }
    }
  }

  /**
   * Return the next winning bid for an expired auction relative to {@code timestamp}. Return null
   * if no more winning bids, in which case all expired auctions will have been removed from our
   * state. Retire auctions in order of expire time.
   */
  private @Nullable TimestampedValue<AuctionBid> nextWinningBid(Instant timestamp) {
    Map<Instant, List<Long>> toBeRetired = new TreeMap<>();
    for (Map.Entry<Long, Auction> entry : openAuctions.entrySet()) {
      if (entry.getValue().expires.compareTo(timestamp) <= 0) {
        List<Long> idsAtTime =
            toBeRetired.computeIfAbsent(entry.getValue().expires, k -> new ArrayList<>());
        idsAtTime.add(entry.getKey());
      }
    }
    for (Map.Entry<Instant, List<Long>> entry : toBeRetired.entrySet()) {
      for (long id : entry.getValue()) {
        Auction auction = openAuctions.get(id);
        NexmarkUtils.info("retiring auction: %s", auction);
        openAuctions.remove(id);
        Bid bestBid = bestBids.get(id);
        if (bestBid != null) {
          TimestampedValue<AuctionBid> result =
              TimestampedValue.of(new AuctionBid(auction, bestBid), auction.expires);
          NexmarkUtils.info("winning: %s", result);
          return result;
        }
      }
    }
    return null;
  }

  @Override
  protected void run() {
    if (lastTimestamp.compareTo(BoundedWindow.TIMESTAMP_MIN_VALUE) > 0) {
      // We may have finally seen the auction a bid was intended for.
      flushBidsWithoutAuctions();
      TimestampedValue<AuctionBid> result = nextWinningBid(lastTimestamp);
      if (result != null) {
        addResult(result);
        return;
      }
    }

    TimestampedValue<Event> timestampedEvent = nextInput();
    if (timestampedEvent == null) {
      // No more events. Flush any still open auctions.
      TimestampedValue<AuctionBid> result = nextWinningBid(BoundedWindow.TIMESTAMP_MAX_VALUE);
      if (result == null) {
        // We are done.
        allDone();
        return;
      }
      addResult(result);
      return;
    }

    Event event = timestampedEvent.getValue();
    if (event.newPerson != null) {
      // Ignore new person events.
      return;
    }

    lastTimestamp = timestampedEvent.getTimestamp();
    if (event.newAuction != null) {
      // Add this new open auction to our state.
      openAuctions.put(event.newAuction.id, event.newAuction);
    } else {
      if (!captureBestBid(event.bid, true)) {
        // We don't know what to do with this bid yet.
        NexmarkUtils.info("bid not yet accounted for: %s", event.bid);
        bidsWithoutAuctions.add(event.bid);
      }
    }
    // Keep looking for winning bids.
  }
}
