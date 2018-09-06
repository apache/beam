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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.NexmarkUtils;
import org.apache.beam.sdk.nexmark.model.AuctionCount;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;

/** A direct implementation of {@link Query5}. */
public class Query5Model extends NexmarkQueryModel implements Serializable {
  /** Simulator for query 5. */
  private class Simulator extends AbstractSimulator<Event, AuctionCount> {
    /** Time of bids still contributing to open windows, indexed by their auction id. */
    private final Map<Long, List<Instant>> bids;

    /** When oldest active window starts. */
    private Instant windowStart;

    public Simulator(NexmarkConfiguration configuration) {
      super(NexmarkUtils.standardEventIterator(configuration));
      bids = new TreeMap<>();
      windowStart = NexmarkUtils.BEGINNING_OF_TIME;
    }

    /**
     * Count bids per auction id for bids strictly before {@code end}. Add the auction ids with the
     * maximum number of bids to results.
     */
    private void countBids(Instant end) {
      Map<Long, Long> counts = new TreeMap<>();
      long maxCount = 0L;
      for (Map.Entry<Long, List<Instant>> entry : bids.entrySet()) {
        long count = 0L;
        long auction = entry.getKey();
        for (Instant bid : entry.getValue()) {
          if (bid.isBefore(end)) {
            count++;
          }
        }
        if (count > 0) {
          counts.put(auction, count);
          maxCount = Math.max(maxCount, count);
        }
      }
      for (Map.Entry<Long, Long> entry : counts.entrySet()) {
        long auction = entry.getKey();
        long count = entry.getValue();
        if (count == maxCount) {
          AuctionCount result = new AuctionCount(auction, count);
          addResult(TimestampedValue.of(result, end));
        }
      }
    }

    /**
     * Retire bids which are strictly before {@code cutoff}. Return true if there are any bids
     * remaining.
     */
    private boolean retireBids(Instant cutoff) {
      boolean anyRemain = false;
      for (Map.Entry<Long, List<Instant>> entry : bids.entrySet()) {
        long auction = entry.getKey();
        Iterator<Instant> itr = entry.getValue().iterator();
        while (itr.hasNext()) {
          Instant bid = itr.next();
          if (bid.isBefore(cutoff)) {
            NexmarkUtils.info("retire: %s for %s", bid, auction);
            itr.remove();
          } else {
            anyRemain = true;
          }
        }
      }
      return anyRemain;
    }

    /** Retire active windows until we've reached {@code newWindowStart}. */
    private void retireWindows(Instant newWindowStart) {
      while (!newWindowStart.equals(windowStart)) {
        NexmarkUtils.info("retiring window %s, aiming for %s", windowStart, newWindowStart);
        // Count bids in the window (windowStart, windowStart + size].
        countBids(windowStart.plus(Duration.standardSeconds(configuration.windowSizeSec)));
        // Advance the window.
        windowStart = windowStart.plus(Duration.standardSeconds(configuration.windowPeriodSec));
        // Retire bids which will never contribute to a future window.
        if (!retireBids(windowStart)) {
          // Can fast forward to latest window since no more outstanding bids.
          windowStart = newWindowStart;
        }
      }
    }

    /** Add bid to state. */
    private void captureBid(Bid bid, Instant timestamp) {
      List<Instant> existing = bids.computeIfAbsent(bid.auction, k -> new ArrayList<>());
      existing.add(timestamp);
    }

    @Override
    public void run() {
      TimestampedValue<Event> timestampedEvent = nextInput();
      if (timestampedEvent == null) {
        // Drain the remaining windows.
        retireWindows(NexmarkUtils.END_OF_TIME);
        allDone();
        return;
      }

      Event event = timestampedEvent.getValue();
      if (event.bid == null) {
        // Ignore non-bid events.
        return;
      }
      Instant timestamp = timestampedEvent.getTimestamp();
      Instant newWindowStart =
          windowStart(
              Duration.standardSeconds(configuration.windowSizeSec),
              Duration.standardSeconds(configuration.windowPeriodSec),
              timestamp);
      // Capture results from any windows we can now retire.
      retireWindows(newWindowStart);
      // Capture current bid.
      captureBid(event.bid, timestamp);
    }
  }

  public Query5Model(NexmarkConfiguration configuration) {
    super(configuration);
  }

  @Override
  public AbstractSimulator<?, ?> simulator() {
    return new Simulator(configuration);
  }

  @Override
  protected <T> Collection<String> toCollection(Iterator<TimestampedValue<T>> itr) {
    return toValue(itr);
  }
}
