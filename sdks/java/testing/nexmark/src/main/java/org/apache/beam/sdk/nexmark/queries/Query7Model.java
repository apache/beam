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
import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.NexmarkUtils;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;

/** A direct implementation of {@link Query7}. */
public class Query7Model extends NexmarkQueryModel<Bid> implements Serializable {
  /** Simulator for query 7. */
  private class Simulator extends AbstractSimulator<Event, Bid> {
    /** Bids with highest bid price seen in the current window. */
    private final List<Bid> highestBids;

    /** When current window started. */
    private Instant windowStart;

    private Instant lastTimestamp;

    public Simulator(NexmarkConfiguration configuration) {
      super(NexmarkUtils.standardEventIterator(configuration));
      highestBids = new ArrayList<>();
      windowStart = NexmarkUtils.BEGINNING_OF_TIME;
      lastTimestamp = BoundedWindow.TIMESTAMP_MIN_VALUE;
    }

    /** Transfer the currently winning bids into results and retire them. */
    private void retireWindow(Instant timestamp) {
      for (Bid bid : highestBids) {
        addResult(TimestampedValue.of(bid, timestamp));
      }
      highestBids.clear();
    }

    /** Keep just the highest price bid. */
    private void captureBid(Bid bid) {
      Iterator<Bid> itr = highestBids.iterator();
      boolean isWinning = true;
      while (itr.hasNext()) {
        Bid existingBid = itr.next();
        if (existingBid.price > bid.price) {
          isWinning = false;
          break;
        }
        NexmarkUtils.info("smaller price: %s", existingBid);
        itr.remove();
      }
      if (isWinning) {
        NexmarkUtils.info("larger price: %s", bid);
        highestBids.add(bid);
      }
    }

    @Override
    protected void run() {
      TimestampedValue<Event> timestampedEvent = nextInput();
      if (timestampedEvent == null) {
        // Capture all remaining bids in results.
        retireWindow(lastTimestamp);
        allDone();
        return;
      }

      Event event = timestampedEvent.getValue();
      if (event.bid == null) {
        // Ignore non-bid events.
        return;
      }
      lastTimestamp = timestampedEvent.getTimestamp();
      Instant newWindowStart =
          windowStart(
              Duration.standardSeconds(configuration.windowSizeSec),
              Duration.standardSeconds(configuration.windowSizeSec),
              lastTimestamp);
      if (!newWindowStart.equals(windowStart)) {
        // Capture highest priced bids in current window and retire it.
        retireWindow(lastTimestamp);
        windowStart = newWindowStart;
      }
      // Keep only the highest bids.
      captureBid(event.bid);
    }
  }

  public Query7Model(NexmarkConfiguration configuration) {
    super(configuration);
  }

  @Override
  public AbstractSimulator<?, Bid> simulator() {
    return new Simulator(configuration);
  }

  @Override
  protected Collection<String> toCollection(Iterator<TimestampedValue<Bid>> itr) {
    return toValue(itr);
  }
}
