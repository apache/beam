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
import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.NexmarkUtils;
import org.apache.beam.sdk.nexmark.model.AuctionPrice;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.values.TimestampedValue;

/** A direct implementation of {@link Query2}. */
public class Query2Model extends NexmarkQueryModel<AuctionPrice> implements Serializable {
  /** Simulator for query 2. */
  private class Simulator extends AbstractSimulator<Event, AuctionPrice> {
    public Simulator(NexmarkConfiguration configuration) {
      super(NexmarkUtils.standardEventIterator(configuration));
    }

    @Override
    protected void run() {
      TimestampedValue<Event> timestampedEvent = nextInput();
      if (timestampedEvent == null) {
        allDone();
        return;
      }
      Event event = timestampedEvent.getValue();
      if (event.bid == null) {
        // Ignore non bid events.
        return;
      }
      Bid bid = event.bid;
      if (bid.auction % configuration.auctionSkip != 0) {
        // Ignore bids for auctions we don't care about.
        return;
      }
      AuctionPrice auctionPrice = new AuctionPrice(bid.auction, bid.price);
      TimestampedValue<AuctionPrice> result =
          TimestampedValue.of(auctionPrice, timestampedEvent.getTimestamp());
      addResult(result);
    }
  }

  public Query2Model(NexmarkConfiguration configuration) {
    super(configuration);
  }

  @Override
  public AbstractSimulator<?, AuctionPrice> simulator() {
    return new Simulator(configuration);
  }

  @Override
  protected Collection<String> toCollection(Iterator<TimestampedValue<AuctionPrice>> itr) {
    return toValueTimestamp(itr);
  }
}
