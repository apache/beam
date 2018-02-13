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
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.values.TimestampedValue;

/**
 * A direct implementation of {@link Query1}.
 */
public class Query1Model extends NexmarkQueryModel implements Serializable {
  /**
   * Simulator for query 1.
   */
  private static class Simulator extends AbstractSimulator<Event, Bid> {
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
        // Ignore non-bid events.
        return;
      }
      Bid bid = event.bid;
      Bid resultBid =
          new Bid(bid.auction, bid.bidder, bid.price * 89 / 100, bid.dateTime, bid.extra);
      TimestampedValue<Bid> result =
          TimestampedValue.of(resultBid, timestampedEvent.getTimestamp());
      addResult(result);
    }
  }

  public Query1Model(NexmarkConfiguration configuration) {
    super(configuration);
  }

  @Override
  public AbstractSimulator<?, ?> simulator() {
    return new Simulator(configuration);
  }

  @Override
  protected <T> Collection<String> toCollection(Iterator<TimestampedValue<T>> itr) {
    return toValueTimestampOrder(itr);
  }
}
