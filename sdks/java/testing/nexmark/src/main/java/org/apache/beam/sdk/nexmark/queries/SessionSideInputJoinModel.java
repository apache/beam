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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.NexmarkUtils;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Ordering;
import org.joda.time.Instant;

/** A direct implementation of {@link SessionSideInputJoin}. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class SessionSideInputJoinModel extends NexmarkQueryModel<Bid> {

  /** Simulator for SESSION_SIDE_INPUT_JOIN query. */
  private static class Simulator extends AbstractSimulator<Event, Bid> {
    private final NexmarkConfiguration configuration;

    /**
     * Active session for each bidder. Data is generated in-order so one suffices. Flushed when the
     * simulator terminates.
     */
    private final Map<Long, List<TimestampedValue<Event>>> activeSessions;

    public Simulator(NexmarkConfiguration configuration) {
      super(NexmarkUtils.standardEventIterator(configuration));
      this.configuration = configuration;
      this.activeSessions = new HashMap<>();
    }

    @Override
    protected void run() {
      TimestampedValue<Event> timestampedEvent = nextInput();
      if (timestampedEvent == null) {
        for (Long bidder : ImmutableSet.copyOf(activeSessions.keySet())) {
          flushSession(bidder);
        }
        allDone();
        return;
      }
      Event event = timestampedEvent.getValue();
      if (event.bid == null) {
        // Ignore non-bid events.
        return;
      }

      List<TimestampedValue<Event>> activeSession = activeSessions.get(event.bid.bidder);
      if (activeSession == null) {
        beginSession(timestampedEvent);
      } else if (timestampedEvent
          .getTimestamp()
          .isAfter(
              activeSession
                  .get(activeSession.size() - 1)
                  .getTimestamp()
                  .plus(configuration.sessionGap))) {
        flushSession(event.bid.bidder);
        beginSession(timestampedEvent);
      } else {
        activeSession.add(timestampedEvent);
      }
    }

    private void beginSession(TimestampedValue<Event> timestampedEvent) {
      checkState(activeSessions.get(timestampedEvent.getValue().bid.bidder) == null);
      List<TimestampedValue<Event>> session = new ArrayList<>();
      session.add(timestampedEvent);
      activeSessions.put(timestampedEvent.getValue().bid.bidder, session);
    }

    private void flushSession(long bidder) {
      List<TimestampedValue<Event>> session = activeSessions.get(bidder);
      checkState(session != null);

      Instant sessionStart =
          Ordering.<Instant>natural()
              .min(
                  session.stream()
                      .<Instant>map(tsv -> tsv.getTimestamp())
                      .collect(Collectors.toList()));

      Instant sessionEnd =
          Ordering.<Instant>natural()
              .max(
                  session.stream()
                      .<Instant>map(tsv -> tsv.getTimestamp())
                      .collect(Collectors.toList()))
              .plus(configuration.sessionGap);

      for (TimestampedValue<Event> timestampedEvent : session) {
        // Join to the side input is always a string representation of the id being looked up
        Bid bid = timestampedEvent.getValue().bid;
        Bid resultBid =
            new Bid(
                bid.auction,
                bid.bidder,
                bid.price,
                bid.dateTime,
                String.format(
                    "%d:%s:%s",
                    bid.bidder % configuration.sideInputRowCount, sessionStart, sessionEnd));
        TimestampedValue<Bid> result =
            TimestampedValue.of(resultBid, timestampedEvent.getTimestamp());
        addResult(result);
      }
      activeSessions.remove(bidder);
    }
  }

  public SessionSideInputJoinModel(NexmarkConfiguration configuration) {
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
