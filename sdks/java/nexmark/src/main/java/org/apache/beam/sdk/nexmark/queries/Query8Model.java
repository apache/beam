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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.NexmarkUtils;
import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.nexmark.model.IdNameReserve;
import org.apache.beam.sdk.nexmark.model.Person;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;

/** A direct implementation of {@link Query8}. */
public class Query8Model extends NexmarkQueryModel implements Serializable {
  /** Simulator for query 8. */
  private class Simulator extends AbstractSimulator<Event, IdNameReserve> {
    /** New persons seen in the current window, indexed by id. */
    private final Map<Long, Person> newPersons;

    /** New auctions seen in the current window, indexed by seller id. */
    private final Multimap<Long, Auction> newAuctions;

    /** When did the current window start. */
    private Instant windowStart;

    public Simulator(NexmarkConfiguration configuration) {
      super(NexmarkUtils.standardEventIterator(configuration));
      newPersons = new HashMap<>();
      newAuctions = ArrayListMultimap.create();
      windowStart = NexmarkUtils.BEGINNING_OF_TIME;
    }

    /** Retire all persons added in last window. */
    private void retirePersons() {
      for (Map.Entry<Long, Person> entry : newPersons.entrySet()) {
        NexmarkUtils.info("retire: %s", entry.getValue());
      }
      newPersons.clear();
    }

    /** Retire all auctions added in last window. */
    private void retireAuctions() {
      for (Map.Entry<Long, Auction> entry : newAuctions.entries()) {
        NexmarkUtils.info("retire: %s", entry.getValue());
      }
      newAuctions.clear();
    }

    /** Capture new result. */
    private void addResult(Auction auction, Person person, Instant timestamp) {
      addResult(
          TimestampedValue.of(
              new IdNameReserve(person.id, person.name, auction.reserve), timestamp));
    }

    @Override
    public void run() {
      TimestampedValue<Event> timestampedEvent = nextInput();
      if (timestampedEvent == null) {
        allDone();
        return;
      }

      Event event = timestampedEvent.getValue();
      if (event.bid != null) {
        // Ignore bid events.
        // Keep looking for next events.
        return;
      }
      Instant timestamp = timestampedEvent.getTimestamp();
      Instant newWindowStart =
          windowStart(
              Duration.standardSeconds(configuration.windowSizeSec),
              Duration.standardSeconds(configuration.windowSizeSec),
              timestamp);
      if (!newWindowStart.equals(windowStart)) {
        // Retire this window.
        retirePersons();
        retireAuctions();
        windowStart = newWindowStart;
      }

      if (event.newAuction != null) {
        // Join new auction with existing person, if any.
        Person person = newPersons.get(event.newAuction.seller);
        if (person != null) {
          addResult(event.newAuction, person, timestamp);
        } else {
          // Remember auction for future new people.
          newAuctions.put(event.newAuction.seller, event.newAuction);
        }
      } else { // event is not an auction, nor a bid, so it is a person
        // Join new person with existing auctions.
        for (Auction auction : newAuctions.get(event.newPerson.id)) {
          addResult(auction, event.newPerson, timestamp);
        }
        // We'll never need these auctions again.
        newAuctions.removeAll(event.newPerson.id);
        // Remember person for future auctions.
        newPersons.put(event.newPerson.id, event.newPerson);
      }
    }
  }

  public Query8Model(NexmarkConfiguration configuration) {
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
