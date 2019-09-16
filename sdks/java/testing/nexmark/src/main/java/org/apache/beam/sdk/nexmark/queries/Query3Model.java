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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.NexmarkUtils;
import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.nexmark.model.NameCityStateId;
import org.apache.beam.sdk.nexmark.model.Person;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ArrayListMultimap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Multimap;
import org.joda.time.Instant;

/** A direct implementation of {@link Query3}. */
public class Query3Model extends NexmarkQueryModel<NameCityStateId> implements Serializable {
  /** Simulator for query 3. */
  private static class Simulator extends AbstractSimulator<Event, NameCityStateId> {
    /** Auctions, indexed by seller id. */
    private final Multimap<Long, Auction> newAuctions;

    /** Persons, indexed by id. */
    private final Map<Long, Person> newPersons;

    public Simulator(NexmarkConfiguration configuration) {
      super(NexmarkUtils.standardEventIterator(configuration));
      newPersons = new HashMap<>();
      newAuctions = ArrayListMultimap.create();
    }

    /** Capture new result. */
    private void addResult(Auction auction, Person person, Instant timestamp) {
      TimestampedValue<NameCityStateId> result =
          TimestampedValue.of(
              new NameCityStateId(person.name, person.city, person.state, auction.id), timestamp);
      addResult(result);
    }

    @Override
    protected void run() {
      TimestampedValue<Event> timestampedEvent = nextInput();
      if (timestampedEvent == null) {
        allDone();
        return;
      }
      Event event = timestampedEvent.getValue();
      if (event.bid != null) {
        // Ignore bid events.
        return;
      }

      Instant timestamp = timestampedEvent.getTimestamp();

      if (event.newAuction != null) {
        // Only want auctions in category 10.
        if (event.newAuction.category == 10) {
          // Join new auction with existing person, if any.
          Person person = newPersons.get(event.newAuction.seller);
          if (person != null) {
            addResult(event.newAuction, person, timestamp);
          } else {
            // Remember auction for future new person event.
            newAuctions.put(event.newAuction.seller, event.newAuction);
          }
        }
      } else {
        // Only want people in OR, ID or CA.
        if ("OR".equals(event.newPerson.state)
            || "ID".equals(event.newPerson.state)
            || "CA".equals(event.newPerson.state)) {
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
  }

  public Query3Model(NexmarkConfiguration configuration) {
    super(configuration);
  }

  @Override
  public AbstractSimulator<?, NameCityStateId> simulator() {
    return new Simulator(configuration);
  }

  @Override
  protected Collection<String> toCollection(Iterator<TimestampedValue<NameCityStateId>> itr) {
    return toValue(itr);
  }
}
