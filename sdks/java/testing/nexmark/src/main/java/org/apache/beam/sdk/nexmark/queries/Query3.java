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

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.nexmark.model.NameCityStateId;
import org.apache.beam.sdk.nexmark.model.Person;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Query 3, 'Local Item Suggestion'. Who is selling in OR, ID or CA in category 10, and for what
 * auction ids? In CQL syntax:
 *
 * <pre>
 * SELECT Istream(P.name, P.city, P.state, A.id)
 * FROM Auction A [ROWS UNBOUNDED], Person P [ROWS UNBOUNDED]
 * WHERE A.seller = P.id AND (P.state = `OR' OR P.state = `ID' OR P.state = `CA') AND A.category
 * = 10;
 * </pre>
 *
 * <p>We'll implement this query to allow 'new auction' events to come before the 'new person'
 * events for the auction seller. Those auctions will be stored until the matching person is seen.
 * Then all subsequent auctions for a person will use the stored person record.
 *
 * <p>A real system would use an external system to maintain the id-to-person association.
 */
@SuppressWarnings({
  "nullness", // TODO(https://github.com/apache/beam/issues/20497)
  // TODO(https://github.com/apache/beam/issues/21230): Remove when new version of
  // errorprone is released (2.11.0)
  "unused"
})
public class Query3 extends NexmarkQueryTransform<NameCityStateId> {

  private static final Logger LOG = LoggerFactory.getLogger(Query3.class);
  private final JoinDoFn joinDoFn;

  public Query3(NexmarkConfiguration configuration) {
    super("Query3");
    joinDoFn = new JoinDoFn(name, configuration.maxAuctionsWaitingTime);
  }

  @Override
  public PCollection<NameCityStateId> expand(PCollection<Event> events) {
    PCollection<KV<Long, Event>> auctionsBySellerId =
        events
            // Only want the new auction events.
            .apply(NexmarkQueryUtil.JUST_NEW_AUCTIONS)

            // We only want auctions in category 10.
            .apply(name + ".InCategory", Filter.by(auction -> auction.category == 10))

            // Key auctions by their seller id and move to union Event type.
            .apply(
                "EventByAuctionSeller",
                ParDo.of(
                    new DoFn<Auction, KV<Long, Event>>() {
                      @ProcessElement
                      public void processElement(ProcessContext c) {
                        Event e = new Event();
                        e.newAuction = c.element();
                        c.output(KV.of(c.element().seller, e));
                      }
                    }));

    PCollection<KV<Long, Event>> personsById =
        events
            // Only want the new people events.
            .apply(NexmarkQueryUtil.JUST_NEW_PERSONS)

            // We only want people in OR, ID, CA.
            .apply(
                name + ".InState",
                Filter.by(
                    person ->
                        "OR".equals(person.state)
                            || "ID".equals(person.state)
                            || "CA".equals(person.state)))

            // Key persons by their id and move to the union event type.
            .apply(
                "EventByPersonId",
                ParDo.of(
                    new DoFn<Person, KV<Long, Event>>() {
                      @ProcessElement
                      public void processElement(ProcessContext c) {
                        Event e = new Event();
                        e.newPerson = c.element();
                        c.output(KV.of(c.element().id, e));
                      }
                    }));

    // Join auctions and people.
    return PCollectionList.of(auctionsBySellerId)
        .and(personsById)
        .apply(Flatten.pCollections())
        .apply(name + ".Join", ParDo.of(joinDoFn))
        // Project what we want.
        .apply(
            name + ".Project",
            ParDo.of(
                new DoFn<KV<Auction, Person>, NameCityStateId>() {

                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    Auction auction = c.element().getKey();
                    Person person = c.element().getValue();
                    c.output(
                        new NameCityStateId(person.name, person.city, person.state, auction.id));
                  }
                }));
  }

  /**
   * Join {@code auctions} and {@code people} by person id and emit their cross-product one pair at
   * a time.
   *
   * <p>We know a person may submit any number of auctions. Thus new person event must have the
   * person record stored in persistent state in order to match future auctions by that person.
   *
   * <p>However we know that each auction is associated with at most one person, so only need to
   * store auction records in persistent state until we have seen the corresponding person record.
   * And of course may have already seen that record.
   *
   * <p>To prevent state from accumulating over time, we cleanup buffered people or auctions after a
   * max waiting time.
   */
  private static class JoinDoFn extends DoFn<KV<Long, Event>, KV<Auction, Person>> {

    private final int maxAuctionsWaitingTime;
    private static final String AUCTIONS = "auctions";
    private static final String PERSON = "person";

    @StateId(PERSON)
    private static final StateSpec<ValueState<Person>> personSpec = StateSpecs.value(Person.CODER);

    private static final String STATE_EXPIRING = "stateExpiring";

    @StateId(AUCTIONS)
    private final StateSpec<BagState<Auction>> auctionsSpec = StateSpecs.bag(Auction.CODER);

    @TimerId(STATE_EXPIRING)
    private final TimerSpec timerSpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    private final Counter newAuctionCounter;
    private final Counter newPersonCounter;
    private final Counter newOldOutputCounter;
    private final Counter oldNewOutputCounter;
    private final Counter fatalCounter;

    private JoinDoFn(String name, int maxAuctionsWaitingTime) {
      this.maxAuctionsWaitingTime = maxAuctionsWaitingTime;
      newAuctionCounter = Metrics.counter(name, "newAuction");
      newPersonCounter = Metrics.counter(name, "newPerson");
      newOldOutputCounter = Metrics.counter(name, "newOldOutput");
      oldNewOutputCounter = Metrics.counter(name, "oldNewOutput");
      fatalCounter = Metrics.counter(name, "fatal");
    }

    @ProcessElement
    public void processElement(
        @Element KV<Long, Event> element,
        OutputReceiver<KV<Auction, Person>> output,
        @TimerId(STATE_EXPIRING) Timer timer,
        @StateId(PERSON) @AlwaysFetched ValueState<Person> personState,
        @StateId(AUCTIONS) BagState<Auction> auctionsState) {
      // We would *almost* implement this by  rewindowing into the global window and
      // running a combiner over the result. The combiner's accumulator would be the
      // state we use below. However, combiners cannot emit intermediate results, thus
      // we need to wait for the pending ReduceFn API

      Person existingPerson = personState.read();
      Event event = element.getValue();
      Instant eventTime = null;
      // Event is a union object, handle a new person or auction.
      if (event.newPerson != null) {
        Person person = event.newPerson;
        eventTime = person.dateTime;
        if (existingPerson == null) {
          newPersonCounter.inc();
          personState.write(person);
          // We've now seen the person for this person id so can flush any
          // pending auctions for the same seller id (an auction is done by only one seller).
          Iterable<Auction> pendingAuctions = auctionsState.read();
          if (pendingAuctions != null) {
            for (Auction pendingAuction : pendingAuctions) {
              oldNewOutputCounter.inc();
              output.output(KV.of(pendingAuction, person));
            }
            auctionsState.clear();
          }
        } else {
          if (person.equals(existingPerson)) {
            LOG.error("Duplicate person {}", person);
          } else {
            LOG.error("Conflicting persons {} and {}", existingPerson, person);
          }
          fatalCounter.inc();
        }
      } else if (event.newAuction != null) {
        Auction auction = event.newAuction;
        eventTime = auction.dateTime;
        newAuctionCounter.inc();
        if (existingPerson == null) {
          auctionsState.add(auction);
        } else {
          newAuctionCounter.inc();
          newOldOutputCounter.inc();
          output.output(KV.of(auction, existingPerson));
        }
      } else {
        LOG.error("Only expecting people or auctions but received {}", event);
        fatalCounter.inc();
      }
      if (eventTime != null) {
        // Set or reset the cleanup timer to clear the state.
        Instant firingTime = eventTime.plus(Duration.standardSeconds(maxAuctionsWaitingTime));
        timer.set(firingTime);
      }
    }

    @OnTimer(STATE_EXPIRING)
    public void onTimerCallback(
        OnTimerContext context,
        @StateId(PERSON) ValueState<Person> personState,
        @StateId(AUCTIONS) BagState<Auction> auctionState) {
      personState.clear();
      auctionState.clear();
    }
  }
}
