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
import java.util.List;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.nexmark.model.NameCityStateId;
import org.apache.beam.sdk.nexmark.model.Person;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
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
public class Query3 extends NexmarkQueryTransform<NameCityStateId> {

  private static final Logger LOG = LoggerFactory.getLogger(Query3.class);
  private final JoinDoFn joinDoFn;

  public Query3(NexmarkConfiguration configuration) {
    super("Query3");
    joinDoFn = new JoinDoFn(name, configuration.maxAuctionsWaitingTime);
  }

  @Override
  public PCollection<NameCityStateId> expand(PCollection<Event> events) {
    int numEventsInPane = 30;

    PCollection<Event> eventsWindowed =
        events.apply(
            Window.<Event>into(new GlobalWindows())
                .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(numEventsInPane)))
                .discardingFiredPanes()
                .withAllowedLateness(Duration.ZERO));
    PCollection<KV<Long, Auction>> auctionsBySellerId =
        eventsWindowed
            // Only want the new auction events.
            .apply(NexmarkQueryUtil.JUST_NEW_AUCTIONS)

            // We only want auctions in category 10.
            .apply(name + ".InCategory", Filter.by(auction -> auction.category == 10))

            // Key auctions by their seller id.
            .apply("AuctionBySeller", NexmarkQueryUtil.AUCTION_BY_SELLER);

    PCollection<KV<Long, Person>> personsById =
        eventsWindowed
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

            // Key people by their id.
            .apply("PersonById", NexmarkQueryUtil.PERSON_BY_ID);

    return
    // Join auctions and people.
    // concatenate KeyedPCollections
    KeyedPCollectionTuple.of(NexmarkQueryUtil.AUCTION_TAG, auctionsBySellerId)
        .and(NexmarkQueryUtil.PERSON_TAG, personsById)
        // group auctions and persons by personId
        .apply(CoGroupByKey.create())
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
   */
  private static class JoinDoFn extends DoFn<KV<Long, CoGbkResult>, KV<Auction, Person>> {

    private final int maxAuctionsWaitingTime;
    private static final String AUCTIONS = "auctions";
    private static final String PERSON = "person";

    @StateId(PERSON)
    private static final StateSpec<ValueState<Person>> personSpec = StateSpecs.value(Person.CODER);

    private static final String PERSON_STATE_EXPIRING = "personStateExpiring";

    @StateId(AUCTIONS)
    private final StateSpec<ValueState<List<Auction>>> auctionsSpec =
        StateSpecs.value(ListCoder.of(Auction.CODER));

    @TimerId(PERSON_STATE_EXPIRING)
    private final TimerSpec timerSpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    // Used to refer the metrics namespace
    private final String name;

    private final Counter newAuctionCounter;
    private final Counter newPersonCounter;
    private final Counter newNewOutputCounter;
    private final Counter newOldOutputCounter;
    private final Counter oldNewOutputCounter;
    private final Counter fatalCounter;

    private JoinDoFn(String name, int maxAuctionsWaitingTime) {
      this.name = name;
      this.maxAuctionsWaitingTime = maxAuctionsWaitingTime;
      newAuctionCounter = Metrics.counter(name, "newAuction");
      newPersonCounter = Metrics.counter(name, "newPerson");
      newNewOutputCounter = Metrics.counter(name, "newNewOutput");
      newOldOutputCounter = Metrics.counter(name, "newOldOutput");
      oldNewOutputCounter = Metrics.counter(name, "oldNewOutput");
      fatalCounter = Metrics.counter(name, "fatal");
    }

    @ProcessElement
    public void processElement(
        ProcessContext c,
        @TimerId(PERSON_STATE_EXPIRING) Timer timer,
        @StateId(PERSON) ValueState<Person> personState,
        @StateId(AUCTIONS) ValueState<List<Auction>> auctionsState) {
      // We would *almost* implement this by  rewindowing into the global window and
      // running a combiner over the result. The combiner's accumulator would be the
      // state we use below. However, combiners cannot emit intermediate results, thus
      // we need to wait for the pending ReduceFn API.

      Person existingPerson = personState.read();
      if (existingPerson != null) {
        // We've already seen the new person event for this person id.
        // We can join with any new auctions on-the-fly without needing any
        // additional persistent state.
        for (Auction newAuction : c.element().getValue().getAll(NexmarkQueryUtil.AUCTION_TAG)) {
          newAuctionCounter.inc();
          newOldOutputCounter.inc();
          c.output(KV.of(newAuction, existingPerson));
        }
        return;
      }

      Person theNewPerson = null;
      for (Person newPerson : c.element().getValue().getAll(NexmarkQueryUtil.PERSON_TAG)) {
        if (theNewPerson == null) {
          theNewPerson = newPerson;
        } else {
          if (theNewPerson.equals(newPerson)) {
            LOG.error("Duplicate person {}", theNewPerson);
          } else {
            LOG.error("Conflicting persons {} and {}", theNewPerson, newPerson);
          }
          fatalCounter.inc();
          continue;
        }
        newPersonCounter.inc();
        // We've now seen the person for this person id so can flush any
        // pending auctions for the same seller id (an auction is done by only one seller).
        List<Auction> pendingAuctions = auctionsState.read();
        if (pendingAuctions != null) {
          for (Auction pendingAuction : pendingAuctions) {
            oldNewOutputCounter.inc();
            c.output(KV.of(pendingAuction, newPerson));
          }
          auctionsState.clear();
        }
        // Also deal with any new auctions.
        for (Auction newAuction : c.element().getValue().getAll(NexmarkQueryUtil.AUCTION_TAG)) {
          newAuctionCounter.inc();
          newNewOutputCounter.inc();
          c.output(KV.of(newAuction, newPerson));
        }
        // Remember this person for any future auctions.
        personState.write(newPerson);
        // set a time out to clear this state
        Instant firingTime =
            new Instant(newPerson.dateTime).plus(Duration.standardSeconds(maxAuctionsWaitingTime));
        timer.set(firingTime);
      }
      if (theNewPerson != null) {
        return;
      }

      // We'll need to remember the auctions until we see the corresponding
      // new person event.
      List<Auction> pendingAuctions = auctionsState.read();
      if (pendingAuctions == null) {
        pendingAuctions = new ArrayList<>();
      }
      for (Auction newAuction : c.element().getValue().getAll(NexmarkQueryUtil.AUCTION_TAG)) {
        newAuctionCounter.inc();
        pendingAuctions.add(newAuction);
      }
      auctionsState.write(pendingAuctions);
    }

    @OnTimer(PERSON_STATE_EXPIRING)
    public void onTimerCallback(
        OnTimerContext context, @StateId(PERSON) ValueState<Person> personState) {
      personState.clear();
    }
  }
}
