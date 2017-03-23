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
package org.apache.beam.integration.nexmark.queries;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.integration.nexmark.NexmarkConfiguration;
import org.apache.beam.integration.nexmark.NexmarkQuery;
import org.apache.beam.integration.nexmark.NexmarkUtils;
import org.apache.beam.integration.nexmark.model.Auction;
import org.apache.beam.integration.nexmark.model.Event;
import org.apache.beam.integration.nexmark.model.KnownSize;
import org.apache.beam.integration.nexmark.model.NameCityStateId;
import org.apache.beam.integration.nexmark.model.Person;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.state.StateSpec;
import org.apache.beam.sdk.util.state.StateSpecs;
import org.apache.beam.sdk.util.state.ValueState;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
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
 * events for the auction seller. Those auctions will be stored until the matching person is
 * seen. Then all subsequent auctions for a person will use the stored person record.
 *
 * <p>A real system would use an external system to maintain the id-to-person association.
 */
public class Query3 extends NexmarkQuery {
  private static final Logger LOG = LoggerFactory.getLogger(Query3.class);
//  private static final StateContext GLOBAL_NAMESPACE = StateContexts.global();
  private static final StateSpec<Object, ValueState<List<Auction>>> AUCTION_LIST_CODED_TAG =
      StateSpecs.value(ListCoder.of(Auction.CODER));
  private static final StateSpec<Object, ValueState<Person>> PERSON_CODED_TAG =
      StateSpecs.value(Person.CODER);

  /**
   * Join {@code auctions} and {@code people} by person id and emit their cross-product one pair
   * at a time.
   *
   * <p>We know a person may submit any number of auctions. Thus new person event must have the
   * person record stored in persistent state in order to match future auctions by that person.
   *
   * <p>However we know that each auction is associated with at most one person, so only need
   * to store auction records in persistent state until we have seen the corresponding person
   * record. And of course may have already seen that record.
   */
  private static class JoinDoFn extends DoFn<KV<Long, CoGbkResult>, KV<Auction, Person>> {
    private final Aggregator<Long, Long> newAuctionCounter =
        createAggregator("newAuction", Sum.ofLongs());
    private final Aggregator<Long, Long> newPersonCounter =
        createAggregator("newPerson", Sum.ofLongs());
    private final Aggregator<Long, Long> newNewOutputCounter =
        createAggregator("newNewOutput", Sum.ofLongs());
    private final Aggregator<Long, Long> newOldOutputCounter =
        createAggregator("newOldOutput", Sum.ofLongs());
    private final Aggregator<Long, Long> oldNewOutputCounter =
        createAggregator("oldNewOutput", Sum.ofLongs());
    public final Aggregator<Long, Long> fatalCounter = createAggregator("fatal", Sum.ofLongs());

    @ProcessElement
    public void processElement(ProcessContext c) throws IOException {
      //TODO: This is using the internal state API. Rework to use the
      //TODO Ismael this is broken for not access to state
      // We would *almost* implement this by  rewindowing into the global window and
      // running a combiner over the result. The combiner's accumulator would be the
      // state we use below. However, combiners cannot emit intermediate results, thus
      // we need to wait for the pending ReduceFn API.
//      StateInternals<?> stateInternals = c.windowingInternals().stateInternals();
//      ValueState<Person> personState = stateInternals.state(GLOBAL_NAMESPACE, PERSON_CODED_TAG);
//      Person existingPerson = personState.read();
      Person existingPerson = null;
      if (existingPerson != null) {
        // We've already seen the new person event for this person id.
        // We can join with any new auctions on-the-fly without needing any
        // additional persistent state.
        for (Auction newAuction : c.element().getValue().getAll(AUCTION_TAG)) {
          newAuctionCounter.addValue(1L);
          newOldOutputCounter.addValue(1L);
          c.output(KV.of(newAuction, existingPerson));
        }
        return;
      }

//      ValueState<List<Auction>> auctionsState =
//          stateInternals.state(GLOBAL_NAMESPACE, AUCTION_LIST_CODED_TAG);
      Person theNewPerson = null;
      for (Person newPerson : c.element().getValue().getAll(PERSON_TAG)) {
        if (theNewPerson == null) {
          theNewPerson = newPerson;
        } else {
          if (theNewPerson.equals(newPerson)) {
            LOG.error("**** duplicate person {} ****", theNewPerson);
          } else {
            LOG.error("**** conflicting persons {} and {} ****", theNewPerson, newPerson);
          }
          fatalCounter.addValue(1L);
          continue;
        }
        newPersonCounter.addValue(1L);
        // We've now seen the person for this person id so can flush any
        // pending auctions for the same seller id.
        List<Auction> pendingAuctions = null; //auctionsState.read();
        if (pendingAuctions != null) {
          for (Auction pendingAuction : pendingAuctions) {
            oldNewOutputCounter.addValue(1L);
            c.output(KV.of(pendingAuction, newPerson));
          }
//          auctionsState.clear();
        }
        // Also deal with any new auctions.
        for (Auction newAuction : c.element().getValue().getAll(AUCTION_TAG)) {
          newAuctionCounter.addValue(1L);
          newNewOutputCounter.addValue(1L);
          c.output(KV.of(newAuction, newPerson));
        }
        // Remember this person for any future auctions.

//        personState.write(newPerson);
      }
      if (theNewPerson != null) {
        return;
      }

      // We'll need to remember the auctions until we see the corresponding
      // new person event.
      List<Auction> pendingAuctions = null; //auctionsState.read();
      if (pendingAuctions == null) {
        pendingAuctions = new ArrayList<>();
      }
      for (Auction newAuction : c.element().getValue().getAll(AUCTION_TAG)) {
        newAuctionCounter.addValue(1L);
        pendingAuctions.add(newAuction);
      }
//      auctionsState.write(pendingAuctions);
    }
  }

  private final JoinDoFn joinDoFn = new JoinDoFn();

  public Query3(NexmarkConfiguration configuration) {
    super(configuration, "Query3");
  }

  @Override
  @Nullable
  public Aggregator<Long, Long> getFatalCount() {
    return joinDoFn.fatalCounter;
  }

  private PCollection<NameCityStateId> applyTyped(PCollection<Event> events) {
    // Batch into incremental results windows.
    events = events.apply(
        Window.<Event>into(FixedWindows.of(Duration.standardSeconds(configuration.windowSizeSec))));

    PCollection<KV<Long, Auction>> auctionsBySellerId =
        events
            // Only want the new auction events.
            .apply(JUST_NEW_AUCTIONS)

            // We only want auctions in category 10.
            .apply(name + ".InCategory", Filter.by(new SerializableFunction<Auction, Boolean>() {
              @Override
              public Boolean apply(Auction auction) {
                return auction.category == 10;
              }
            }))

            // Key auctions by their seller id.
            .apply("AuctionBySeller", AUCTION_BY_SELLER);

    PCollection<KV<Long, Person>> personsById =
        events
            // Only want the new people events.
            .apply(JUST_NEW_PERSONS)

            // We only want people in OR, ID, CA.
            .apply(name + ".InState", Filter.by(new SerializableFunction<Person, Boolean>() {
              @Override
              public Boolean apply(Person person) {
                return person.state.equals("OR") || person.state.equals("ID")
                    || person.state.equals("CA");
              }
            }))

            // Key people by their id.
            .apply("PersonById", PERSON_BY_ID);

    return
      // Join auctions and people.
        // concatenate KeyedPCollections
      KeyedPCollectionTuple.of(AUCTION_TAG, auctionsBySellerId)
            .and(PERSON_TAG, personsById)
        // group auctions and persons by personId
        .apply(CoGroupByKey.<Long>create())
            .apply(name + ".Join", ParDo.of(joinDoFn))

            // Project what we want.
            .apply(name + ".Project",
                ParDo.of(new DoFn<KV<Auction, Person>, NameCityStateId>() {
                      @ProcessElement
                      public void processElement(ProcessContext c) {
                        Auction auction = c.element().getKey();
                        Person person = c.element().getValue();
                        c.output(new NameCityStateId(
                            person.name, person.city, person.state, auction.id));
                      }
                    }));
  }

  @Override
  protected PCollection<KnownSize> applyPrim(PCollection<Event> events) {
    return NexmarkUtils.castToKnownSize(name, applyTyped(events));
  }
}
