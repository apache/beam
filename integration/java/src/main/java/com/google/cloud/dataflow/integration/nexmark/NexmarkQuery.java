/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.integration.nexmark;

import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.Filter;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.TimestampedValue;
import com.google.cloud.dataflow.sdk.values.TupleTag;

import org.joda.time.Instant;

import javax.annotation.Nullable;

/**
 * Base class for the eight 'NEXMark' queries. Supplies some fragments common to
 * multiple queries.
 */
public abstract class NexmarkQuery
    extends PTransform<PCollection<Event>, PCollection<TimestampedValue<KnownSize>>> {
  protected static final TupleTag<Auction> AUCTION_TAG = new TupleTag<>("auctions");
  protected static final TupleTag<Bid> BID_TAG = new TupleTag<>("bids");
  protected static final TupleTag<Person> PERSON_TAG = new TupleTag<>("person");

  /** Predicate to detect a new person event. */
  protected static final SerializableFunction<Event, Boolean> IS_NEW_PERSON =
      new SerializableFunction<Event, Boolean>() {
        @Override
        public Boolean apply(Event event) {
          return event.newPerson != null;
        }
      };

  /** DoFn to convert a new person event to a person. */
  protected static final DoFn<Event, Person> AS_PERSON = new DoFn<Event, Person>() {
    @Override
    public void processElement(ProcessContext c) {
      c.output(c.element().newPerson);
    }
  };

  /** Predicate to detect a new auction event. */
  protected static final SerializableFunction<Event, Boolean> IS_NEW_AUCTION =
      new SerializableFunction<Event, Boolean>() {
        @Override
        public Boolean apply(Event event) {
          return event.newAuction != null;
        }
      };

  /** DoFn to convert a new auction event to an auction. */
  protected static final DoFn<Event, Auction> AS_AUCTION = new DoFn<Event, Auction>() {
    @Override
    public void processElement(ProcessContext c) {
      c.output(c.element().newAuction);
    }
  };

  /** Predicate to detect a new bid event. */
  protected static final SerializableFunction<Event, Boolean> IS_BID =
      new SerializableFunction<Event, Boolean>() {
        @Override
        public Boolean apply(Event event) {
          return event.bid != null;
        }
      };

  /** DoFn to convert a bid event to a bid. */
  protected static final DoFn<Event, Bid> AS_BID = new DoFn<Event, Bid>() {
    @Override
    public void processElement(ProcessContext c) {
      c.output(c.element().bid);
    }
  };

  /** Transform to key each person by their id. */
  protected static final ParDo.Bound<Person, KV<Long, Person>> PERSON_BY_ID =
      ParDo.named("PersonById")
          .of(new DoFn<Person, KV<Long, Person>>() {
            @Override
            public void processElement(ProcessContext c) {
              c.output(KV.of(c.element().id, c.element()));
            }
          });

  /** Transform to key each auction by its id. */
  protected static final ParDo.Bound<Auction, KV<Long, Auction>> AUCTION_BY_ID =
      ParDo.named("AuctionById")
          .of(new DoFn<Auction, KV<Long, Auction>>() {
            @Override
            public void processElement(ProcessContext c) {
              c.output(KV.of(c.element().id, c.element()));
            }
          });

  /** Transform to key each auction by its seller id. */
  protected static final ParDo.Bound<Auction, KV<Long, Auction>> AUCTION_BY_SELLER =
      ParDo.named("AuctionBySeller")
          .of(new DoFn<Auction, KV<Long, Auction>>() {
            @Override
            public void processElement(ProcessContext c) {
              c.output(KV.of(c.element().seller, c.element()));
            }
          });

  /** Transform to key each bid by it's auction id. */
  protected static final ParDo.Bound<Bid, KV<Long, Bid>> BID_BY_AUCTION =
      ParDo.named("BidByAuction")
          .of(new DoFn<Bid, KV<Long, Bid>>() {
            @Override
            public void processElement(ProcessContext c) {
              c.output(KV.of(c.element().auction, c.element()));
            }
          });

  /** Transform to project the auction id from each bid.*/
  protected static final ParDo.Bound<Bid, Long> BID_TO_AUCTION =
      ParDo.named("BidToAuction")
          .of(new DoFn<Bid, Long>() {
            @Override
            public void processElement(ProcessContext c) {
              c.output(c.element().auction);
            }
          });

  /** Transform to project the price from each bid. */
  protected static final ParDo.Bound<Bid, Long> BID_TO_PRICE =
      ParDo.named("BidToPrice")
          .of(new DoFn<Bid, Long>() {
            @Override
            public void processElement(ProcessContext c) {
              c.output(c.element().price);
            }
          });

  public static final DoFn<Event, Event> EVENT_TIMESTAMP_FROM_DATA = new DoFn<Event, Event> () {
          @Override
          public void processElement(ProcessContext c) {
            Event e = c.element();
            if (e.bid != null) {
              c.outputWithTimestamp(e, new Instant(e.bid.dateTime));
            } else if (e.newPerson != null) {
              c.outputWithTimestamp(e, new Instant(e.newPerson.dateTime));
            } else if (e.newAuction != null) {
              c.outputWithTimestamp(e, new Instant(e.newAuction.dateTime));
            }
          }
        };


  /**
   * Transform to filter for just the new auction events.
   */
  protected static final PTransform<PCollection<Event>, PCollection<Auction>> JUST_NEW_AUCTIONS =
      new PTransform<PCollection<Event>, PCollection<Auction>>("justNewAuctions") {
        @Override
        public PCollection<Auction> apply(PCollection<Event> input) {
          return input.apply(Filter.byPredicate(IS_NEW_AUCTION).named("IsAuction"))
              .apply(ParDo.named("AsAuction").of(AS_AUCTION));
        }
      };

  /**
   * Transform to filter for just the new person events.
   */
  protected static final PTransform<PCollection<Event>, PCollection<Person>> JUST_NEW_PERSONS =
      new PTransform<PCollection<Event>, PCollection<Person>>("justNewPersons") {
        @Override
        public PCollection<Person> apply(PCollection<Event> input) {
          return input.apply(Filter.byPredicate(IS_NEW_PERSON).named("IsPerson"))
              .apply(ParDo.named("AsPerson").of(AS_PERSON));
        }
      };

  /**
   * Transform to filter for just the bid events.
   */
  protected static final PTransform<PCollection<Event>, PCollection<Bid>> JUST_BIDS =
      new PTransform<PCollection<Event>, PCollection<Bid>>("justBids") {
        @Override
        public PCollection<Bid> apply(PCollection<Event> input) {
          return input.apply(Filter.byPredicate(IS_BID).named("IsBid"))
              .apply(ParDo.named("AsBid").of(AS_BID));
        }
      };

  protected final NexmarkConfiguration configuration;
  public final Monitor<Event> eventMonitor;
  public final Monitor<KnownSize> resultMonitor;
  public final Monitor<Event> endOfStreamMonitor;

  protected NexmarkQuery(NexmarkConfiguration configuration, String name) {
    super(name);
    this.configuration = configuration;
    if (configuration.debug) {
      eventMonitor = new Monitor<Event>(name + ".Events", "event");
      resultMonitor = new Monitor<KnownSize>(name + ".Results", "result");
      endOfStreamMonitor = new Monitor<Event>(name + ".EndOfStream", "end");
    } else {
      eventMonitor = null;
      resultMonitor = null;
      endOfStreamMonitor = null;
    }
  }

  /**
   * Return the aggregator which counts fatal errors in this query. Return null if no such
   * aggregator.
   */
  @Nullable
  public Aggregator<Long, Long> getFatalCount() {
    return null;
  }

  /**
   * Implement the actual query. All we know about the result is it has a known encoded size.
   */
  protected abstract PCollection<KnownSize> applyPrim(PCollection<Event> events);

  @Override
  public PCollection<TimestampedValue<KnownSize>> apply(PCollection<Event> events) {

    if (configuration.debug) {
    events =
        events
            // Monitor events as they go by.
            .apply(eventMonitor.getTransform())
            // Count each type of event.
            .apply(NexmarkUtils.snoop(name));
    }

    if (configuration.cpuDelayMs > 0) {
      // Slow down by pegging one core at 100%.
      events = events.apply(NexmarkUtils.<Event>cpuDelay(name, configuration.cpuDelayMs));
    }

    if (configuration.diskBusyBytes > 0) {
      // Slow down by forcing bytes to durable store.
      events = events.apply(NexmarkUtils.<Event>diskBusy(name, configuration.diskBusyBytes));
    }

    // Run the query.
    PCollection<KnownSize> queryResults = applyPrim(events);

    if (configuration.debug) {
      // Monitor results as they go by.
      queryResults = queryResults.apply(resultMonitor.getTransform());
    }
    // Timestamp the query results.
    return queryResults.apply(NexmarkUtils.<KnownSize>stamp(name));
  }
}
