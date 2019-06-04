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

import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.nexmark.model.Person;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Instant;

/** Utilities for working with NEXmark data stream. */
public class NexmarkQueryUtil {
  // Do not instantiate
  private NexmarkQueryUtil() {}

  public static final TupleTag<Auction> AUCTION_TAG = new TupleTag<>("auctions");
  public static final TupleTag<Bid> BID_TAG = new TupleTag<>("bids");
  public static final TupleTag<Person> PERSON_TAG = new TupleTag<>("person");

  /** Predicate to detect a new person event. */
  public static final SerializableFunction<Event, Boolean> IS_NEW_PERSON =
      event -> event.newPerson != null;

  /** DoFn to convert a new person event to a person. */
  public static final DoFn<Event, Person> AS_PERSON =
      new DoFn<Event, Person>() {
        @ProcessElement
        public void processElement(ProcessContext c) {
          c.output(c.element().newPerson);
        }
      };

  /** Predicate to detect a new auction event. */
  public static final SerializableFunction<Event, Boolean> IS_NEW_AUCTION =
      event -> event.newAuction != null;

  /** DoFn to convert a new auction event to an auction. */
  public static final DoFn<Event, Auction> AS_AUCTION =
      new DoFn<Event, Auction>() {
        @ProcessElement
        public void processElement(ProcessContext c) {
          c.output(c.element().newAuction);
        }
      };

  /** Predicate to detect a new bid event. */
  public static final SerializableFunction<Event, Boolean> IS_BID = event -> event.bid != null;

  /** DoFn to convert a bid event to a bid. */
  public static final DoFn<Event, Bid> AS_BID =
      new DoFn<Event, Bid>() {
        @ProcessElement
        public void processElement(ProcessContext c) {
          c.output(c.element().bid);
        }
      };

  /** Transform to key each person by their id. */
  public static final ParDo.SingleOutput<Person, KV<Long, Person>> PERSON_BY_ID =
      ParDo.of(
          new DoFn<Person, KV<Long, Person>>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
              c.output(KV.of(c.element().id, c.element()));
            }
          });

  /** Transform to key each auction by its id. */
  public static final ParDo.SingleOutput<Auction, KV<Long, Auction>> AUCTION_BY_ID =
      ParDo.of(
          new DoFn<Auction, KV<Long, Auction>>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
              c.output(KV.of(c.element().id, c.element()));
            }
          });

  /** Transform to key each auction by its seller id. */
  public static final ParDo.SingleOutput<Auction, KV<Long, Auction>> AUCTION_BY_SELLER =
      ParDo.of(
          new DoFn<Auction, KV<Long, Auction>>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
              c.output(KV.of(c.element().seller, c.element()));
            }
          });

  /** Transform to key each bid by it's auction id. */
  public static final ParDo.SingleOutput<Bid, KV<Long, Bid>> BID_BY_AUCTION =
      ParDo.of(
          new DoFn<Bid, KV<Long, Bid>>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
              c.output(KV.of(c.element().auction, c.element()));
            }
          });

  /** Transform to project the auction id from each bid. */
  public static final ParDo.SingleOutput<Bid, Long> BID_TO_AUCTION =
      ParDo.of(
          new DoFn<Bid, Long>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
              c.output(c.element().auction);
            }
          });

  /** Transform to project the price from each bid. */
  public static final ParDo.SingleOutput<Bid, Long> BID_TO_PRICE =
      ParDo.of(
          new DoFn<Bid, Long>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
              c.output(c.element().price);
            }
          });

  /** Transform to emit each event with the timestamp embedded within it. */
  public static final ParDo.SingleOutput<Event, Event> EVENT_TIMESTAMP_FROM_DATA =
      ParDo.of(
          new DoFn<Event, Event>() {
            @ProcessElement
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
          });

  /** Transform to filter for just the new auction events. */
  public static final PTransform<PCollection<Event>, PCollection<Auction>> JUST_NEW_AUCTIONS =
      new PTransform<PCollection<Event>, PCollection<Auction>>("justNewAuctions") {
        @Override
        public PCollection<Auction> expand(PCollection<Event> input) {
          return input
              .apply("IsNewAuction", Filter.by(IS_NEW_AUCTION))
              .apply("AsAuction", ParDo.of(AS_AUCTION));
        }
      };

  /** Transform to filter for just the new person events. */
  public static final PTransform<PCollection<Event>, PCollection<Person>> JUST_NEW_PERSONS =
      new PTransform<PCollection<Event>, PCollection<Person>>("justNewPersons") {
        @Override
        public PCollection<Person> expand(PCollection<Event> input) {
          return input
              .apply("IsNewPerson", Filter.by(IS_NEW_PERSON))
              .apply("AsPerson", ParDo.of(AS_PERSON));
        }
      };

  /** Transform to filter for just the bid events. */
  public static final PTransform<PCollection<Event>, PCollection<Bid>> JUST_BIDS =
      new PTransform<PCollection<Event>, PCollection<Bid>>("justBids") {
        @Override
        public PCollection<Bid> expand(PCollection<Event> input) {
          return input.apply("IsBid", Filter.by(IS_BID)).apply("AsBid", ParDo.of(AS_BID));
        }
      };
}
