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

import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

/**
 * Query 1, 'Currency Conversion'. Convert each bid value from dollars to euros. In CQL syntax:
 *
 * <pre>
 * SELECT Istream(auction, DOLTOEUR(price), bidder, datetime)
 * FROM bid [ROWS UNBOUNDED];
 * </pre>
 *
 * <p>To make things more interesting, allow the 'currency conversion' to be arbitrarily slowed
 * down.
 */
public class Query1 extends NexmarkQueryTransform<Bid> {
  public Query1(NexmarkConfiguration configuration) {
    super("Query1");
  }

  @Override
  public PCollection<Bid> expand(PCollection<Event> events) {
    return events
        // Only want the bid events.
        .apply(NexmarkQueryUtil.JUST_BIDS)

        // Map the conversion function over all bids.
        .apply(
            name + ".ToEuros",
            ParDo.of(
                new DoFn<Bid, Bid>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    Bid bid = c.element();
                    c.output(
                        new Bid(
                            bid.auction,
                            bid.bidder,
                            (bid.price * 89) / 100,
                            bid.dateTime,
                            bid.extra));
                  }
                }));
  }
}
