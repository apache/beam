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
import org.apache.beam.sdk.nexmark.model.AuctionBid;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.values.PCollection;

/**
 * Query "9", 'Winning bids'. Select just the winning bids. Not in original NEXMark suite, but handy
 * for testing. See {@link WinningBids} for the details.
 */
public class Query9 extends NexmarkQueryTransform<AuctionBid> {
  private final NexmarkConfiguration configuration;

  public Query9(NexmarkConfiguration configuration) {
    super("Query9");
    this.configuration = configuration;
  }

  @Override
  public PCollection<AuctionBid> expand(PCollection<Event> events) {
    return events.apply(Filter.by(new AuctionOrBid())).apply(new WinningBids(name, configuration));
  }
}
