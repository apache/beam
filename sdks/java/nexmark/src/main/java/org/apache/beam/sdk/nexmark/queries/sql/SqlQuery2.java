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
package org.apache.beam.sdk.nexmark.queries.sql;

import static org.apache.beam.sdk.nexmark.model.sql.adapter.ModelAdaptersMapping.ADAPTERS;
import static org.apache.beam.sdk.nexmark.queries.NexmarkQuery.IS_BID;

import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.extensions.sql.BeamSql;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.nexmark.model.sql.ToRow;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.Row;

/**
 * Query 2, 'Filtering. Find bids with specific auction ids and show their bid price.
 * In CQL syntax:
 *
 * <pre>
 * SELECT Rstream(auction, price)
 * FROM Bid [NOW]
 * WHERE auction = 1007 OR auction = 1020 OR auction = 2001 OR auction = 2019 OR auction = 2087;
 * </pre>
 *
 * <p>As written that query will only yield a few hundred results over event streams of
 * arbitrary size. To make it more interesting we instead choose bids for every
 * {@code skipFactor}'th auction.
 */
public class SqlQuery2 extends PTransform<PCollection<Event>, PCollection<Row>> {

  private static final String QUERY_TEMPLATE =
      "SELECT auction, bidder, price, dateTime, extra  FROM PCOLLECTION "
          + " WHERE MOD(auction, %d) = 0";

  private final PTransform<PInput, PCollection<Row>> query;

  public SqlQuery2(long skipFactor) {
    super("SqlQuery2");

    String queryString = String.format(QUERY_TEMPLATE, skipFactor);
    query = BeamSql.query(queryString);
  }

  @Override
  public PCollection<Row> expand(PCollection<Event> allEvents) {
    RowCoder bidRecordCoder = getBidRowCoder();

    PCollection<Row> bidEventsRows = allEvents
        .apply(Filter.by(IS_BID))
        .apply(ToRow.parDo())
        .setCoder(bidRecordCoder);

    return bidEventsRows.apply(query);
  }

  private RowCoder getBidRowCoder() {
    return ADAPTERS.get(Bid.class).getRowType().getRowCoder();
  }
}
