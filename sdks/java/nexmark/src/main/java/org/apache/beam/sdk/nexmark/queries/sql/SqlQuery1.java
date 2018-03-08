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
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.Row;

/**
 * Query 1, 'Currency Conversion'. Convert each bid value from dollars to euros.
 * In CQL syntax:
 *
 * <pre>
 * SELECT Istream(auction, DOLTOEUR(price), bidder, datetime)
 * FROM bid [ROWS UNBOUNDED];
 * </pre>
 *
 * <p>To make things more interesting, allow the 'currency conversion' to be arbitrarily
 * slowed down.
 */
public class SqlQuery1 extends PTransform<PCollection<Event>, PCollection<Bid>> {

  private static final PTransform<PInput, PCollection<Row>> QUERY = BeamSql
      .query("SELECT auction, bidder, DolToEur(price) as price, dateTime, extra FROM PCOLLECTION")
      .registerUdf("DolToEur", new DolToEur());

  /**
   * Dollar to Euro conversion.
   */
  public static class DolToEur implements SerializableFunction<Long, Long> {
    @Override
    public Long apply(Long price) {
      return (price * 89) / 100;
    }
  }

  public SqlQuery1() {
    super("SqlQuery1");
  }

  @Override
  public PCollection<Bid> expand(PCollection<Event> allEvents) {
    RowCoder bidRecordCoder = getBidRowCoder();

    PCollection<Row> bidEventsRows = allEvents
        .apply(Filter.by(IS_BID))
        .apply(getName() + ".ToRow", ToRow.parDo())
        .setCoder(bidRecordCoder);

    PCollection<Row> queryResultsRows = bidEventsRows.apply(QUERY);

    return queryResultsRows
        .apply(bidParDo())
        .setCoder(Bid.CODER);
  }

  private RowCoder getBidRowCoder() {
    return ADAPTERS.get(Bid.class).getRowType().getRowCoder();
  }

  private ParDo.SingleOutput<Row, Bid> bidParDo() {
    return ADAPTERS.get(Bid.class).parDo();
  }
}
