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

import static org.apache.beam.sdk.nexmark.queries.NexmarkQuery.IS_BID;

import com.google.common.base.Joiner;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.model.AuctionCount;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.nexmark.model.Event.Type;
import org.apache.beam.sdk.nexmark.model.sql.SelectEvent;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;

/**
 * Query 5, 'Hot Items'. Which auctions have seen the most bids in the last hour (updated every
 * minute). In CQL syntax:
 *
 * <pre>{@code
 * SELECT Rstream(auction)
 * FROM (SELECT B1.auction, count(*) AS num
 *       FROM Bid [RANGE 60 MINUTE SLIDE 1 MINUTE] B1
 *       GROUP BY B1.auction)
 * WHERE num >= ALL (SELECT count(*)
 *                   FROM Bid [RANGE 60 MINUTE SLIDE 1 MINUTE] B2
 *                   GROUP BY B2.auction);
 * }</pre>
 *
 * <p>To make things a bit more dynamic and easier to test we use much shorter windows, and we'll
 * also preserve the bid counts.
 */
public class SqlQuery5 extends PTransform<PCollection<Event>, PCollection<AuctionCount>> {

  private static final String QUERY_TEMPLATE =
      Joiner.on("\n\t")
          .join(
              " SELECT AuctionBids.auction, AuctionBids.num",
              " FROM (",
              "   SELECT",
              "     B1.auction,",
              "     count(*) AS num,",
              "     HOP_START(B1.dateTime, INTERVAL '%1$d' SECOND, INTERVAL '%2$d' SECOND) AS starttime",
              "   FROM Bid B1 ",
              "   GROUP BY ",
              "     B1.auction,",
              "     HOP(B1.dateTime, INTERVAL '%1$d' SECOND, INTERVAL '%2$d' SECOND)",
              " ) AS AuctionBids",
              " JOIN (",
              "   SELECT ",
              "     max(CountBids.num) AS maxnum, ",
              "     CountBids.starttime",
              "   FROM (",
              "     SELECT",
              "       count(*) AS num,",
              "       HOP_START(B2.dateTime, INTERVAL '%1$d' SECOND, INTERVAL '%2$d' SECOND) AS starttime",
              "     FROM Bid B2 ",
              "     GROUP BY ",
              "       B2.auction, ",
              "       HOP(B2.dateTime, INTERVAL '%1$d' SECOND, INTERVAL '%2$d' SECOND)",
              "     ) AS CountBids",
              "   GROUP BY CountBids.starttime",
              " ) AS MaxBids ",
              " ON AuctionBids.starttime = MaxBids.starttime AND AuctionBids.num >= MaxBids.maxnum ");

  private final PTransform<PInput, PCollection<Row>> query;

  public SqlQuery5(NexmarkConfiguration configuration) {
    super("SqlQuery5");

    String queryString =
        String.format(QUERY_TEMPLATE, configuration.windowPeriodSec, configuration.windowSizeSec);
    query = SqlTransform.query(queryString);
  }

  @Override
  public PCollection<AuctionCount> expand(PCollection<Event> allEvents) {
    PCollection<Row> bids =
        allEvents
            .apply(Filter.by(IS_BID))
            .apply(getName() + ".SelectEvent", new SelectEvent(Type.BID));

    return PCollectionTuple.of(new TupleTag<>("Bid"), bids)
        .apply(query)
        .apply(Convert.fromRows(AuctionCount.class));
  }
}
