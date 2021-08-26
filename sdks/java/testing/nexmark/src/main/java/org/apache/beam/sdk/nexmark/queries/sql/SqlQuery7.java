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

import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.nexmark.model.Event.Type;
import org.apache.beam.sdk.nexmark.model.sql.SelectEvent;
import org.apache.beam.sdk.nexmark.queries.NexmarkQueryTransform;
import org.apache.beam.sdk.nexmark.queries.NexmarkQueryUtil;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;

/**
 * Query 7, 'Highest Bid'. Select the bids with the highest bid price in the last minute. In CQL
 * syntax:
 *
 * <pre>
 * SELECT Rstream(B.auction, B.price, B.bidder)
 * FROM Bid [RANGE 1 MINUTE SLIDE 1 MINUTE] B
 * WHERE B.price = (SELECT MAX(B1.price)
 *                  FROM BID [RANGE 1 MINUTE SLIDE 1 MINUTE] B1);
 * </pre>
 *
 * <p>We will use a shorter window to help make testing easier.
 */
public class SqlQuery7 extends NexmarkQueryTransform<Bid> {

  private static final String QUERY_TEMPLATE =
      ""
          + " SELECT B.auction, B.price, B.bidder, B.dateTime, B.extra "
          + "    FROM (SELECT B.auction, B.price, B.bidder, B.dateTime, B.extra, "
          + "       TUMBLE_START(B.dateTime, INTERVAL '%1$d' SECOND) AS starttime "
          + "    FROM Bid B "
          + "    GROUP BY B.auction, B.price, B.bidder, B.dateTime, B.extra, "
          + "       TUMBLE(B.dateTime, INTERVAL '%1$d' SECOND)) B "
          + " JOIN (SELECT MAX(B1.price) AS maxprice, "
          + "       TUMBLE_START(B1.dateTime, INTERVAL '%1$d' SECOND) AS starttime "
          + "    FROM Bid B1 "
          + "    GROUP BY TUMBLE(B1.dateTime, INTERVAL '%1$d' SECOND)) B1 "
          + " ON B.starttime = B1.starttime AND B.price = B1.maxprice ";

  private final PTransform<PInput, PCollection<Row>> query;

  public SqlQuery7(NexmarkConfiguration configuration) {
    super("SqlQuery7");

    String queryString = String.format(QUERY_TEMPLATE, configuration.windowSizeSec);
    query = SqlTransform.query(queryString);
  }

  @Override
  public PCollection<Bid> expand(PCollection<Event> allEvents) {
    PCollection<Row> bids =
        allEvents
            .apply(Filter.by(NexmarkQueryUtil.IS_BID))
            .apply(getName() + ".SelectEvent", new SelectEvent(Type.BID));

    return PCollectionTuple.of(new TupleTag<>("Bid"), bids)
        .apply(query)
        .apply(Convert.fromRows(Bid.class));
  }
}
