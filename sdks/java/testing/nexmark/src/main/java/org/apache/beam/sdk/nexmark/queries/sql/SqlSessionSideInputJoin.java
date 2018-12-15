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

import static com.google.common.base.Preconditions.checkState;

import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.nexmark.model.sql.SelectEvent;
import org.apache.beam.sdk.nexmark.queries.NexmarkQueryTransform;
import org.apache.beam.sdk.nexmark.queries.NexmarkQueryUtil;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;

/** Advanced stream enrichment: join a sessionized stream to a bounded side input. */
public class SqlSessionSideInputJoin extends NexmarkQueryTransform<Bid> {
  private final String query;

  public SqlSessionSideInputJoin(NexmarkConfiguration configuration) {
    super("SqlSessionSideInputJoin");

    // Notes on the sensitivities of our parsing and planning:
    //  - cannot directly join MOD(bidder, x) = side.id because only equijoins on col refs allowed,
    //    so we need a WITH clause or subquery
    //  - must have the CAST inside the WITH clause for the same reason, otherwise the cast
    //    occurs in the join condition CAST(side_id AS BIGINT) = side.id
    query =
        String.format(
            "WITH sessions (bidder, bids, starttime, endtime) AS (%n"
                + "  SELECT %n"
                + "    bidder %n"
                + "    ARRAY_AGG(ROW(auction, bidder, price, dateTime)),%n"
                + "    SESSION_START(bid.bidtime, INTERVAL '%d' MINUTES))%n"
                + "    SESSION_START(bid.bidtime, INTERVAL '%d' MINUTES))%n"
                + "  FROM bid_with_side %n"
                + "  GROUP BY bidder, SESSION(bid.bidtime, INTERVAL '%d' MINUTES))"
                + "WITH session_with_side (bidder, bids, starttime, endtime, side_id) AS (%n"
                + "  SELECT *, CAST(MOD(bidder, %d) AS BIGINT) side_id FROM sessions%n"
                + ")%n"
                + "SELECT %n"
                + "  session_bid.auction, %n"
                + "  session_with_side.bidder, %n"
                + "  session_bid.price, %n"
                + "  session_bid.dateTime, %n"
                + "  CONCAT(side.extra, ':', starttime, ':', endtime) %n"
                + "FROM session_with_side, side, %n"
                + "JOIN UNNEST(session_with_side.bids) session_bid %n"
                + "WHERE bid_with_side.side_id = side.id",
            configuration.sessionGap.getStandardMinutes(),
            configuration.sessionGap.getStandardMinutes(),
            configuration.sessionGap.getStandardMinutes(),
            configuration.sideInputRowCount);
  }

  @Override
  public boolean needsSideInput() {
    return true;
  }

  @Override
  public PCollection<Bid> expand(PCollection<Event> events) {
    PCollection<Row> bids =
        events
            .apply(Filter.by(NexmarkQueryUtil.IS_BID))
            .apply(getName() + ".SelectEvent", new SelectEvent(Event.Type.BID));

    checkState(getSideInput() != null, "Configuration error: side input is null");

    TupleTag<Row> sideTag = new TupleTag<Row>("side") {};
    TupleTag<Row> bidTag = new TupleTag<Row>("bid") {};

    Schema schema =
        Schema.of(
            Schema.Field.of("id", Schema.FieldType.INT64),
            Schema.Field.of("extra", Schema.FieldType.STRING));

    PCollection<Row> sideRows =
        getSideInput()
            .setSchema(
                schema,
                kv -> Row.withSchema(schema).addValues(kv.getKey(), kv.getValue()).build(),
                row -> KV.of(row.getInt64("id"), row.getString("extra")))
            .apply("SideToRows", Convert.toRows());

    return PCollectionTuple.of(bidTag, bids)
        .and(sideTag, sideRows)
        .apply(SqlTransform.query(query))
        .apply("ResultToBid", Convert.fromRows(Bid.class));
  }
}
