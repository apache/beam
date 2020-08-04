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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.extensions.sql.impl.CalciteQueryPlanner;
import org.apache.beam.sdk.extensions.sql.impl.QueryPlanner;
import org.apache.beam.sdk.extensions.sql.zetasql.ZetaSQLQueryPlanner;
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
import org.apache.beam.sdk.values.TypeDescriptors;

/** Basic stream enrichment: join a stream to a bounded side input. */
public class SqlBoundedSideInputJoin extends NexmarkQueryTransform<Bid> {
  private final String query;
  private final NexmarkConfiguration configuration;
  private final Class<? extends QueryPlanner> plannerClass;

  private SqlBoundedSideInputJoin(
      String name,
      NexmarkConfiguration configuration,
      Class<? extends QueryPlanner> plannerClass,
      String query) {
    super("SqlBoundedSideInputJoin");
    this.configuration = configuration;
    this.plannerClass = plannerClass;
    this.query = query;
  }

  public static SqlBoundedSideInputJoin calciteSqlBoundedSideInputJoin(
      NexmarkConfiguration configuration) {

    // Notes on the sensitivities of Calcite SQL parsing and planning:
    //  - cannot directly join MOD(bidder, x) = side.id because only equijoins on col refs allowed,
    //    so we need a WITH clause or subquery
    //  - must have the CAST inside the WITH clause for the same reason, otherwise the cast
    //    occurs in the join condition CAST(side_id AS BIGINT) = side.id
    return new SqlBoundedSideInputJoin(
        "SqlBoundedSideInputJoin",
        configuration,
        CalciteQueryPlanner.class,
        "WITH bid_with_side (auction, bidder, price, dateTime, extra, side_id) AS (%n"
            + "  SELECT *, CAST(MOD(bidder, %d) AS BIGINT) side_id FROM bid%n"
            + ")%n"
            + " SELECT bid_with_side.auction%n"
            + ", bid_with_side.bidder%n"
            + ", bid_with_side.price%n"
            + ", bid_with_side.dateTime%n"
            + ", side.extra%n"
            + " FROM bid_with_side, side%n"
            + " WHERE bid_with_side.side_id = side.id");
  }

  public static SqlBoundedSideInputJoin zetaSqlBoundedSideInputJoin(
      NexmarkConfiguration configuration) {
    // Differences from Calcite SQL:
    //   - INT64 instead of BIGINT
    //   - no column list for WITH table alias
    return new SqlBoundedSideInputJoin(
        "ZetaSqlBoundedSideInputJoin",
        configuration,
        ZetaSQLQueryPlanner.class,
        "WITH bid_with_side AS (%n"
            + "  SELECT *, CAST(MOD(bidder, %d) AS INT64) side_id FROM bid%n"
            + ")%n"
            + " SELECT bid_with_side.auction%n"
            + ", bid_with_side.bidder%n"
            + ", bid_with_side.price%n"
            + ", bid_with_side.dateTime%n"
            + ", side.extra%n"
            + " FROM bid_with_side, side%n"
            + " WHERE bid_with_side.side_id = side.id");
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
                TypeDescriptors.kvs(TypeDescriptors.longs(), TypeDescriptors.strings()),
                kv -> Row.withSchema(schema).addValues(kv.getKey(), kv.getValue()).build(),
                row -> KV.of(row.getInt64("id"), row.getString("extra")))
            .apply("SideToRows", Convert.toRows());

    return PCollectionTuple.of(bidTag, bids)
        .and(sideTag, sideRows)
        .apply(
            SqlTransform.query(String.format(query, configuration.sideInputRowCount))
                .withQueryPlannerClass(plannerClass))
        .apply("ResultToBid", Convert.fromRows(Bid.class));
  }
}
