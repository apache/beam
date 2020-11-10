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
import org.apache.beam.sdk.extensions.sql.impl.CalciteQueryPlanner;
import org.apache.beam.sdk.extensions.sql.impl.QueryPlanner;
import org.apache.beam.sdk.extensions.sql.zetasql.ZetaSQLQueryPlanner;
import org.apache.beam.sdk.nexmark.model.AuctionPrice;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.nexmark.model.Event.Type;
import org.apache.beam.sdk.nexmark.model.sql.SelectEvent;
import org.apache.beam.sdk.nexmark.queries.NexmarkQueryTransform;
import org.apache.beam.sdk.nexmark.queries.NexmarkQueryUtil;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.values.PCollection;

/**
 * Query 2, 'Filtering. Find bids with specific auction ids and show their bid price. In CQL syntax:
 *
 * <pre>
 * SELECT Rstream(auction, price)
 * FROM Bid [NOW]
 * WHERE auction = 1007 OR auction = 1020 OR auction = 2001 OR auction = 2019 OR auction = 2087;
 * </pre>
 *
 * <p>As written that query will only yield a few hundred results over event streams of arbitrary
 * size. To make it more interesting we instead choose bids for every {@code skipFactor}'th auction.
 */
public class SqlQuery2 extends NexmarkQueryTransform<AuctionPrice> {

  private static final String QUERY_TEMPLATE =
      "SELECT auction, price FROM PCOLLECTION WHERE MOD(auction, %d) = 0";

  private final long skipFactor;
  private final Class<? extends QueryPlanner> plannerClass;

  private SqlQuery2(String name, long skipFactor, Class<? extends QueryPlanner> plannerClass) {
    super("SqlQuery2");
    this.plannerClass = plannerClass;
    this.skipFactor = skipFactor;
  }

  public static SqlQuery2 calciteSqlQuery2(long skipFactor) {
    return new SqlQuery2("SqlQuery2", skipFactor, CalciteQueryPlanner.class);
  }

  public static SqlQuery2 zetaSqlQuery2(long skipFactor) {
    return new SqlQuery2("ZetaSqlQuery2", skipFactor, ZetaSQLQueryPlanner.class);
  }

  @Override
  public PCollection<AuctionPrice> expand(PCollection<Event> allEvents) {
    return allEvents
        .apply(Filter.by(NexmarkQueryUtil.IS_BID))
        .apply(getName() + ".SelectEvent", new SelectEvent(Type.BID))
        .apply(
            SqlTransform.query(String.format(QUERY_TEMPLATE, skipFactor))
                .withQueryPlannerClass(plannerClass))
        .apply(Convert.fromRows(AuctionPrice.class));
  }
}
