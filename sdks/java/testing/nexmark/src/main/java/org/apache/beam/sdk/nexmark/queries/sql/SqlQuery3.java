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
import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.nexmark.model.Event.Type;
import org.apache.beam.sdk.nexmark.model.NameCityStateId;
import org.apache.beam.sdk.nexmark.model.Person;
import org.apache.beam.sdk.nexmark.model.sql.SelectEvent;
import org.apache.beam.sdk.nexmark.queries.NexmarkQueryTransform;
import org.apache.beam.sdk.nexmark.queries.Query3;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Duration;

/**
 * Query 3, 'Local Item Suggestion'. Who is selling in OR, ID or CA in category 10, and for what
 * auction ids? In CQL syntax:
 *
 * <pre>
 * SELECT Istream(P.name, P.city, P.state, A.id)
 * FROM Auction A [ROWS UNBOUNDED], Person P [ROWS UNBOUNDED]
 * WHERE A.seller = P.id AND (P.state = `OR' OR P.state = `ID' OR P.state = `CA') AND A.category
 * = 10;
 * </pre>
 *
 * <p>This implementation runs as written, but results may not be what is expected from a correct
 * join, and behavior doesn't match the java version.
 *
 * <p>At the moment join is implemented as a CoGBK, it joins the trigger outputs. It means that in
 * discarding mode it will join only new elements arrived since last trigger firing. And in
 * accumulating mode it will output the results which were already emitted in last trigger firing.
 *
 * <p>Additionally, it is currently not possible to match the elements across windows.
 *
 * <p>All of the above makes it not intuitive, inflexible, and produces results which may not be
 * what users are expecting.
 *
 * <p>Java version of the query ({@link Query3}) solves this by caching the auctions in the state
 * cell if there was no matching seller yet. And then flushes them when sellers become available.
 *
 * <p>Correct join semantics implementation is tracked in BEAM-3190, BEAM-3191
 */
public class SqlQuery3 extends NexmarkQueryTransform<NameCityStateId> {

  private static final String QUERY =
      ""
          + " SELECT "
          + "    P.name, P.city, P.state, A.id "
          + " FROM "
          + "    Auction A INNER JOIN Person P on A.seller = P.id "
          + " WHERE "
          + "    A.category = 10 "
          + "    AND (P.state = 'OR' OR P.state = 'ID' OR P.state = 'CA')";

  private final NexmarkConfiguration configuration;
  private final Class<? extends QueryPlanner> plannerClass;

  private SqlQuery3(
      String name, NexmarkConfiguration configuration, Class<? extends QueryPlanner> plannerClass) {
    super(name);
    this.configuration = configuration;
    this.plannerClass = plannerClass;
  }

  public static SqlQuery3 calciteSqlQuery3(NexmarkConfiguration configuration) {
    return new SqlQuery3("SqlQuery3", configuration, CalciteQueryPlanner.class);
  }

  public static SqlQuery3 zetaSqlQuery3(NexmarkConfiguration configuration) {
    return new SqlQuery3("ZetaSqlQuery3", configuration, ZetaSQLQueryPlanner.class);
  }

  @Override
  public PCollection<NameCityStateId> expand(PCollection<Event> allEvents) {
    PCollection<Event> windowed =
        allEvents.apply(
            Window.into(FixedWindows.of(Duration.standardSeconds(configuration.windowSizeSec))));

    String auctionName = Auction.class.getSimpleName();
    PCollection<Row> auctions =
        windowed
            .apply(getName() + ".Filter." + auctionName, Filter.by(e1 -> e1.newAuction != null))
            .apply(getName() + ".ToRecords." + auctionName, new SelectEvent(Type.AUCTION));

    String personName = Person.class.getSimpleName();
    PCollection<Row> people =
        windowed
            .apply(getName() + ".Filter." + personName, Filter.by(e -> e.newPerson != null))
            .apply(getName() + ".ToRecords." + personName, new SelectEvent(Type.PERSON));

    PCollectionTuple inputStreams =
        PCollectionTuple.of(new TupleTag<>("Auction"), auctions)
            .and(new TupleTag<>("Person"), people);

    return inputStreams
        .apply(SqlTransform.query(QUERY).withQueryPlannerClass(plannerClass))
        .apply(Convert.fromRows(NameCityStateId.class));
  }
}
