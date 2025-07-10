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

import java.util.Map;
import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.NexmarkQueryName;
import org.apache.beam.sdk.nexmark.queries.NexmarkQuery;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
})
public class SqlQueryUtils {
  public static Map<NexmarkQueryName, NexmarkQuery> createSqlQueries(
      NexmarkConfiguration configuration) {
    return ImmutableMap.<NexmarkQueryName, NexmarkQuery>builder()
        .put(
            NexmarkQueryName.PASSTHROUGH,
            new NexmarkQuery(configuration, SqlQuery0.calciteSqlQuery0()))
        .put(NexmarkQueryName.CURRENCY_CONVERSION, new NexmarkQuery(configuration, new SqlQuery1()))
        .put(
            NexmarkQueryName.SELECTION,
            new NexmarkQuery(configuration, SqlQuery2.calciteSqlQuery2(configuration.auctionSkip)))
        .put(
            NexmarkQueryName.LOCAL_ITEM_SUGGESTION,
            new NexmarkQuery(configuration, SqlQuery3.calciteSqlQuery3(configuration)))

        // SqlQuery5 is disabled for now, uses non-equi-joins,
        // never worked right, was giving incorrect results.
        // Gets rejected after PR/8301, causing failures.
        //
        // See:
        //   https://github.com/apache/beam/issues/19541
        //   https://github.com/apache/beam/pull/8301
        //   https://github.com/apache/beam/pull/8422#issuecomment-487676350
        //
        //        .put(
        //            NexmarkQueryName.HOT_ITEMS,
        //            new NexmarkQuery(configuration, new SqlQuery5(configuration)))
        .put(
            NexmarkQueryName.HIGHEST_BID,
            new NexmarkQuery(configuration, new SqlQuery7(configuration)))
        .put(
            NexmarkQueryName.BOUNDED_SIDE_INPUT_JOIN,
            new NexmarkQuery(
                configuration,
                SqlBoundedSideInputJoin.calciteSqlBoundedSideInputJoin(configuration)))
        .build();
  }
}
