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
package org.apache.beam.sdk.extensions.tpc.query;

/** queries in H benchmark. */
public class TpcHQuery {
  public static final String QUERYTEST =
      "select\n"
          + "\tcount(*) as count_order\n"
          + "from\n"
          + "\tlineitem\n"
          + "where\n"
          + "\tl_shipdate <= date '1998-09-01' + interval '90' day (3) ";

  public static final String QUERY1 =
      "select\n"
          + "\tl_returnflag,\n"
          + "\tl_linestatus,\n"
          + "\tsum(l_quantity) as sum_qty,\n"
          + "\tsum(l_extendedprice) as sum_base_price,\n"
          + "\tsum(l_extendedprice * (1 - l_discount)) as sum_disc_price,\n"
          + "\tsum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,\n"
          + "\tavg(l_quantity) as avg_qty,\n"
          + "\tavg(l_extendedprice) as avg_price,\n"
          + "\tavg(l_discount) as avg_disc,\n"
          + "\tcount(*) as count_order\n"
          + "from\n"
          + "\tlineitem\n"
          + "where\n"
          + "\tl_shipdate <= date '1998-12-01' - interval '90' day (3)\n"
          + "group by\n"
          + "\tl_returnflag,\n"
          + "\tl_linestatus\n"
          + "order by\n"
          + "\tl_returnflag,\n"
          + "\tl_linestatus limit 10";

  public static final String QUERY3 =
      "select\n"
          + "\tl_orderkey,\n"
          + "\tsum(l_extendedprice * (1 - l_discount)) as revenue,\n"
          + "\to_orderdate,\n"
          + "\to_shippriority\n"
          + "from\n"
          + "\tcustomer,\n"
          + "\torders,\n"
          + "\tlineitem\n"
          + "where\n"
          + "\tc_mktsegment = 'BUILDING'\n"
          + "\tand c_custkey = o_custkey\n"
          + "\tand l_orderkey = o_orderkey\n"
          + "\tand o_orderdate < date '1995-03-15'\n"
          + "\tand l_shipdate > date '1995-03-15'\n"
          + "group by\n"
          + "\tl_orderkey,\n"
          + "\to_orderdate,\n"
          + "\to_shippriority\n"
          + "order by\n"
          + "\trevenue desc,\n"
          + "\to_orderdate\n"
          + "limit 10";

  public static final String QUERY4 =
      "select\n"
          + "\to_orderpriority,\n"
          + "\tcount(*) as order_count\n"
          + "from\n"
          + "\torders\n"
          + "where\n"
          + "\to_orderdate >= date '1993-07-01'\n"
          + "\tand o_orderdate < date '1993-07-01' + interval '3' month\n"
          + "\tand exists (\n"
          + "\t\tselect\n"
          + "\t\t\t*\n"
          + "\t\tfrom\n"
          + "\t\t\tlineitem\n"
          + "\t\twhere\n"
          + "\t\t\tl_orderkey = o_orderkey\n"
          + "\t\t\tand l_commitdate < l_receiptdate\n"
          + "\t)\n"
          + "group by\n"
          + "\to_orderpriority\n"
          + "order by\n"
          + "\to_orderpriority limit 10";

  public static final String QUERY5 =
      "select\n"
          + "\tn_name,\n"
          + "\tsum(l_extendedprice * (1 - l_discount)) as revenue\n"
          + "from\n"
          + "\tcustomer,\n"
          + "\torders,\n"
          + "\tlineitem,\n"
          + "\tsupplier,\n"
          + "\tnation,\n"
          + "\tregion\n"
          + "where\n"
          + "\tc_custkey = o_custkey\n"
          + "\tand l_orderkey = o_orderkey\n"
          + "\tand l_suppkey = s_suppkey\n"
          + "\tand c_nationkey = s_nationkey\n"
          + "\tand s_nationkey = n_nationkey\n"
          + "\tand n_regionkey = r_regionkey\n"
          + "\tand r_name = 'ASIA'\n"
          + "\tand o_orderdate >= date '1994-01-01'\n"
          + "\tand o_orderdate < date '1994-01-01' + interval '1' year\n"
          + "group by\n"
          + "\tn_name\n"
          + "order by\n"
          + "\trevenue desc limit 10";

  public static final String QUERY6 =
      "select\n"
          + "\tsum(l_extendedprice * l_discount) as revenue\n"
          + "from\n"
          + "\tlineitem\n"
          + "where\n"
          + "\tl_shipdate >= date '1994-01-01'\n"
          + "\tand l_shipdate < date '1994-01-01' + interval '1' year\n"
          + "\tand l_discount between .06 - 0.01 and .06 + 0.01\n"
          + "\tand l_quantity < 24 limit 10";
}
