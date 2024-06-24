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
package org.apache.beam.sdk.io.cassandra;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Token;
import java.math.BigInteger;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.cassandra.CassandraIO.Read;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
class ReadFn<T> extends DoFn<Read<T>, T> {

  private static final Logger LOG = LoggerFactory.getLogger(ReadFn.class);

  @ProcessElement
  public void processElement(@Element Read<T> read, OutputReceiver<T> receiver) {
    try {
      Session session = ConnectionManager.getSession(read);
      Mapper<T> mapper = read.mapperFactoryFn().apply(session);
      String partitionKey =
          session.getCluster().getMetadata().getKeyspace(read.keyspace().get())
              .getTable(read.table().get()).getPartitionKey().stream()
              .map(ColumnMetadata::getName)
              .collect(Collectors.joining(","));

      String query = generateRangeQuery(read, partitionKey, read.ringRanges() != null);
      PreparedStatement preparedStatement = session.prepare(query);
      Set<RingRange> ringRanges =
          read.ringRanges() == null ? Collections.emptySet() : read.ringRanges().get();

      for (RingRange rr : ringRanges) {
        Token startToken = session.getCluster().getMetadata().newToken(rr.getStart().toString());
        Token endToken = session.getCluster().getMetadata().newToken(rr.getEnd().toString());
        if (rr.isWrapping()) {
          // A wrapping range is one that overlaps from the end of the partitioner range and its
          // start (ie : when the start token of the split is greater than the end token)
          // We need to generate two queries here : one that goes from the start token to the end
          // of
          // the partitioner range, and the other from the start of the partitioner range to the
          // end token of the split.
          outputResults(
              session.execute(getLowestSplitQuery(read, partitionKey, rr.getEnd())),
              receiver,
              mapper);
          outputResults(
              session.execute(getHighestSplitQuery(read, partitionKey, rr.getStart())),
              receiver,
              mapper);
        } else {
          ResultSet rs =
              session.execute(
                  preparedStatement.bind().setToken(0, startToken).setToken(1, endToken));
          outputResults(rs, receiver, mapper);
        }
      }

      if (read.ringRanges() == null) {
        ResultSet rs = session.execute(preparedStatement.bind());
        outputResults(rs, receiver, mapper);
      }
    } catch (Exception ex) {
      LOG.error("error", ex);
    }
  }

  private static <T> void outputResults(
      ResultSet rs, OutputReceiver<T> outputReceiver, Mapper<T> mapper) {
    Iterator<T> iter = mapper.map(rs);
    while (iter.hasNext()) {
      T n = iter.next();
      outputReceiver.output(n);
    }
  }

  private static String getHighestSplitQuery(
      Read<?> spec, String partitionKey, BigInteger highest) {
    String highestClause = String.format("(token(%s) >= %d)", partitionKey, highest);
    String finalHighQuery =
        (spec.query() == null)
            ? buildInitialQuery(spec, true) + highestClause
            : spec.query() + getJoinerClause(spec.query().get()) + highestClause;
    LOG.debug("CassandraIO generated a wrapAround query : {}", finalHighQuery);
    return finalHighQuery;
  }

  private static String getLowestSplitQuery(Read<?> spec, String partitionKey, BigInteger lowest) {
    String lowestClause = String.format("(token(%s) < %d)", partitionKey, lowest);
    String finalLowQuery =
        (spec.query() == null)
            ? buildInitialQuery(spec, true) + lowestClause
            : spec.query() + getJoinerClause(spec.query().get()) + lowestClause;
    LOG.debug("CassandraIO generated a wrapAround query : {}", finalLowQuery);
    return finalLowQuery;
  }

  private static String generateRangeQuery(
      Read<?> spec, String partitionKey, Boolean hasRingRange) {
    final String rangeFilter =
        hasRingRange
            ? Joiner.on(" AND ")
                .skipNulls()
                .join(
                    String.format("(token(%s) >= ?)", partitionKey),
                    String.format("(token(%s) < ?)", partitionKey))
            : "";
    final String combinedQuery = buildInitialQuery(spec, hasRingRange) + rangeFilter;
    LOG.debug("CassandraIO generated query : {}", combinedQuery);
    return combinedQuery;
  }

  private static String buildInitialQuery(Read<?> spec, Boolean hasRingRange) {
    return (spec.query() == null)
        ? String.format("SELECT * FROM %s.%s", spec.keyspace().get(), spec.table().get())
            + " WHERE "
        : spec.query().get() + (hasRingRange ? getJoinerClause(spec.query().get()) : "");
  }

  private static String getJoinerClause(String queryString) {
    return queryString.toUpperCase().contains("WHERE") ? " AND " : " WHERE ";
  }
}
