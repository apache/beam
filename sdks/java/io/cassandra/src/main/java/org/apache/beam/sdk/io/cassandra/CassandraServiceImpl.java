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

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.beam.sdk.io.BoundedSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of the {@link CassandraService} that actually use a Cassandra instance.
 */
public class CassandraServiceImpl<T> implements CassandraService<T> {

  private static final Logger LOG = LoggerFactory.getLogger(CassandraServiceImpl.class);

  private static final long MIN_TOKEN = Long.MIN_VALUE;
  private static final long MAX_TOKEN = Long.MAX_VALUE;
  private static final BigInteger TOTAL_TOKEN_COUNT =
      BigInteger.valueOf(MAX_TOKEN).subtract(BigInteger.valueOf(MIN_TOKEN));

  private class CassandraReaderImpl<T> extends BoundedSource.BoundedReader<T> {

    private final CassandraIO.CassandraSource<T> source;

    private Cluster cluster;
    private Session session;
    private ResultSet resultSet;
    private Iterator<T> iterator;
    private T current;

    public CassandraReaderImpl(CassandraIO.CassandraSource<T> source) {
      this.source = source;
    }

    @Override
    public boolean start() throws IOException {
      LOG.debug("Starting Cassandra reader");
      cluster = getCluster(source.spec.hosts(), source.spec.port(), source.spec.username(),
          source.spec.password(), source.spec.localDc(), source.spec.consistencyLevel());
      session = cluster.connect();
      LOG.debug("Query: " + source.splitQuery);
      resultSet = session.execute(source.splitQuery);

      final MappingManager mappingManager = new MappingManager(session);
      Mapper mapper = mappingManager.mapper(source.spec.entity());
      iterator = mapper.map(resultSet).iterator();
      return advance();
    }

    @Override
    public boolean advance() throws IOException {
      if (iterator.hasNext()) {
        current = iterator.next();
        return true;
      }
      current = null;
      return false;
    }

    @Override
    public void close() {
      LOG.debug("Closing Cassandra reader");
      if (session != null) {
        session.close();
      }
      if (cluster != null) {
        cluster.close();
      }
    }

    @Override
    public T getCurrent() throws NoSuchElementException {
      if (current == null) {
        throw new NoSuchElementException();
      }
      return current;
    }

    @Override
    public CassandraIO.CassandraSource<T> getCurrentSource() {
      return source;
    }

  }

  @Override
  public CassandraReaderImpl<T> createReader(CassandraIO.CassandraSource<T> source) {
    return new CassandraReaderImpl<>(source);
  }

  @Override
  public long getEstimatedSizeBytes(CassandraIO.Read<T> spec) {
    try (Cluster cluster = getCluster(spec.hosts(), spec.port(), spec.username(), spec.password(),
        spec.localDc(), spec.consistencyLevel())) {
      if (isMurmur3Partitioner(cluster)) {
        try {
          List<TokenRange> tokenRanges = getTokenRanges(cluster,
              spec.keyspace(),
              spec.table());
          return getEstimatedSizeBytes(tokenRanges);
        } catch (Exception e) {
          LOG.warn("Can't estimate the size", e);
          return 0L;
        }
      } else {
        LOG.warn("Only Murmur3 partitioner is supported, can't estimate the size");
        return 0L;
      }
    }
  }

  /**
   * Actually estimate the size of the data to read on the cluster, based on the given token
   * ranges to address.
   */
  @VisibleForTesting
  protected static long getEstimatedSizeBytes(List<TokenRange> tokenRanges) {
    long size = 0L;
    for (TokenRange tokenRange : tokenRanges) {
      size = size + tokenRange.meanPartitionSize * tokenRange.partitionCount;
    }
    return Math.round(size / getRingFraction(tokenRanges));
  }

  @Override
  public List<BoundedSource<T>> split(CassandraIO.Read<T> spec,
      long desiredBundleSizeBytes) {
    try (Cluster cluster = getCluster(spec.hosts(), spec.port(), spec.username(), spec.password(),
        spec.localDc(), spec.consistencyLevel())) {
      if (isMurmur3Partitioner(cluster)) {
        LOG.info("Murmur3Partitioner detected, splitting");
        return split(spec, desiredBundleSizeBytes, getEstimatedSizeBytes(spec));
      } else {
        LOG.warn("Only Murmur3Partitioner is supported for splitting, using an unique source for "
            + "the read");
        String splitQuery = QueryBuilder.select().from(spec.keyspace(), spec.table()).toString();
        List<BoundedSource<T>> sources = new ArrayList<>();
        sources.add(new CassandraIO.CassandraSource<>(spec, splitQuery));
        return sources;
      }
    }
  }

  /**
   * Compute the number of splits based on the estimated size and the desired bundle size, and
   * create several sources.
   */
  @VisibleForTesting
  protected List<BoundedSource<T>> split(CassandraIO.Read<T> spec,
                                                long desiredBundleSizeBytes,
                                                long estimatedSizeBytes) {
    long numSplits = 1;
    List<BoundedSource<T>> sourceList = new ArrayList<>();
    if (desiredBundleSizeBytes > 0) {
      numSplits = estimatedSizeBytes / desiredBundleSizeBytes;
    }
    if (numSplits <= 0) {
      LOG.warn("Number of splits is less than 0 ({}), fallback to 1", numSplits);
      numSplits = 1;
    }

    LOG.info("Number of splits is {}", numSplits);

    double startRange = MIN_TOKEN;
    double endRange = MAX_TOKEN;
    double startToken, endToken;

    endToken = startRange;
    double incrementValue = endRange - startRange / numSplits;
    String splitQuery;
    if (numSplits == 1) {
      // we have an unique split
      splitQuery = QueryBuilder.select().from(spec.keyspace(), spec.table()).toString();
      sourceList.add(new CassandraIO.CassandraSource<>(spec, splitQuery));
    } else {
      // we have more than one split
      for (int i = 0; i < numSplits; i++) {
        startToken = endToken;
        endToken = startToken + incrementValue;
        Select.Where builder = QueryBuilder.select().from(spec.keyspace(), spec.table()).where();
        if (i > 0) {
          builder = builder.and(QueryBuilder.gte("token($pk)", startToken));
        }
        if (i < (numSplits - 1)) {
          builder = builder.and(QueryBuilder.lt("token($pk)", endToken));
        }
        sourceList.add(new CassandraIO.CassandraSource(spec, builder.toString()));
      }
    }
    return sourceList;
  }

  /**
   * Get a Cassandra cluster using hosts and port.
   */
  private Cluster getCluster(List<String> hosts, int port, String username, String password,
                             String localDc, String consistencyLevel) {
    Cluster.Builder builder = Cluster.builder()
        .addContactPoints(hosts.toArray(new String[0]))
        .withPort(port);

    if (username != null) {
      builder.withAuthProvider(new PlainTextAuthProvider(username, password));
    }

    if (localDc != null) {
      builder.withLoadBalancingPolicy(
          new TokenAwarePolicy(new DCAwareRoundRobinPolicy.Builder().withLocalDc(localDc).build()));
    } else {
      builder.withLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()));
    }

    if (consistencyLevel != null) {
      builder.withQueryOptions(
          new QueryOptions().setConsistencyLevel(ConsistencyLevel.valueOf(consistencyLevel)));
    }

    return builder.build();
  }

  /**
   * Gets the list of token ranges that a table occupies on a give Cassandra node.
   *
   * <p>NB: This method is compatible with Cassandra 2.1.5 and greater.
   */
  private static List<TokenRange> getTokenRanges(Cluster cluster, String keyspace, String table) {
    try (Session session = cluster.newSession()) {
      ResultSet resultSet =
          session.execute(
              "SELECT range_start, range_end, partitions_count, mean_partition_size FROM "
                  + "system.size_estimates WHERE keyspace_name = ? AND table_name = ?",
              keyspace,
              table);

      ArrayList<TokenRange> tokenRanges = new ArrayList<>();
      for (Row row : resultSet) {
        TokenRange tokenRange =
            new TokenRange(
                row.getLong("partitions_count"),
                row.getLong("mean_partition_size"),
                row.getLong("range_start"),
                row.getLong("range_end"));
        tokenRanges.add(tokenRange);
      }
      // The table may not contain the estimates yet
      // or have partitions_count and mean_partition_size fields = 0
      // if the data was just inserted and the amount of data in the table was small.
      // This is very common situation during tests,
      // when we insert a few rows and immediately query them.
      // However, for tiny data sets the lack of size estimates is not a problem at all,
      // because we don't want to split tiny data anyways.
      // Therefore, we're not issuing a warning if the result set was empty
      // or mean_partition_size and partitions_count = 0.
      return tokenRanges;
    }
  }

  /**
   * Compute the percentage of token addressed compared with the whole tokens in the cluster.
   */
  @VisibleForTesting
  protected static double getRingFraction(List<TokenRange> tokenRanges) {
    double ringFraction = 0;
    for (TokenRange tokenRange : tokenRanges) {
      ringFraction = ringFraction + (distance(tokenRange.rangeStart, tokenRange.rangeEnd)
          .doubleValue() / TOTAL_TOKEN_COUNT.doubleValue());
    }
    return ringFraction;
  }

  /**
   * Measure distance between two tokens.
   */
  @VisibleForTesting
  protected static BigInteger distance(long left, long right) {
    if (right > left) {
      return BigInteger.valueOf(right).subtract(BigInteger.valueOf(left));
    } else {
      return BigInteger.valueOf(right).subtract(BigInteger.valueOf(left)).add(TOTAL_TOKEN_COUNT);
    }
  }

  /**
   * Check if the current partitioner is the Murmur3 (default in Cassandra version newer than 2).
   */
  @VisibleForTesting
  protected static boolean isMurmur3Partitioner(Cluster cluster) {
    return cluster.getMetadata().getPartitioner()
        .equals("org.apache.cassandra.dht.Murmur3Partitioner");
  }

  /**
   * Represent a token range in Cassandra instance, wrapping the partition count, size and token
   * range.
   */
  @VisibleForTesting
  protected static class TokenRange {
    private final long partitionCount;
    private final long meanPartitionSize;
    private final long rangeStart;
    private final long rangeEnd;

    public TokenRange(
        long partitionCount, long meanPartitionSize, long rangeStart, long
        rangeEnd) {
      this.partitionCount = partitionCount;
      this.meanPartitionSize = meanPartitionSize;
      this.rangeStart = rangeStart;
      this.rangeEnd = rangeEnd;
    }
  }

  /**
   * Writer storing an entity into Apache Cassandra database.
   */
  protected class WriterImpl<T> implements Writer<T> {

    private final CassandraIO.Write<T> spec;

    private final Cluster cluster;
    private final Session session;
    private final MappingManager mappingManager;

    public WriterImpl(CassandraIO.Write<T> spec) {
      this.spec = spec;
      this.cluster = getCluster(spec.hosts(), spec.port(), spec.username(), spec.password(),
          spec.localDc(), spec.consistencyLevel());
      this.session = cluster.connect(spec.keyspace());
      this.mappingManager = new MappingManager(session);
    }

    /**
     * Write the entity to the Cassandra instance, using {@link Mapper} obtained with the
     * {@link MappingManager}. This method use {@link Mapper#save(Object)} method, which is
     * synchronous. It means the entity is guaranteed to be reliably committed to Cassandra.
     */
    @Override
    public void write(T entity) {
      Mapper<T> mapper = (Mapper<T>) mappingManager.mapper(entity.getClass());
      mapper.save(entity);
    }

    @Override
    public void close() {
      if (session != null) {
        session.close();
      }
      if (cluster != null) {
        cluster.close();
      }
    }

  }

  @Override
  public Writer createWriter(CassandraIO.Write<T> spec) {
    return new WriterImpl(spec);
  }

}
