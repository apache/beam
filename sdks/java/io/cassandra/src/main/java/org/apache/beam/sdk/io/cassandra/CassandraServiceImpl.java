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
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.google.common.util.concurrent.ListenableFuture;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.vendor.guava.v20_0.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Iterators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** An implementation of the {@link CassandraService} that actually use a Cassandra instance. */
public class CassandraServiceImpl<T> implements CassandraService<T> {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraServiceImpl.class);
  private static final String MURMUR3PARTITIONER = "org.apache.cassandra.dht.Murmur3Partitioner";

  private class CassandraReaderImpl extends BoundedSource.BoundedReader<T> {
    private final CassandraIO.CassandraSource<T> source;
    private Cluster cluster;
    private Session session;
    private Iterator<T> iterator;
    private T current;

    CassandraReaderImpl(CassandraIO.CassandraSource<T> source) {
      this.source = source;
    }

    @Override
    public boolean start() {
      LOG.debug("Starting Cassandra reader");
      cluster =
          getCluster(
              source.spec.hosts(),
              source.spec.port(),
              source.spec.username(),
              source.spec.password(),
              source.spec.localDc(),
              source.spec.consistencyLevel());
      session = cluster.connect(source.spec.keyspace());
      LOG.debug("Queries: " + source.splitQueries);
      List<ResultSetFuture> futures = new ArrayList<>();
      for (String query : source.splitQueries) {
        futures.add(session.executeAsync(query));
      }

      final MappingManager mappingManager = new MappingManager(session);
      Mapper mapper = mappingManager.mapper(source.spec.entity());

      for (ResultSetFuture result : futures) {
        if (iterator == null) {
          iterator = mapper.map(result.getUninterruptibly()).iterator();
        } else {
          iterator = Iterators.concat(iterator, mapper.map(result.getUninterruptibly()).iterator());
        }
      }

      return advance();
    }

    @Override
    public boolean advance() {
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
  public CassandraReaderImpl createReader(CassandraIO.CassandraSource<T> source) {
    return new CassandraReaderImpl(source);
  }

  @Override
  public long getEstimatedSizeBytes(CassandraIO.Read<T> spec) {
    try (Cluster cluster =
        getCluster(
            spec.hosts(),
            spec.port(),
            spec.username(),
            spec.password(),
            spec.localDc(),
            spec.consistencyLevel())) {
      if (isMurmur3Partitioner(cluster)) {
        try {
          List<TokenRange> tokenRanges = getTokenRanges(cluster, spec.keyspace(), spec.table());
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
   * Actually estimate the size of the data to read on the cluster, based on the given token ranges
   * to address.
   */
  @VisibleForTesting
  static long getEstimatedSizeBytes(List<TokenRange> tokenRanges) {
    long size = 0L;
    for (TokenRange tokenRange : tokenRanges) {
      size = size + tokenRange.meanPartitionSize * tokenRange.partitionCount;
    }
    return Math.round(size / getRingFraction(tokenRanges));
  }

  @Override
  public List<BoundedSource<T>> split(CassandraIO.Read<T> spec, long desiredBundleSizeBytes) {
    try (Cluster cluster =
        getCluster(
            spec.hosts(),
            spec.port(),
            spec.username(),
            spec.password(),
            spec.localDc(),
            spec.consistencyLevel())) {
      if (isMurmur3Partitioner(cluster)) {
        LOG.info("Murmur3Partitioner detected, splitting");
        return split(spec, desiredBundleSizeBytes, getEstimatedSizeBytes(spec), cluster);
      } else {
        LOG.warn(
            "Only Murmur3Partitioner is supported for splitting, using an unique source for "
                + "the read");
        String splitQuery = QueryBuilder.select().from(spec.keyspace(), spec.table()).toString();
        return Collections.singletonList(
            new CassandraIO.CassandraSource<>(spec, Collections.singletonList(splitQuery)));
      }
    }
  }

  /**
   * Compute the number of splits based on the estimated size and the desired bundle size, and
   * create several sources.
   */
  private List<BoundedSource<T>> split(
      CassandraIO.Read<T> spec,
      long desiredBundleSizeBytes,
      long estimatedSizeBytes,
      Cluster cluster) {
    long numSplits =
        getNumSplits(desiredBundleSizeBytes, estimatedSizeBytes, spec.minNumberOfSplits());
    LOG.info("Number of desired splits is {}", numSplits);

    SplitGenerator splitGenerator = new SplitGenerator(cluster.getMetadata().getPartitioner());
    List<BigInteger> tokens =
        cluster.getMetadata().getTokenRanges().stream()
            .map(tokenRange -> new BigInteger(tokenRange.getEnd().getValue().toString()))
            .collect(Collectors.toList());
    List<List<RingRange>> splits = splitGenerator.generateSplits(numSplits, tokens);
    LOG.info("{} splits were actually generated", splits.size());

    final String partitionKey =
        cluster.getMetadata().getKeyspace(spec.keyspace()).getTable(spec.table()).getPartitionKey()
            .stream()
            .map(ColumnMetadata::getName)
            .collect(Collectors.joining(","));

    List<BoundedSource<T>> sources = new ArrayList<>();
    for (List<RingRange> split : splits) {
      List<String> queries = new ArrayList<>();
      for (RingRange range : split) {
        Select.Where builder = QueryBuilder.select().from(spec.keyspace(), spec.table()).where();
        if (range.isWrapping()) {
          // A wrapping range is one that overlaps from the end of the partitioner range and its
          // start (ie : when the start token of the split is greater than the end token)
          // We need to generate two queries here : one that goes from the start token to the end of
          // the partitioner range, and the other from the start of the partitioner range to the
          // end token of the split.
          builder = builder.and(QueryBuilder.gte("token(" + partitionKey + ")", range.getStart()));
          String query = builder.toString();
          LOG.debug("Cassandra generated read query : {}", query);
          queries.add(query);

          // Generation of the second query of the wrapping range
          builder = QueryBuilder.select().from(spec.keyspace(), spec.table()).where();
          builder = builder.and(QueryBuilder.lt("token(" + partitionKey + ")", range.getEnd()));
          query = builder.toString();
          LOG.debug("Cassandra generated read query : {}", query);
          queries.add(query);
        } else {
          builder = builder.and(QueryBuilder.gte("token(" + partitionKey + ")", range.getStart()));
          builder = builder.and(QueryBuilder.lt("token(" + partitionKey + ")", range.getEnd()));
          String query = builder.toString();
          LOG.debug("Cassandra generated read query : {}", query);
          queries.add(query);
        }
      }
      sources.add(new CassandraIO.CassandraSource<>(spec, queries));
    }
    return sources;
  }

  private static long getNumSplits(
      long desiredBundleSizeBytes, long estimatedSizeBytes, @Nullable Integer minNumberOfSplits) {
    long numSplits = desiredBundleSizeBytes > 0 ? (estimatedSizeBytes / desiredBundleSizeBytes) : 1;
    if (numSplits <= 0) {
      LOG.warn("Number of splits is less than 0 ({}), fallback to 1", numSplits);
      numSplits = 1;
    }
    return minNumberOfSplits != null ? Math.max(numSplits, minNumberOfSplits) : numSplits;
  }

  /** Get a Cassandra cluster using hosts and port. */
  private Cluster getCluster(
      List<String> hosts,
      int port,
      String username,
      String password,
      String localDc,
      String consistencyLevel) {
    Cluster.Builder builder =
        Cluster.builder().addContactPoints(hosts.toArray(new String[0])).withPort(port);

    if (username != null) {
      builder.withAuthProvider(new PlainTextAuthProvider(username, password));
    }

    DCAwareRoundRobinPolicy.Builder dcAwarePolicyBuilder = new DCAwareRoundRobinPolicy.Builder();
    if (localDc != null) {
      dcAwarePolicyBuilder.withLocalDc(localDc);
    }

    builder.withLoadBalancingPolicy(new TokenAwarePolicy(dcAwarePolicyBuilder.build()));

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
                new BigInteger(row.getString("range_start")),
                new BigInteger(row.getString("range_end")));
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

  /** Compute the percentage of token addressed compared with the whole tokens in the cluster. */
  @VisibleForTesting
  static double getRingFraction(List<TokenRange> tokenRanges) {
    double ringFraction = 0;
    for (TokenRange tokenRange : tokenRanges) {
      ringFraction =
          ringFraction
              + (distance(tokenRange.rangeStart, tokenRange.rangeEnd).doubleValue()
                  / SplitGenerator.getRangeSize(MURMUR3PARTITIONER).doubleValue());
    }
    return ringFraction;
  }

  /** Measure distance between two tokens. */
  @VisibleForTesting
  static BigInteger distance(BigInteger left, BigInteger right) {
    return (right.compareTo(left) > 0)
        ? right.subtract(left)
        : right.subtract(left).add(SplitGenerator.getRangeSize(MURMUR3PARTITIONER));
  }

  /**
   * Check if the current partitioner is the Murmur3 (default in Cassandra version newer than 2).
   */
  @VisibleForTesting
  static boolean isMurmur3Partitioner(Cluster cluster) {
    return MURMUR3PARTITIONER.equals(cluster.getMetadata().getPartitioner());
  }

  /**
   * Represent a token range in Cassandra instance, wrapping the partition count, size and token
   * range.
   */
  @VisibleForTesting
  static class TokenRange {
    private final long partitionCount;
    private final long meanPartitionSize;
    private final BigInteger rangeStart;
    private final BigInteger rangeEnd;

    public TokenRange(
        long partitionCount, long meanPartitionSize, BigInteger rangeStart, BigInteger rangeEnd) {
      this.partitionCount = partitionCount;
      this.meanPartitionSize = meanPartitionSize;
      this.rangeStart = rangeStart;
      this.rangeEnd = rangeEnd;
    }
  }

  /** Writer storing an entity into Apache Cassandra database. */
  class WriterImpl extends MutatorImpl implements Writer<T> {

    WriterImpl(CassandraIO.Mutate<T> spec) {
      super(spec, Mapper::saveAsync, "writes");
    }

    @Override
    public void write(T entity) throws ExecutionException, InterruptedException {
      mutate(entity);
    }
  }

  /** Mutator allowing to do side effects into Apache Cassandra database. */
  abstract class MutatorImpl {
    /**
     * The threshold of 100 concurrent async queries is a heuristic commonly used by the Apache
     * Cassandra community. There is no real gain to expect in tuning this value.
     */
    private static final int CONCURRENT_ASYNC_QUERIES = 100;

    private final CassandraIO.Mutate<T> spec;

    private final Cluster cluster;
    private final Session session;
    private final MappingManager mappingManager;
    private List<ListenableFuture<Void>> mutateFutures;
    private final BiFunction<Mapper<T>, T, ListenableFuture<Void>> mutator;
    private final String operationName;

    MutatorImpl(
        CassandraIO.Mutate<T> spec,
        BiFunction<Mapper<T>, T, ListenableFuture<Void>> mutator,
        String operationName) {
      this.spec = spec;
      this.cluster =
          getCluster(
              spec.hosts(),
              spec.port(),
              spec.username(),
              spec.password(),
              spec.localDc(),
              spec.consistencyLevel());
      this.session = cluster.connect(spec.keyspace());
      this.mappingManager = new MappingManager(session);
      this.mutateFutures = new ArrayList<>();
      this.mutator = mutator;
      this.operationName = operationName;
    }

    /**
     * Mutate the entity to the Cassandra instance, using {@link Mapper} obtained with the {@link
     * MappingManager}. This method uses {@link Mapper#saveAsync(Object)} method, which is
     * asynchronous. Beam will wait for all futures to complete, to guarantee all writes have
     * succeeded.
     */
    void mutate(T entity) throws ExecutionException, InterruptedException {
      Mapper<T> mapper = (Mapper<T>) mappingManager.mapper(entity.getClass());
      this.mutateFutures.add(mutator.apply(mapper, entity));
      if (this.mutateFutures.size() == CONCURRENT_ASYNC_QUERIES) {
        // We reached the max number of allowed in flight queries.
        // Write methods are synchronous in Beam as stated by the CassandraService interface,
        // so we wait for each async query to return before exiting.
        LOG.debug(
            "Waiting for a batch of {} Cassandra {} to be executed...",
            CONCURRENT_ASYNC_QUERIES,
            operationName);
        waitForFuturesToFinish();
        this.mutateFutures = new ArrayList<>();
      }
    }

    public void close() throws ExecutionException, InterruptedException {
      if (this.mutateFutures.size() > 0) {
        // Waiting for the last in flight async queries to return before finishing the bundle.
        waitForFuturesToFinish();
      }

      if (session != null) {
        session.close();
      }
      if (cluster != null) {
        cluster.close();
      }
    }

    private void waitForFuturesToFinish() throws ExecutionException, InterruptedException {
      for (ListenableFuture<Void> future : mutateFutures) {
        future.get();
      }
    }
  }

  @Override
  public Writer<T> createWriter(CassandraIO.Mutate<T> spec) {
    return new WriterImpl(spec);
  }

  /** Deleter storing an entity into Apache Cassandra database. */
  protected class DeleterImpl extends MutatorImpl implements Deleter<T> {

    DeleterImpl(CassandraIO.Mutate<T> spec) {
      super(spec, Mapper::deleteAsync, "deletes");
    }

    @Override
    public void delete(T entity) throws ExecutionException, InterruptedException {
      mutate(entity);
    }
  }

  @Override
  public Deleter<T> createDeleter(CassandraIO.Mutate<T> spec) {
    return new DeleterImpl(spec);
  }
}
