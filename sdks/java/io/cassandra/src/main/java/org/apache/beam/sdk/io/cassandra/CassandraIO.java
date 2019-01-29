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

import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkState;

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
import com.google.auto.value.AutoValue;
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
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.vendor.guava.v20_0.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Iterators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An IO to read from Apache Cassandra.
 *
 * <h3>Reading from Apache Cassandra</h3>
 *
 * <p>{@code CassandraIO} provides a source to read and returns a bounded collection of entities as
 * {@code PCollection<Entity>}. An entity is built by Cassandra mapper ({@code
 * com.datastax.driver.mapping.EntityMapper}) based on a POJO containing annotations (as described
 * http://docs.datastax .com/en/developer/java-driver/2.1/manual/object_mapper/creating/").
 *
 * <p>The following example illustrates various options for configuring the IO:
 *
 * <pre>{@code
 * pipeline.apply(CassandraIO.<Person>read()
 *     .withHosts(Arrays.asList("host1", "host2"))
 *     .withPort(9042)
 *     .withKeyspace("beam")
 *     .withTable("Person")
 *     .withEntity(Person.class)
 *     .withCoder(SerializableCoder.of(Person.class))
 *     // above options are the minimum set, returns PCollection<Person>
 *
 * }</pre>
 *
 * <h3>Writing to Apache Cassandra</h3>
 *
 * <p>{@code CassandraIO} provides a sink to write a collection of entities to Apache Cassandra.
 *
 * <p>The following example illustrates various options for configuring the IO write:
 *
 * <pre>{@code
 * pipeline
 *    .apply(...) // provides a PCollection<Person> where Person is an entity
 *    .apply(CassandraIO.<Person>write()
 *        .withHosts(Arrays.asList("host1", "host2"))
 *        .withPort(9042)
 *        .withKeyspace("beam")
 *        .withEntity(Person.class));
 * }</pre>
 */
@Experimental(Experimental.Kind.SOURCE_SINK)
public class CassandraIO {

  private static final Logger LOG = LoggerFactory.getLogger(CassandraIO.class);
  private CassandraIO() {}

  /** Provide a {@link Read} {@link PTransform} to read data from a Cassandra database. */
  public static <T> Read<T> read() {
    return new AutoValue_CassandraIO_Read.Builder<T>()
        .build();
  }

  /** Provide a {@link Write} {@link PTransform} to write data to a Cassandra database. */
  public static <T> Write<T> write() {
    return Write.<T>builder(MutationType.WRITE)
        .build();
  }

  /** Provide a {@link Write} {@link PTransform} to delete data to a Cassandra database. */
  public static <T> Write<T> delete() {
    return Write.<T>builder(MutationType.DELETE)
        .build();
  }

  /**
   * A {@link PTransform} to read data from Apache Cassandra. See {@link CassandraIO} for more
   * information on usage and configuration.
   */
  @AutoValue
  public abstract static class Read<T> extends PTransform<PBegin, PCollection<T>> {
    @Nullable
    abstract List<String> hosts();

    @Nullable
    abstract Integer port();

    @Nullable
    abstract String keyspace();

    @Nullable
    abstract String table();

    @Nullable
    abstract Class<T> entity();

    @Nullable
    abstract Coder<T> coder();

    @Nullable
    abstract String username();

    @Nullable
    abstract String password();

    @Nullable
    abstract String localDc();

    @Nullable
    abstract String consistencyLevel();

    @Nullable
    abstract Integer minNumberOfSplits();

    abstract Builder<T> builder();

    /** Specify the hosts of the Apache Cassandra instances. */
    public Read<T> withHosts(List<String> hosts) {
      checkArgument(hosts != null, "hosts can not be null");
      checkArgument(!hosts.isEmpty(), "hosts can not be empty");
      return builder().setHosts(hosts).build();
    }

    /** Specify the port number of the Apache Cassandra instances. */
    public Read<T> withPort(int port) {
      checkArgument(port > 0, "port must be > 0, but was: %s", port);
      return builder().setPort(port).build();
    }

    /** Specify the Cassandra keyspace where to read data. */
    public Read<T> withKeyspace(String keyspace) {
      checkArgument(keyspace != null, "keyspace can not be null");
      return builder().setKeyspace(keyspace).build();
    }

    /** Specify the Cassandra table where to read data. */
    public Read<T> withTable(String table) {
      checkArgument(table != null, "table can not be null");
      return builder().setTable(table).build();
    }

    /**
     * Specify the entity class (annotated POJO). The {@link CassandraIO} will read the data and
     * convert the data as entity instances. The {@link PCollection} resulting from the read will
     * contains entity elements.
     */
    public Read<T> withEntity(Class<T> entity) {
      checkArgument(entity != null, "entity can not be null");
      return builder().setEntity(entity).build();
    }

    /** Specify the {@link Coder} used to serialize the entity in the {@link PCollection}. */
    public Read<T> withCoder(Coder<T> coder) {
      checkArgument(coder != null, "coder can not be null");
      return builder().setCoder(coder).build();
    }

    /** Specify the username for authentication. */
    public Read<T> withUsername(String username) {
      checkArgument(username != null, "username can not be null");
      return builder().setUsername(username).build();
    }

    /** Specify the password for authentication. */
    public Read<T> withPassword(String password) {
      checkArgument(password != null, "password can not be null");
      return builder().setPassword(password).build();
    }

    /** Specify the local DC used for the load balancing. */
    public Read<T> withLocalDc(String localDc) {
      checkArgument(localDc != null, "localDc can not be null");
      return builder().setLocalDc(localDc).build();
    }

    public Read<T> withConsistencyLevel(String consistencyLevel) {
      checkArgument(consistencyLevel != null, "consistencyLevel can not be null");
      return builder().setConsistencyLevel(consistencyLevel).build();
    }

    /**
     * It's possible that system.size_estimates isn't populated or that the number of splits
     * computed by Beam is still to low for Cassandra to handle it. This setting allows to enforce a
     * minimum number of splits in case Beam cannot compute it correctly.
     */
    public Read<T> withMinNumberOfSplits(Integer minNumberOfSplits) {
      checkArgument(minNumberOfSplits != null, "minNumberOfSplits can not be null");
      checkArgument(minNumberOfSplits > 0, "minNumberOfSplits must be greater than 0");
      return builder().setMinNumberOfSplits(minNumberOfSplits).build();
    }

    @Override
    public PCollection<T> expand(PBegin input) {
      checkArgument(
          (hosts() != null && port() != null),
          "Either withHosts() and withPort(), is required");
      checkArgument(keyspace() != null, "withKeyspace() is required");
      checkArgument(table() != null, "withTable() is required");
      checkArgument(entity() != null, "withEntity() is required");
      checkArgument(coder() != null, "withCoder() is required");

      return input.apply(org.apache.beam.sdk.io.Read.from(new CassandraSource<>(this, null)));
    }

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setHosts(List<String> hosts);

      abstract Builder<T> setPort(Integer port);

      abstract Builder<T> setKeyspace(String keyspace);

      abstract Builder<T> setTable(String table);

      abstract Builder<T> setEntity(Class<T> entity);

      abstract Builder<T> setCoder(Coder<T> coder);

      abstract Builder<T> setUsername(String username);

      abstract Builder<T> setPassword(String password);

      abstract Builder<T> setLocalDc(String localDc);

      abstract Builder<T> setConsistencyLevel(String consistencyLevel);

      abstract Builder<T> setMinNumberOfSplits(Integer minNumberOfSplits);

      abstract Read<T> build();
    }
  }

  @VisibleForTesting
  static class CassandraSource<T> extends BoundedSource<T> {
    final Read<T> spec;
    final List<String> splitQueries;
    private static final String MURMUR3PARTITIONER = "org.apache.cassandra.dht.Murmur3Partitioner";

    CassandraSource(Read<T> spec, List<String> splitQueries) {
      this.spec = spec;
      this.splitQueries = splitQueries;
    }

    @Override
    public Coder<T> getOutputCoder() {
      return spec.coder();
    }

    @Override
    public BoundedReader<T> createReader(PipelineOptions pipelineOptions) {
      return new CassandraReader(this);
    }

    @Override
    public List<BoundedSource<T>> split(
        long desiredBundleSizeBytes, PipelineOptions pipelineOptions) {
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

    @Override
    public long getEstimatedSizeBytes(PipelineOptions pipelineOptions) {
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
            List<TokenRange> tokenRanges = getTokenRanges(cluster, spec.keyspace(), spec
                .table());
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

    private long getEstimatedSizeBytes(CassandraIO.Read<T> spec) {
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
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      if (spec.hosts() != null) {
        builder.add(DisplayData.item("hosts", spec.hosts().toString()));
      }
      if (spec.port() != null) {
        builder.add(DisplayData.item("port", spec.port()));
      }
      builder.addIfNotNull(DisplayData.item("keyspace", spec.keyspace()));
      builder.addIfNotNull(DisplayData.item("table", spec.table()));
      builder.addIfNotNull(DisplayData.item("username", spec.username()));
      builder.addIfNotNull(DisplayData.item("localDc", spec.localDc()));
      builder.addIfNotNull(DisplayData.item("consistencyLevel", spec.consistencyLevel()));
    }
    // ------------- CASSANDRA UTIL METHODS ---------------//

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

    /**
     * Check if the current partitioner is the Murmur3 (default in Cassandra version newer than 2).
     */
    @VisibleForTesting
    static boolean isMurmur3Partitioner(Cluster cluster) {
      return MURMUR3PARTITIONER.equals(cluster.getMetadata().getPartitioner());
    }

    /** Measure distance between two tokens. */
    @VisibleForTesting
    static BigInteger distance(BigInteger left, BigInteger right) {
      return (right.compareTo(left) > 0)
          ? right.subtract(left)
          : right.subtract(left).add(SplitGenerator.getRangeSize(MURMUR3PARTITIONER));
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

      TokenRange(
          long partitionCount, long meanPartitionSize, BigInteger rangeStart, BigInteger rangeEnd) {
        this.partitionCount = partitionCount;
        this.meanPartitionSize = meanPartitionSize;
        this.rangeStart = rangeStart;
        this.rangeEnd = rangeEnd;
      }
    }

    private class CassandraReader extends BoundedSource.BoundedReader<T> {
      private final CassandraIO.CassandraSource<T> source;
      private Cluster cluster;
      private Session session;
      private Iterator<T> iterator;
      private T current;

      CassandraReader(CassandraSource<T> source) {
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

  }

  /** Specify the mutation type: either write or delete. */
  public enum MutationType {
    WRITE,
    DELETE
  }

  /**
   * A {@link PTransform} to mutate into Apache Cassandra. See {@link CassandraIO} for details on
   * usage and configuration.
   */
  @AutoValue
  public abstract static class Write<T> extends PTransform<PCollection<T>, PDone> {
    @Nullable
    abstract List<String> hosts();

    @Nullable
    abstract Integer port();

    @Nullable
    abstract String keyspace();

    @Nullable
    abstract Class<T> entity();

    @Nullable
    abstract String username();

    @Nullable
    abstract String password();

    @Nullable
    abstract String localDc();

    @Nullable
    abstract String consistencyLevel();

    abstract MutationType mutationType();

    abstract Builder<T> builder();

    static <T> Builder<T> builder(MutationType mutationType) {
      return new AutoValue_CassandraIO_Write.Builder<T>().setMutationType(mutationType);
    }

    /** Specify the Cassandra instance hosts where to write data. */
    public Write<T> withHosts(List<String> hosts) {
      checkArgument(
          hosts != null,
          "CassandraIO." + getMutationTypeName() + "().withHosts(hosts) called with null hosts");
      checkArgument(
          !hosts.isEmpty(),
          "CassandraIO."
              + getMutationTypeName()
              + "().withHosts(hosts) called with empty "
              + "hosts list");
      return builder().setHosts(hosts).build();
    }

    /** Specify the Cassandra instance port number where to write data. */
    public Write<T> withPort(int port) {
      checkArgument(
          port > 0,
          "CassandraIO."
              + getMutationTypeName()
              + "().withPort(port) called with invalid port "
              + "number (%s)",
          port);
      return builder().setPort(port).build();
    }

    /** Specify the Cassandra keyspace where to write data. */
    public Write<T> withKeyspace(String keyspace) {
      checkArgument(
          keyspace != null,
          "CassandraIO."
              + getMutationTypeName()
              + "().withKeyspace(keyspace) called with "
              + "null keyspace");
      return builder().setKeyspace(keyspace).build();
    }

    /**
     * Specify the entity class in the input {@link PCollection}. The {@link CassandraIO} will map
     * this entity to the Cassandra table thanks to the annotations.
     */
    public Write<T> withEntity(Class<T> entity) {
      checkArgument(
          entity != null,
          "CassandraIO."
              + getMutationTypeName()
              + "().withEntity(entity) called with null "
              + "entity");
      return builder().setEntity(entity).build();
    }

    /** Specify the username used for authentication. */
    public Write<T> withUsername(String username) {
      checkArgument(
          username != null,
          "CassandraIO."
              + getMutationTypeName()
              + "().withUsername(username) called with "
              + "null username");
      return builder().setUsername(username).build();
    }

    /** Specify the password used for authentication. */
    public Write<T> withPassword(String password) {
      checkArgument(
          password != null,
          "CassandraIO."
              + getMutationTypeName()
              + "().withPassword(password) called with "
              + "null password");
      return builder().setPassword(password).build();
    }

    /** Specify the local DC used by the load balancing policy. */
    public Write<T> withLocalDc(String localDc) {
      checkArgument(
          localDc != null,
          "CassandraIO."
              + getMutationTypeName()
              + "().withLocalDc(localDc) called with null"
              + " localDc");
      return builder().setLocalDc(localDc).build();
    }

    public Write<T> withConsistencyLevel(String consistencyLevel) {
      checkArgument(
          consistencyLevel != null,
          "CassandraIO."
              + getMutationTypeName()
              + "().withConsistencyLevel"
              + "(consistencyLevel) called with null consistencyLevel");
      return builder().setConsistencyLevel(consistencyLevel).build();
    }

    @Override
    public void validate(PipelineOptions pipelineOptions) {
      checkState(
          hosts() != null,
          "CassandraIO."
              + getMutationTypeName()
              + "() requires a list of hosts to be set via withHosts(hosts)");
      checkState(
          port() != null,
          "CassandraIO."
              + getMutationTypeName()
              + "() requires a "
              + "valid port number to be set via withPort(port)");
      checkState(
          keyspace() != null,
          "CassandraIO."
              + getMutationTypeName()
              + "() requires a keyspace to be set via "
              + "withKeyspace(keyspace)");
      checkState(
          entity() != null,
          "CassandraIO."
              + getMutationTypeName()
              + "() requires an entity to be set via "
              + "withEntity(entity)");
    }

    @Override
    public PDone expand(PCollection<T> input) {
      if (mutationType() == MutationType.DELETE) {
        input.apply(ParDo.of(new DeleteFn<>(this)));
      } else {
        input.apply(ParDo.of(new WriteFn<>(this)));
      }
      return PDone.in(input.getPipeline());
    }

    private String getMutationTypeName() {
      return mutationType() == null
          ? MutationType.WRITE.name().toLowerCase()
          : mutationType().name().toLowerCase();
    }

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setHosts(List<String> hosts);

      abstract Builder<T> setPort(Integer port);

      abstract Builder<T> setKeyspace(String keyspace);

      abstract Builder<T> setEntity(Class<T> entity);

      abstract Builder<T> setUsername(String username);

      abstract Builder<T> setPassword(String password);

      abstract Builder<T> setLocalDc(String localDc);

      abstract Builder<T> setConsistencyLevel(String consistencyLevel);

      abstract Builder<T> setMutationType(MutationType mutationType);

      abstract Write<T> build();
    }
  }

  private static class WriteFn<T> extends DoFn<T, Void> {
    private final Write<T> spec;
    private Writer<T> writer;

    WriteFn(Write<T> spec) {
      this.spec = spec;
    }

    @Setup
    public void setup() {
      writer = new Writer<>(spec);
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws ExecutionException, InterruptedException {
      writer.write(c.element());
    }

    @Teardown
    public void teardown() throws Exception {
      writer.close();
      writer = null;
    }
  }

  private static class DeleteFn<T> extends DoFn<T, Void> {
    private final Write<T> spec;
    private Deleter<T> deleter;

    DeleteFn(Write<T> spec) {
      this.spec = spec;
    }

    @Setup
    public void setup() {
      deleter = new Deleter<>(spec);
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws ExecutionException, InterruptedException {
      deleter.delete(c.element());
    }

    @Teardown
    public void teardown() throws Exception {
      deleter.close();
      deleter = null;
    }
  }
  /** Get a Cassandra cluster using hosts and port. */
  private static Cluster getCluster(
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


  /** Mutator allowing to do side effects into Apache Cassandra database. */
  abstract static class Mutator<T> {
    /**
     * The threshold of 100 concurrent async queries is a heuristic commonly used by the Apache
     * Cassandra community. There is no real gain to expect in tuning this value.
     */
    private static final int CONCURRENT_ASYNC_QUERIES = 100;

    private final Write<T> spec;

    private final Cluster cluster;
    private final Session session;
    private final MappingManager mappingManager;
    private List<ListenableFuture<Void>> mutateFutures;
    private final BiFunction<Mapper<T>, T, ListenableFuture<Void>> mutator;
    private final String operationName;

    Mutator(
        Write<T> spec,
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

  /** Writer storing an entity into Apache Cassandra database. */
  private static class Writer<T> extends Mutator<T> {

    Writer(Write<T> spec) {
      super(spec, Mapper::saveAsync, "writes");
    }

    public void write(T entity) throws ExecutionException, InterruptedException {
      mutate(entity);
    }
  }

  /** Deleter storing an entity into Apache Cassandra database. */
  private static class Deleter<T> extends Mutator<T> {

    Deleter(Write<T> spec) {
      super(spec, Mapper::deleteAsync, "deletes");
    }

    public void delete(T entity) throws ExecutionException, InterruptedException {
      mutate(entity);
    }
  }

}
