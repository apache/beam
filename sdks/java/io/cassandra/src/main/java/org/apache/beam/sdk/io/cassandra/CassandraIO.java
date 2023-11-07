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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.SSLOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.google.auto.value.AutoValue;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An IO to read and write from/to Apache Cassandra
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
 * <p>Alternatively, one may use {@code CassandraIO.<Person>readAll()
 * .withCoder(SerializableCoder.of(Person.class))} to query a subset of the Cassandra database by
 * creating a PCollection of {@code CassandraIO.Read<Person>} each with their own query or
 * RingRange.
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
 *
 * <h3>Cassandra Socket Options</h3>
 *
 * <p>The following example illustrates setting timeouts for the Cassandra client:
 *
 * <pre>{@code
 * pipeline.apply(CassandraIO.<Person>read()
 *     .withHosts(Arrays.asList("host1", "host2"))
 *     .withPort(9042)
 *     .withConnectTimeout(1000)
 *     .withReadTimeout(5000)
 *     .withKeyspace("beam")
 *     .withTable("Person")
 *     .withEntity(Person.class)
 *     .withCoder(SerializableCoder.of(Person.class))
 * }</pre>
 */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class CassandraIO {

  private static final Logger LOG = LoggerFactory.getLogger(CassandraIO.class);

  private CassandraIO() {}

  /** Provide a {@link Read} {@link PTransform} to read data from a Cassandra database. */
  public static <T> Read<T> read() {
    return new AutoValue_CassandraIO_Read.Builder<T>().build();
  }

  /** Provide a {@link ReadAll} {@link PTransform} to read data from a Cassandra database. */
  public static <T> ReadAll<T> readAll() {
    return new AutoValue_CassandraIO_ReadAll.Builder<T>().build();
  }

  /** Provide a {@link Write} {@link PTransform} to write data to a Cassandra database. */
  public static <T> Write<T> write() {
    return Write.<T>builder(MutationType.WRITE).build();
  }

  /** Provide a {@link Write} {@link PTransform} to delete data to a Cassandra database. */
  public static <T> Write<T> delete() {
    return Write.<T>builder(MutationType.DELETE).build();
  }

  /**
   * A {@link PTransform} to read data from Apache Cassandra. See {@link CassandraIO} for more
   * information on usage and configuration.
   */
  @AutoValue
  @AutoValue.CopyAnnotations
  @SuppressWarnings({"rawtypes"})
  public abstract static class Read<T> extends PTransform<PBegin, PCollection<T>> {

    abstract @Nullable ValueProvider<List<String>> hosts();

    abstract @Nullable ValueProvider<String> query();

    abstract @Nullable ValueProvider<Integer> port();

    abstract @Nullable ValueProvider<String> keyspace();

    abstract @Nullable ValueProvider<String> table();

    abstract @Nullable Class<T> entity();

    abstract @Nullable Coder<T> coder();

    abstract @Nullable ValueProvider<String> username();

    abstract @Nullable ValueProvider<String> password();

    abstract @Nullable ValueProvider<String> localDc();

    abstract @Nullable ValueProvider<String> consistencyLevel();

    abstract @Nullable ValueProvider<Integer> minNumberOfSplits();

    abstract @Nullable ValueProvider<Integer> connectTimeout();

    abstract @Nullable ValueProvider<Integer> readTimeout();

    abstract @Nullable SerializableFunction<Session, Mapper> mapperFactoryFn();

    @Nullable
    abstract ValueProvider<Set<RingRange>> ringRanges();

    @Nullable
    abstract ValueProvider<SSLOptions> sslOptions();

    abstract Builder<T> builder();

    /** Specify the hosts of the Apache Cassandra instances. */
    public Read<T> withHosts(List<String> hosts) {
      checkArgument(hosts != null, "hosts can not be null");
      checkArgument(!hosts.isEmpty(), "hosts can not be empty");
      return withHosts(ValueProvider.StaticValueProvider.of(hosts));
    }

    /** Specify the hosts of the Apache Cassandra instances. */
    public Read<T> withHosts(ValueProvider<List<String>> hosts) {
      return builder().setHosts(hosts).build();
    }

    /** Specify the port number of the Apache Cassandra instances. */
    public Read<T> withPort(int port) {
      checkArgument(port > 0, "port must be > 0, but was: %s", port);
      return withPort(ValueProvider.StaticValueProvider.of(port));
    }

    /** Specify the port number of the Apache Cassandra instances. */
    public Read<T> withPort(ValueProvider<Integer> port) {
      return builder().setPort(port).build();
    }

    /** Specify the Cassandra keyspace where to read data. */
    public Read<T> withKeyspace(String keyspace) {
      checkArgument(keyspace != null, "keyspace can not be null");
      return withKeyspace(ValueProvider.StaticValueProvider.of(keyspace));
    }

    /** Specify the Cassandra keyspace where to read data. */
    public Read<T> withKeyspace(ValueProvider<String> keyspace) {
      return builder().setKeyspace(keyspace).build();
    }

    /** Specify the Cassandra table where to read data. */
    public Read<T> withTable(String table) {
      checkArgument(table != null, "table can not be null");
      return withTable(ValueProvider.StaticValueProvider.of(table));
    }

    /** Specify the Cassandra table where to read data. */
    public Read<T> withTable(ValueProvider<String> table) {
      return builder().setTable(table).build();
    }

    /** Specify the query to read data. */
    public Read<T> withQuery(String query) {
      checkArgument(query != null && query.length() > 0, "query cannot be null");
      return withQuery(ValueProvider.StaticValueProvider.of(query));
    }

    /** Specify the query to read data. */
    public Read<T> withQuery(ValueProvider<String> query) {
      return builder().setQuery(query).build();
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
      return withUsername(ValueProvider.StaticValueProvider.of(username));
    }

    /** Specify the username for authentication. */
    public Read<T> withUsername(ValueProvider<String> username) {
      return builder().setUsername(username).build();
    }

    /** Specify the password used for authentication. */
    public Read<T> withPassword(String password) {
      checkArgument(password != null, "password can not be null");
      return withPassword(ValueProvider.StaticValueProvider.of(password));
    }

    /** Specify the password used for authentication. */
    public Read<T> withPassword(ValueProvider<String> password) {
      return builder().setPassword(password).build();
    }

    /** Specify the local DC used for the load balancing. */
    public Read<T> withLocalDc(String localDc) {
      checkArgument(localDc != null, "localDc can not be null");
      return withLocalDc(ValueProvider.StaticValueProvider.of(localDc));
    }

    /** Specify the local DC used for the load balancing. */
    public Read<T> withLocalDc(ValueProvider<String> localDc) {
      return builder().setLocalDc(localDc).build();
    }

    /** Specify the consistency level for the request (e.g. ONE, LOCAL_ONE, LOCAL_QUORUM, etc). */
    public Read<T> withConsistencyLevel(String consistencyLevel) {
      checkArgument(consistencyLevel != null, "consistencyLevel can not be null");
      return withConsistencyLevel(ValueProvider.StaticValueProvider.of(consistencyLevel));
    }

    /** Specify the consistency level for the request (e.g. ONE, LOCAL_ONE, LOCAL_QUORUM, etc). */
    public Read<T> withConsistencyLevel(ValueProvider<String> consistencyLevel) {
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
      return withMinNumberOfSplits(ValueProvider.StaticValueProvider.of(minNumberOfSplits));
    }

    /**
     * It's possible that system.size_estimates isn't populated or that the number of splits
     * computed by Beam is still to low for Cassandra to handle it. This setting allows to enforce a
     * minimum number of splits in case Beam cannot compute it correctly.
     */
    public Read<T> withMinNumberOfSplits(ValueProvider<Integer> minNumberOfSplits) {
      return builder().setMinNumberOfSplits(minNumberOfSplits).build();
    }

    /**
     * Specify the Cassandra client connect timeout in ms. See
     * https://docs.datastax.com/en/drivers/java/3.8/com/datastax/driver/core/SocketOptions.html#setConnectTimeoutMillis-int-
     */
    public Read<T> withConnectTimeout(Integer timeout) {
      checkArgument(timeout != null, "Connect timeout can not be null");
      checkArgument(timeout > 0, "Connect timeout must be > 0, but was: %s", timeout);
      return withConnectTimeout(ValueProvider.StaticValueProvider.of(timeout));
    }

    /**
     * Specify the Cassandra client connect timeout in ms. See
     * https://docs.datastax.com/en/drivers/java/3.8/com/datastax/driver/core/SocketOptions.html#setConnectTimeoutMillis-int-
     */
    public Read<T> withConnectTimeout(ValueProvider<Integer> timeout) {
      return builder().setConnectTimeout(timeout).build();
    }

    /**
     * Specify the Cassandra client read timeout in ms. See
     * https://docs.datastax.com/en/drivers/java/3.8/com/datastax/driver/core/SocketOptions.html#setReadTimeoutMillis-int-
     */
    public Read<T> withReadTimeout(Integer timeout) {
      checkArgument(timeout != null, "Read timeout can not be null");
      checkArgument(timeout > 0, "Read timeout must be > 0, but was: %s", timeout);
      return withReadTimeout(ValueProvider.StaticValueProvider.of(timeout));
    }

    /**
     * Specify the Cassandra client read timeout in ms. See
     * https://docs.datastax.com/en/drivers/java/3.8/com/datastax/driver/core/SocketOptions.html#setReadTimeoutMillis-int-
     */
    public Read<T> withReadTimeout(ValueProvider<Integer> timeout) {
      return builder().setReadTimeout(timeout).build();
    }

    /**
     * A factory to create a specific {@link Mapper} for a given Cassandra Session. This is useful
     * to provide mappers that don't rely in Cassandra annotated objects.
     */
    public Read<T> withMapperFactoryFn(SerializableFunction<Session, Mapper> mapperFactory) {
      checkArgument(
          mapperFactory != null,
          "CassandraIO.withMapperFactory" + "(withMapperFactory) called with null value");
      return builder().setMapperFactoryFn(mapperFactory).build();
    }

    public Read<T> withRingRanges(Set<RingRange> ringRange) {
      return withRingRanges(ValueProvider.StaticValueProvider.of(ringRange));
    }

    public Read<T> withRingRanges(ValueProvider<Set<RingRange>> ringRange) {
      return builder().setRingRanges(ringRange).build();
    }

    /**
     * Optionally, specify {@link SSLOptions} configuration to utilize SSL. See
     * https://docs.datastax.com/en/developer/java-driver/3.11/manual/ssl/#jsse-programmatic
     */
    public Read<T> withSsl(SSLOptions sslOptions) {
      return withSsl(ValueProvider.StaticValueProvider.of(sslOptions));
    }

    /**
     * Optionally, specify {@link SSLOptions} configuration to utilize SSL. See
     * https://docs.datastax.com/en/developer/java-driver/3.11/manual/ssl/#jsse-programmatic
     */
    public Read<T> withSsl(ValueProvider<SSLOptions> sslOptions) {
      return builder().setSslOptions(sslOptions).build();
    }

    @Override
    public PCollection<T> expand(PBegin input) {
      checkArgument((hosts() != null && port() != null), "WithHosts() and withPort() are required");
      checkArgument(keyspace() != null, "withKeyspace() is required");
      checkArgument(table() != null, "withTable() is required");
      checkArgument(entity() != null, "withEntity() is required");
      checkArgument(coder() != null, "withCoder() is required");

      PCollection<Read<T>> splits =
          input
              .apply(Create.of(this))
              .apply("Create Splits", ParDo.of(new SplitFn<T>()))
              .setCoder(SerializableCoder.of(new TypeDescriptor<Read<T>>() {}));

      return splits.apply("ReadAll", CassandraIO.<T>readAll().withCoder(coder()));
    }

    private static class SplitFn<T> extends DoFn<Read<T>, Read<T>> {
      @ProcessElement
      public void process(
          @Element CassandraIO.Read<T> read, OutputReceiver<Read<T>> outputReceiver) {
        Set<RingRange> ringRanges = getRingRanges(read);
        for (RingRange rr : ringRanges) {
          outputReceiver.output(read.withRingRanges(ImmutableSet.of(rr)));
        }
      }

      private static <T> Set<RingRange> getRingRanges(Read<T> read) {
        try (Cluster cluster =
            getCluster(
                read.hosts(),
                read.port(),
                read.username(),
                read.password(),
                read.localDc(),
                read.consistencyLevel(),
                read.connectTimeout(),
                read.readTimeout(),
                read.sslOptions())) {
          if (isMurmur3Partitioner(cluster)) {
            LOG.info("Murmur3Partitioner detected, splitting");
            Integer splitCount;
            if (read.minNumberOfSplits() != null && read.minNumberOfSplits().get() != null) {
              splitCount = read.minNumberOfSplits().get();
            } else {
              splitCount = cluster.getMetadata().getAllHosts().size();
            }
            List<BigInteger> tokens =
                cluster.getMetadata().getTokenRanges().stream()
                    .map(tokenRange -> new BigInteger(tokenRange.getEnd().getValue().toString()))
                    .collect(Collectors.toList());
            SplitGenerator splitGenerator =
                new SplitGenerator(cluster.getMetadata().getPartitioner());

            return splitGenerator.generateSplits(splitCount, tokens).stream()
                .flatMap(List::stream)
                .collect(Collectors.toSet());

          } else {
            LOG.warn(
                "Only Murmur3Partitioner is supported for splitting, using an unique source for "
                    + "the read");
            String partitioner = cluster.getMetadata().getPartitioner();
            RingRange totalRingRange =
                RingRange.of(
                    SplitGenerator.getRangeMin(partitioner),
                    SplitGenerator.getRangeMax(partitioner));
            return Collections.singleton(totalRingRange);
          }
        }
      }
    }

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setHosts(ValueProvider<List<String>> hosts);

      abstract Builder<T> setQuery(ValueProvider<String> query);

      abstract Builder<T> setPort(ValueProvider<Integer> port);

      abstract Builder<T> setKeyspace(ValueProvider<String> keyspace);

      abstract Builder<T> setTable(ValueProvider<String> table);

      abstract Builder<T> setEntity(Class<T> entity);

      abstract Optional<Class<T>> entity();

      abstract Builder<T> setCoder(Coder<T> coder);

      abstract Builder<T> setUsername(ValueProvider<String> username);

      abstract Builder<T> setPassword(ValueProvider<String> password);

      abstract Builder<T> setLocalDc(ValueProvider<String> localDc);

      abstract Builder<T> setConsistencyLevel(ValueProvider<String> consistencyLevel);

      abstract Builder<T> setMinNumberOfSplits(ValueProvider<Integer> minNumberOfSplits);

      abstract Builder<T> setConnectTimeout(ValueProvider<Integer> timeout);

      abstract Builder<T> setReadTimeout(ValueProvider<Integer> timeout);

      abstract Builder<T> setMapperFactoryFn(SerializableFunction<Session, Mapper> mapperFactoryFn);

      abstract Optional<SerializableFunction<Session, Mapper>> mapperFactoryFn();

      abstract Builder<T> setRingRanges(ValueProvider<Set<RingRange>> ringRange);

      abstract Builder<T> setSslOptions(ValueProvider<SSLOptions> sslOptions);

      abstract Read<T> autoBuild();

      public Read<T> build() {
        if (!mapperFactoryFn().isPresent() && entity().isPresent()) {
          setMapperFactoryFn(new DefaultObjectMapperFactory(entity().get()));
        }
        return autoBuild();
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
  @AutoValue.CopyAnnotations
  @SuppressWarnings({"rawtypes"})
  public abstract static class Write<T> extends PTransform<PCollection<T>, PDone> {

    abstract @Nullable ValueProvider<List<String>> hosts();

    abstract @Nullable ValueProvider<Integer> port();

    abstract @Nullable ValueProvider<String> keyspace();

    abstract @Nullable Class<T> entity();

    abstract @Nullable ValueProvider<String> username();

    abstract @Nullable ValueProvider<String> password();

    abstract @Nullable ValueProvider<String> localDc();

    abstract @Nullable ValueProvider<String> consistencyLevel();

    abstract MutationType mutationType();

    abstract @Nullable ValueProvider<Integer> connectTimeout();

    abstract @Nullable ValueProvider<Integer> readTimeout();

    abstract @Nullable ValueProvider<SSLOptions> sslOptions();

    abstract @Nullable SerializableFunction<Session, Mapper> mapperFactoryFn();

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
      return withHosts(ValueProvider.StaticValueProvider.of(hosts));
    }

    /** Specify the hosts of the Apache Cassandra instances. */
    public Write<T> withHosts(ValueProvider<List<String>> hosts) {
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
      return withPort(ValueProvider.StaticValueProvider.of(port));
    }

    /** Specify the port number of the Apache Cassandra instances. */
    public Write<T> withPort(ValueProvider<Integer> port) {
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
      return withKeyspace(ValueProvider.StaticValueProvider.of(keyspace));
    }

    /** Specify the Cassandra keyspace where to read data. */
    public Write<T> withKeyspace(ValueProvider<String> keyspace) {
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
      return withUsername(ValueProvider.StaticValueProvider.of(username));
    }

    /** Specify the username for authentication. */
    public Write<T> withUsername(ValueProvider<String> username) {
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
      return withPassword(ValueProvider.StaticValueProvider.of(password));
    }

    /** Specify the password used for authentication. */
    public Write<T> withPassword(ValueProvider<String> password) {
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
      return withLocalDc(ValueProvider.StaticValueProvider.of(localDc));
    }

    /** Specify the local DC used for the load balancing. */
    public Write<T> withLocalDc(ValueProvider<String> localDc) {
      return builder().setLocalDc(localDc).build();
    }

    /** Specify the consistency level for the request (e.g. ONE, LOCAL_ONE, LOCAL_QUORUM, etc). */
    public Write<T> withConsistencyLevel(String consistencyLevel) {
      checkArgument(
          consistencyLevel != null,
          "CassandraIO."
              + getMutationTypeName()
              + "().withConsistencyLevel"
              + "(consistencyLevel) called with null consistencyLevel");
      return withConsistencyLevel(ValueProvider.StaticValueProvider.of(consistencyLevel));
    }

    /** Specify the consistency level for the request (e.g. ONE, LOCAL_ONE, LOCAL_QUORUM, etc). */
    public Write<T> withConsistencyLevel(ValueProvider<String> consistencyLevel) {
      return builder().setConsistencyLevel(consistencyLevel).build();
    }

    /** Cassandra client socket option for connect timeout in ms. */
    public Write<T> withConnectTimeout(Integer timeout) {
      checkArgument(
          (timeout != null && timeout > 0),
          "CassandraIO."
              + getMutationTypeName()
              + "().withConnectTimeout(timeout) called with invalid timeout "
              + "number (%s)",
          timeout);
      return withConnectTimeout(ValueProvider.StaticValueProvider.of(timeout));
    }

    /** Cassandra client socket option for connect timeout in ms. */
    public Write<T> withConnectTimeout(ValueProvider<Integer> timeout) {
      return builder().setConnectTimeout(timeout).build();
    }

    /** Cassandra client socket option to set the read timeout in ms. */
    public Write<T> withReadTimeout(Integer timeout) {
      checkArgument(
          (timeout != null && timeout > 0),
          "CassandraIO."
              + getMutationTypeName()
              + "().withReadTimeout(timeout) called with invalid timeout "
              + "number (%s)",
          timeout);
      return withReadTimeout(ValueProvider.StaticValueProvider.of(timeout));
    }

    /** Cassandra client socket option to set the read timeout in ms. */
    public Write<T> withReadTimeout(ValueProvider<Integer> timeout) {
      return builder().setReadTimeout(timeout).build();
    }

    public Write<T> withMapperFactoryFn(SerializableFunction<Session, Mapper> mapperFactoryFn) {
      checkArgument(
          mapperFactoryFn != null,
          "CassandraIO."
              + getMutationTypeName()
              + "().mapperFactoryFn"
              + "(mapperFactoryFn) called with null value");
      return builder().setMapperFactoryFn(mapperFactoryFn).build();
    }

    /**
     * Optionally, specify {@link SSLOptions} configuration to utilize SSL. See
     * https://docs.datastax.com/en/developer/java-driver/3.11/manual/ssl/#jsse-programmatic
     */
    public Write<T> withSsl(SSLOptions sslOptions) {
      return withSsl(ValueProvider.StaticValueProvider.of(sslOptions));
    }

    /**
     * Optionally, specify {@link SSLOptions} configuration to utilize SSL. See
     * https://docs.datastax.com/en/developer/java-driver/3.11/manual/ssl/#jsse-programmatic
     */
    public Write<T> withSsl(ValueProvider<SSLOptions> sslOptions) {
      return builder().setSslOptions(sslOptions).build();
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

      abstract Builder<T> setHosts(ValueProvider<List<String>> hosts);

      abstract Builder<T> setPort(ValueProvider<Integer> port);

      abstract Builder<T> setKeyspace(ValueProvider<String> keyspace);

      abstract Builder<T> setEntity(Class<T> entity);

      abstract Optional<Class<T>> entity();

      abstract Builder<T> setUsername(ValueProvider<String> username);

      abstract Builder<T> setPassword(ValueProvider<String> password);

      abstract Builder<T> setLocalDc(ValueProvider<String> localDc);

      abstract Builder<T> setConsistencyLevel(ValueProvider<String> consistencyLevel);

      abstract Builder<T> setMutationType(MutationType mutationType);

      abstract Builder<T> setConnectTimeout(ValueProvider<Integer> timeout);

      abstract Builder<T> setReadTimeout(ValueProvider<Integer> timeout);

      abstract Builder<T> setMapperFactoryFn(SerializableFunction<Session, Mapper> mapperFactoryFn);

      abstract Optional<SerializableFunction<Session, Mapper>> mapperFactoryFn();

      abstract Builder<T> setSslOptions(ValueProvider<SSLOptions> sslOptions);

      abstract Write<T> autoBuild(); // not public

      public Write<T> build() {

        if (!mapperFactoryFn().isPresent() && entity().isPresent()) {
          setMapperFactoryFn(new DefaultObjectMapperFactory(entity().get()));
        }
        return autoBuild();
      }
    }
  }

  private static class WriteFn<T> extends DoFn<T, Void> {
    private final Write<T> spec;
    private transient Mutator<T> writer;

    WriteFn(Write<T> spec) {
      this.spec = spec;
    }

    @Setup
    public void setup() {
      writer = new Mutator<>(spec, Mapper::saveAsync, "writes");
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws ExecutionException, InterruptedException {
      writer.mutate(c.element());
    }

    @FinishBundle
    public void finishBundle() throws Exception {
      writer.flush();
    }

    @Teardown
    public void teardown() throws Exception {
      writer.close();
      writer = null;
    }
  }

  private static class DeleteFn<T> extends DoFn<T, Void> {
    private final Write<T> spec;
    private transient Mutator<T> deleter;

    DeleteFn(Write<T> spec) {
      this.spec = spec;
    }

    @Setup
    public void setup() {
      deleter = new Mutator<>(spec, Mapper::deleteAsync, "deletes");
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws ExecutionException, InterruptedException {
      deleter.mutate(c.element());
    }

    @FinishBundle
    public void finishBundle() throws Exception {
      deleter.flush();
    }

    @Teardown
    public void teardown() throws Exception {
      deleter.close();
      deleter = null;
    }
  }

  /** Get a Cassandra cluster using hosts and port. */
  static Cluster getCluster(
      ValueProvider<List<String>> hosts,
      ValueProvider<Integer> port,
      ValueProvider<String> username,
      ValueProvider<String> password,
      ValueProvider<String> localDc,
      ValueProvider<String> consistencyLevel,
      ValueProvider<Integer> connectTimeout,
      ValueProvider<Integer> readTimeout,
      ValueProvider<SSLOptions> sslOptions) {

    Cluster.Builder builder =
        Cluster.builder().addContactPoints(hosts.get().toArray(new String[0])).withPort(port.get());

    if (username != null) {
      builder.withAuthProvider(new PlainTextAuthProvider(username.get(), password.get()));
    }

    DCAwareRoundRobinPolicy.Builder dcAwarePolicyBuilder = new DCAwareRoundRobinPolicy.Builder();
    if (localDc != null) {
      dcAwarePolicyBuilder.withLocalDc(localDc.get());
    }

    builder.withLoadBalancingPolicy(new TokenAwarePolicy(dcAwarePolicyBuilder.build()));

    if (consistencyLevel != null) {
      builder.withQueryOptions(
          new QueryOptions().setConsistencyLevel(ConsistencyLevel.valueOf(consistencyLevel.get())));
    }

    SocketOptions socketOptions = new SocketOptions();

    builder.withSocketOptions(socketOptions);

    if (connectTimeout != null) {
      socketOptions.setConnectTimeoutMillis(connectTimeout.get());
    }

    if (readTimeout != null) {
      socketOptions.setReadTimeoutMillis(readTimeout.get());
    }

    if (sslOptions != null) {
      builder.withSSL(sslOptions.get());
    }

    return builder.build();
  }

  /** Mutator allowing to do side effects into Apache Cassandra database. */
  private static class Mutator<T> {
    /**
     * The threshold of 100 concurrent async queries is a heuristic commonly used by the Apache
     * Cassandra community. There is no real gain to expect in tuning this value.
     */
    private static final int CONCURRENT_ASYNC_QUERIES = 100;

    private final Cluster cluster;
    private final Session session;
    private final SerializableFunction<Session, Mapper> mapperFactoryFn;
    private List<Future<Void>> mutateFutures;
    private final BiFunction<Mapper<T>, T, Future<Void>> mutator;
    private final String operationName;

    Mutator(Write<T> spec, BiFunction<Mapper<T>, T, Future<Void>> mutator, String operationName) {
      this.cluster =
          getCluster(
              spec.hosts(),
              spec.port(),
              spec.username(),
              spec.password(),
              spec.localDc(),
              spec.consistencyLevel(),
              spec.connectTimeout(),
              spec.readTimeout(),
              spec.sslOptions());
      this.session = cluster.connect(spec.keyspace().get());
      this.mapperFactoryFn = spec.mapperFactoryFn();
      this.mutateFutures = new ArrayList<>();
      this.mutator = mutator;
      this.operationName = operationName;
    }

    /**
     * Mutate the entity to the Cassandra instance, using {@link Mapper} obtained with the Mapper
     * factory, the DefaultObjectMapperFactory uses {@link
     * com.datastax.driver.mapping.MappingManager}. This method uses {@link
     * Mapper#saveAsync(Object)} method, which is asynchronous. Beam will wait for all futures to
     * complete, to guarantee all writes have succeeded.
     */
    void mutate(T entity) throws ExecutionException, InterruptedException {
      Mapper<T> mapper = mapperFactoryFn.apply(session);
      this.mutateFutures.add(mutator.apply(mapper, entity));
      if (this.mutateFutures.size() == CONCURRENT_ASYNC_QUERIES) {
        // We reached the max number of allowed in flight queries.
        // Write methods are synchronous in Beam,
        // so we wait for each async query to return before exiting.
        LOG.debug(
            "Waiting for a batch of {} Cassandra {} to be executed...",
            CONCURRENT_ASYNC_QUERIES,
            operationName);
        waitForFuturesToFinish();
        this.mutateFutures = new ArrayList<>();
      }
    }

    void flush() throws ExecutionException, InterruptedException {
      if (this.mutateFutures.size() > 0) {
        // Waiting for the last in flight async queries to return before finishing the bundle.
        waitForFuturesToFinish();
      }
    }

    void close() {
      if (session != null) {
        session.close();
      }
      if (cluster != null) {
        cluster.close();
      }
    }

    private void waitForFuturesToFinish() throws ExecutionException, InterruptedException {
      for (Future<Void> future : mutateFutures) {
        future.get();
      }
    }
  }

  /**
   * A {@link PTransform} to read data from Apache Cassandra. See {@link CassandraIO} for more
   * information on usage and configuration.
   */
  @AutoValue
  public abstract static class ReadAll<T> extends PTransform<PCollection<Read<T>>, PCollection<T>> {
    @AutoValue.Builder
    abstract static class Builder<T> {

      abstract Builder<T> setCoder(Coder<T> coder);

      abstract ReadAll<T> autoBuild();

      public ReadAll<T> build() {
        return autoBuild();
      }
    }

    @Nullable
    abstract Coder<T> coder();

    abstract Builder<T> builder();

    /** Specify the {@link Coder} used to serialize the entity in the {@link PCollection}. */
    public ReadAll<T> withCoder(Coder<T> coder) {
      checkArgument(coder != null, "coder can not be null");
      return builder().setCoder(coder).build();
    }

    @Override
    public PCollection<T> expand(PCollection<Read<T>> input) {
      checkArgument(coder() != null, "withCoder() is required");
      return input
          .apply("Reshuffle", Reshuffle.viaRandomKey())
          .apply("Read", ParDo.of(new ReadFn<>()))
          .setCoder(this.coder());
    }
  }

  /**
   * Check if the current partitioner is the Murmur3 (default in Cassandra version newer than 2).
   */
  @VisibleForTesting
  private static boolean isMurmur3Partitioner(Cluster cluster) {
    return MURMUR3PARTITIONER.equals(cluster.getMetadata().getPartitioner());
  }

  private static final String MURMUR3PARTITIONER = "org.apache.cassandra.dht.Murmur3Partitioner";
}
