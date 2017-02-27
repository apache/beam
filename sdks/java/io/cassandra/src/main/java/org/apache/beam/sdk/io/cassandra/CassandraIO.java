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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;
import com.datastax.driver.core.policies.LatencyAwarePolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.google.auto.value.AutoValue;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import javax.annotation.Nullable;
import javax.sql.DataSource;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;




/**
 * IO to read and write data on Cassandra.
 *
 * <h3>Reading from Cassandra Cluster</h3>
 *
 *<p>CassandraIO source returns a bounded collection of {@code T} as a
 * {@code PCollection<T>}. T is the type returned by the provided
 * {@link RowMapper}.
 * <pre>{@code
 * pipeline.apply(CassandraIO.<KV<Integer, String>>read()
 *   .withClusterConfiguration(CassandraIO.ClusterConfiguration
 *   			.create(cassandraHosts, cassandraPort))
 *   .withQuery("select id,name from testbeam.person")
 *   .withRowMapper(new CassandraIO.RowMapper<KV<Integer, String>>() {		
 *     public List<KV<String, String>> mapRow(Row row) throws Exception {
 *       List<KV<String, String>> rsult = new ArrayList<KV<String, String>>();
 *       KV<String, String> kv = KV.of("name", row.getString("name"));
 *       rsult.add(kv);
 *       return rsult;
 *     }
 *   })
 *   .withCoder(ListCoder.of(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))));
 * }</pre>
 *
 *
 *<p>Query parameters can be configured using a user-provided {@link StatementPreparator}.
 * For example:</p>
 *
 * <pre>{@code
 * pipeline.apply(CassandraIO.<KV<Integer, String>>read()
 *   .withClusterConfiguration(CassandraIO.ClusterConfiguration.create(cluster)
 *   .withQuery("select id,name from testbeam.person where name = ?")
 *   .withStatementPreparator(new CassandraIO.StatementPreparator() {
 *     public void setParameters(BoundStatementLast boundStatementLast) throws Exception {
 *       boundStatementLast.bind("Darwin");
 *     }
 *   })
 *   .withRowMapper(new CassandraIO.RowMapper<KV<Integer, String>>() {
 *     public KV<Integer, String> mapRow(ResultSet resultSet) throws Exception {
 *       return KV.of(resultSet.getInt(1), resultSet.getString(2));
 *     }
 *   })
 * }</pre>
 *
 * <h3>Writing to Cassandra cluster</h3>
 *
 * <p>Cassandra sink supports writing records into a database.
 * It writes a {@link PCollection} to the
 * database by converting each T into a {@link BoundStatement}
 * via a user-provided {@link
 * BoundStatementSetter}.
 *
 * <p>Like the source, to configure the sink, you have to provide
 * a {@link ClusterConfiguration}.
 * <pre>{@code
 * pipeline
 *   .apply(...)
 *   .apply(CassandraIO.<KV<Integer, String>>write()
 *      .withClusterConfiguration(CassandraIO.ClusterConfiguration
 *                  .create(cassandraHosts, cassandraPort))
 *      .withStatement("insert into testbeam.person values(?, ?)")
 *      ..withBoundStatementSetter(
 *              new CassandraIO.BoundStatementSetter< List<KV<Integer, String>>>() {
 *                  @Override
 *                  public void setParameters(List<KV<Integer, String>> element,
 *                          BoundStatement boundStatement)
 *                          throws Exception {
 *                      List<String> d = new ArrayList<String>();
 *                      for (KV<Integer, String> kv : element){
 *                           d.add(kv.getValue());
 *                      }
 *                      boundStatement.bind(d.toArray());
 *                  }
 *              }));
 * }</pre>
 *
 * <p>NB: in case of transient failures, Beam runners may execute parts of
 * CassandraIO.Write multiple times for fault tolerance. Because of that, you
 * should avoid using {@code INSERT} statements, since that risks duplicating
 * records in the cluster databases, or failing due to primary key conflicts. 
 */

public class CassandraIO {

    private static final Logger LOG = LoggerFactory
             .getLogger(CassandraIO.class);

    /**
     * Read data from a Cassandra Cluster.
     *
     * @param <T>
     *            Type of the data to be read.
     */
    public static <T> Read<T> read() {
        return new AutoValue_CassandraIO_Read.Builder<T>().build();
    }

    /**
     * Write data to a Cassandra Cluster.
     *
     * @param <T>
     *            Type of the data to be written.
     */
    public static <T> Write<T> write() {
        return new AutoValue_CassandraIO_Write.Builder<T>().build();
    }

    private CassandraIO() {
    }

    /**
     * An interface used by {@link CassandraIO.Read} for converting each row of
     * the {@link row} into an element of the resulting
     * {@link PCollection}.
     */
    public interface RowMapper<T> extends Serializable {
        T mapRow(Row row) throws Exception;
    }

    /**
     * A POJO describing a {@link Cluster}, either providing directly a
     * {@link Cluster} or all properties allowing to create a
     * {@link Cluster}.
     */
    @AutoValue
    public abstract static class ClusterConfiguration implements Serializable {
        @Nullable
        abstract String getCassandraHosts();
        abstract Integer getCassandraPort();
        @Nullable abstract String getCassandraUsername();
        @Nullable abstract String getCassandraPasswd();

        @Nullable
        abstract String getKeyspace();

        abstract Builder builder();

        @AutoValue.Builder
        abstract static class Builder {
            abstract Builder setKeyspace(String keyspace);

            abstract Builder setCassandraHosts(String cassandraHosts);

            abstract Builder setCassandraPort(Integer cassandraPort);

            abstract Builder setCassandraUsername(String cassandraUsername);

            abstract Builder setCassandraPasswd(String cassandraPasswd);

            abstract ClusterConfiguration build();
        }

        public static ClusterConfiguration create(String cassandraHosts, Integer cassandraPort) {
            checkArgument(cassandraHosts != null,
                    "ClusterConfiguration.create(cluster) called with "
                            + "null data cluster");
            checkArgument(cassandraHosts instanceof Serializable,
                    "ClusterConfiguration.create(cassandraHosts) called with "
                    + "a cassandraHosts not Serializable");
            if (cassandraPort == null){
                cassandraPort = 9042;
            }
            return new AutoValue_CassandraIO_ClusterConfiguration.Builder()
                    .setCassandraHosts(cassandraHosts)
                    .setCassandraPort(cassandraPort).build();
        }
        public static ClusterConfiguration create(String cassandraHosts, int cassandraPort,
                String cassandraUsername, String cassandraPasswd) {
            checkArgument(cassandraHosts != null,
                    "ClusterConfiguration.create(cassandraHosts,cassandraPort,"
                    + "cassandraUsername,cassandraPasswd) called with "
                            + "null data cluster configuration");
            checkArgument(cassandraUsername != null,
                    "ClusterConfiguration.create(cassandraHosts,cassandraPort,"
                    + "cassandraUsername,cassandraPasswd) called with "
                            + "null data cluster configuration");
            checkArgument(cassandraPasswd != null,
                    "ClusterConfiguration.create(cassandraHosts,cassandraPort,"
                    + "cassandraUsername,cassandraPasswd) called with "
                            + "null data cluster configuration");

            return new AutoValue_CassandraIO_ClusterConfiguration.Builder()
                    .setCassandraHosts(cassandraHosts)
                    .setCassandraPort(cassandraPort)
                    .setCassandraUsername(cassandraUsername)
                    .setCassandraPasswd(cassandraPasswd).build();
        }

        public static ClusterConfiguration create(String cassandraHosts,
                int cassandraPort, String cassandraUsername, String cassandraPasswd,
                String keyspace) {
            checkArgument(cassandraHosts != null,
                    "ClusterConfiguration.create(cassandraHosts,cassandraPort,"
                    + "cassandraUsername,cassandraPasswd,keyspace) called with "
                            + "null data cluster configuration");
            checkArgument(cassandraUsername != null,
                    "ClusterConfiguration.create(cassandraHosts,cassandraPort,"
                    + "cassandraUsername,cassandraPasswd,keyspace) called with "
                            + "null data cluster configuration");
            checkArgument(cassandraPasswd != null,
                    "ClusterConfiguration.create(cassandraHosts,cassandraPort,"
                    + "cassandraUsername,cassandraPasswd,keyspace) called with "
                            + "null data cluster configuration");
            checkArgument(keyspace != null,
                    "ClusterConfiguration.create(cassandraHosts,cassandraPort,"
                    + "cassandraUsername,cassandraPasswd,keyspace) called with "
                            + "null data cluster configuration");
            return new AutoValue_CassandraIO_ClusterConfiguration.Builder()
                    .setCassandraHosts(cassandraHosts)
                    .setCassandraPort(cassandraPort)
                    .setCassandraUsername(cassandraUsername)
                    .setCassandraPasswd(cassandraPasswd)
                    .setKeyspace(keyspace).build();
        }

        private void populateDisplayData(DisplayData.Builder builder) {
            if (getCassandraHosts() != null) {
                builder.addIfNotNull(DisplayData.item("cassandraHosts",
                        getCassandraHosts().getClass().getName()));
            }
        }

        Session getSession() throws Exception {
                int maxIdle = 5;
                int maxTotal = 100;
                int minIdle = 3;
                int maxWaitMillis = 60 * 100;

                int localCoreConnectionsPerHost = 8;
                int localMaxConnectionsPerHost = 8;
                int localMaxRequestsPerConnection = 100;

                int poolTimeoutMillis = 60 * 1000;
                int connectTimeoutMillis = 60 * 1000;
                int readTimeoutMillis = 60 * 1000;

                GenericObjectPoolConfig conf = new GenericObjectPoolConfig();
                conf.setMaxIdle(maxIdle);
                conf.setMaxTotal(maxTotal);
                conf.setMinIdle(minIdle);
                conf.setMaxWaitMillis(maxWaitMillis);
                conf.setTestOnBorrow(false);
                conf.setTestWhileIdle(false);
                conf.setTestOnReturn(false);
                PoolingOptions poolingOptions = new PoolingOptions();
                poolingOptions.setCoreConnectionsPerHost
                    (HostDistance.LOCAL, localCoreConnectionsPerHost);
                poolingOptions.setMaxConnectionsPerHost(HostDistance.LOCAL,
                        localMaxConnectionsPerHost);
                poolingOptions.setMaxRequestsPerConnection(HostDistance.LOCAL,
                        localMaxRequestsPerConnection);
                poolingOptions.setPoolTimeoutMillis(poolTimeoutMillis);
                SocketOptions socketOptions = new SocketOptions();
                socketOptions.setKeepAlive(true);
                socketOptions.setReceiveBufferSize(100 * 1024 * 1024);
                socketOptions.setTcpNoDelay(true);
                socketOptions.setReadTimeoutMillis(readTimeoutMillis);
                socketOptions.setConnectTimeoutMillis
                                (connectTimeoutMillis);
            String[] nodes = getCassandraHosts().split(",");
            if (getCassandraUsername() == null
                    || getCassandraUsername().length() == 0) {
                Cluster cluster = Cluster.builder()
                        .addContactPoints(nodes)
                        .withPort(getCassandraPort())
                        .withLoadBalancingPolicy(LatencyAwarePolicy
                                .builder(new RoundRobinPolicy()).build())
                        .withQueryOptions(new QueryOptions()
                                .setConsistencyLevel(ConsistencyLevel.ONE))
                        .withReconnectionPolicy(new ConstantReconnectionPolicy(1000))
                        .withProtocolVersion(ProtocolVersion.V3)
                        .withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE)
                        .withReconnectionPolicy(new ConstantReconnectionPolicy(100L))
                        .withPoolingOptions(poolingOptions)
                        .withSocketOptions(socketOptions)
                        .build();
                if (getKeyspace() != null) {
                    return cluster.connect(getKeyspace());
                } else {
                    return cluster.connect();
                }
            } else {
                Cluster cluster = Cluster.builder()
                        .addContactPoints(nodes)
                        .withPort(getCassandraPort())
                        .withCredentials(getCassandraUsername(),
                                getCassandraPasswd())
                        .withLoadBalancingPolicy(LatencyAwarePolicy
                                .builder(new RoundRobinPolicy()).build())
                        .withQueryOptions(new QueryOptions()
                                .setConsistencyLevel(ConsistencyLevel.ONE))
                        .withReconnectionPolicy(new ConstantReconnectionPolicy(1000))
                        .withProtocolVersion(ProtocolVersion.V3)
                        .withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE)
                        .withReconnectionPolicy(new ConstantReconnectionPolicy(100L))
                        .withPoolingOptions(poolingOptions)
                        .withSocketOptions(socketOptions)
                        .build();
                if (getKeyspace() != null) {
                    return cluster.connect(getKeyspace());
                } else {
                    return cluster.connect();
                }
            }
        }
    }

    /**
     * An interface used by the CassandraIO Write to set the parameters of the
     * {@link BoundStatement} used to setParameters into the cluster database.
     */
    public interface StatementPreparator extends Serializable {
        void setParameters(BoundStatement boundStatement)
                throws Exception;
    }

    /** A {@link PTransform} to read data from a Cassandra cluster. */
    @AutoValue
    public abstract static class Read<T>
            extends PTransform<PBegin, PCollection<T>> {
        @Nullable
        abstract ClusterConfiguration getClusterConfiguration();

        @Nullable
        abstract String getQuery();

        @Nullable
        abstract StatementPreparator getStatementPreparator();

        @Nullable
        abstract RowMapper<T> getRowMapper();

        @Nullable
        abstract Coder<T> getCoder();

        abstract Builder<T> toBuilder();

        @AutoValue.Builder
        abstract static class Builder<T> {
            abstract Builder<T> setClusterConfiguration(
                    ClusterConfiguration clusterConfiguration);

            abstract Builder<T> setQuery(String query);

            abstract Builder<T> setStatementPreparator(
                    StatementPreparator statementPreparator);

            abstract Builder<T> setRowMapper(RowMapper<T> rowMapper);

            abstract Builder<T> setCoder(Coder<T> coder);

            abstract Read<T> build();
        }

        public Read<T> withClusterConfiguration(ClusterConfiguration clusterConfiguration) {
            checkArgument(clusterConfiguration != null,
                    "CassandraIO.read().withClusterConfiguration"
                            + "(clusterConfiguration) called with null cluster");
            return toBuilder().setClusterConfiguration(clusterConfiguration).build();
        }

        public Read<T> withQuery(String query) {
            checkArgument(query != null,
                    "CassandraIO.read().withQuery(query) called with null query");
            return toBuilder().setQuery(query).build();
        }

        public Read<T> withStatementPrepator(
                StatementPreparator statementPreparator) {
            checkArgument(statementPreparator != null,
                    "CassandraIO.read().withStatementPreparator(statementPreparator) called "
                            + "with null statementPreparator");
            return toBuilder().setStatementPreparator(statementPreparator)
                    .build();
        }

        public Read<T> withRowMapper(RowMapper<T> rowMapper) {
            checkArgument(rowMapper != null,
                    "CassandraIO.read().withRowMapper(rowMapper) "
                    + "called with null rowMapper");
            return toBuilder().setRowMapper(rowMapper).build();
        }

        public Read<T> withCoder(Coder<T> coder) {
            checkArgument(coder != null,
                    "CassandraIO.read().withCoder(coder) "
                    + "called with null coder");
            return toBuilder().setCoder(coder).build();
        }

        @Override
        public PCollection<T> expand(PBegin input) {
            return input.apply(Create.of(getQuery()))
                    .apply(ParDo.of(new ReadFn<>(this))).setCoder(getCoder())
                    .apply(ParDo.of(new DoFn<T, KV<Integer, T>>() {
                        private Random random;

                        @Setup
                        public void setup() {
                            random = new Random();
                        }

                        @ProcessElement
                        public void processElement(ProcessContext context) {
                            context.output(
                                    KV.of(random.nextInt(), context.element()));
                        }
                    })).apply(GroupByKey.<Integer, T> create())
                    .apply(Values.<Iterable<T>> create())
                    .apply(Flatten.<T> iterables());
        }

        @Override
        public void validate(PBegin input) {
            checkState(getQuery() != null,
                    "CassandraIO.read() requires a query to be set via withQuery(query)");
            checkState(getRowMapper() != null,
                    "CassandraIO.read() requires a rowMapper to be "
                    + "set via withRowMapper(rowMapper)");
            checkState(getCoder() != null,
                    "CassandraIO.read() requires a coder to be set via withCoder(coder)");
            checkState(getClusterConfiguration() != null,
                    "CassandraIO.read() requires a Cluster configuration to be set via "
                            + "withClusterConfiguration(clusterConfiguration)");
        }

        @Override
        public void populateDisplayData(DisplayData.Builder builder) {
            super.populateDisplayData(builder);
            builder.add(DisplayData.item("query", getQuery()));
            builder.add(DisplayData.item("rowMapper",
                    getRowMapper().getClass().getName()));
            builder.add(
                    DisplayData.item("coder", getCoder().getClass().getName()));
            getClusterConfiguration().populateDisplayData(builder);
        }

        /** A {@link DoFn} executing the SQL query to read from the cluster. */
        static class ReadFn<T> extends DoFn<String, T> {
            private CassandraIO.Read<T> spec;
            private Session session;

            private ReadFn(Read<T> spec) {
                this.spec = spec;
            }

            @Setup
            public void setup() throws Exception {
                session = spec.getClusterConfiguration().getSession();
            }

            @ProcessElement
            public void processElement(ProcessContext context)
                    throws Exception {
                String query = context.element();
                PreparedStatement statement = session.prepare(query);
                BoundStatement boundStatementLast = new BoundStatement(statement);
                if (this.spec.getStatementPreparator() != null){
                    this.spec.getStatementPreparator().setParameters(boundStatementLast);
                }
                ResultSet resultSet = session.execute(boundStatementLast);
                for (Row row : resultSet) {
                    context.output(spec.getRowMapper().mapRow(row));
                }
            }

            @Teardown
            public void teardown() throws Exception {
                if (session != null) {
                    session.close();
                }
            }
        }
    }

    /**
     * An interface used by the CassandraIO Write to set the parameters of the
     * {@link BoundStatement} used to setParameters into the cluster.
     */
    public interface BoundStatementSetter<T> extends Serializable {
        void setParameters(T element, BoundStatement boundStatement)
                throws Exception;
    }

    /** A {@link PTransform} to write to a Cassandra cluster. */
    @AutoValue
    public abstract static class Write<T>
            extends PTransform<PCollection<T>, PDone> {
        @Nullable
        abstract ClusterConfiguration getClusterConfiguration();

        @Nullable
        abstract String getStatement();

        @Nullable
        abstract BoundStatementSetter<T> getBoundStatementSetter();

        abstract Builder<T> toBuilder();

        @AutoValue.Builder
        abstract static class Builder<T> {
            abstract Builder<T> setClusterConfiguration(
                    ClusterConfiguration clusterConfiguration);

            abstract Builder<T> setStatement(String statement);

            abstract Builder<T> setBoundStatementSetter(
                    BoundStatementSetter<T> setter);

            abstract Write<T> build();
        }

        public Write<T> withClusterConfiguration(ClusterConfiguration clusterConfiguration) {
            return toBuilder().setClusterConfiguration(clusterConfiguration).build();
        }

        public Write<T> withStatement(String statement) {
            return toBuilder().setStatement(statement).build();
        }

        public Write<T> withBoundStatementSetter(
                BoundStatementSetter<T> setter) {
            return toBuilder().setBoundStatementSetter(setter).build();
        }

        @Override
        public PDone expand(PCollection<T> input) {
            input.apply(ParDo.of(new WriteFn<T>(this)));
            return PDone.in(input.getPipeline());
        }

        @Override
        public void validate(PCollection<T> input) {
            checkArgument(getClusterConfiguration() != null,
                    "CassandraIO.write() requires a configuration to be "
                            + "set via "
                            + ".withClusterConfiguration(configuration)");
            checkArgument(getStatement() != null, "CassandraIO.write() requires"
                    + " a statement to be set via .withStatement(statement)");
            checkArgument(getBoundStatementSetter() != null,
                    "CassandraIO.write() requires a BoundStatementSetter"
                            + " to be set via.withBoundStatementSetter("
                            + "BoundStatementSetter)");
        }

        private static class WriteFn<T> extends DoFn<T, Void> {

            private final Write<T> spec;

            private Session session;
            private BoundStatement boundStatement;

            public WriteFn(Write<T> spec) {
                this.spec = spec;
            }

            @Setup
            public void setup() throws Exception {
                session = spec.getClusterConfiguration().getSession();
                PreparedStatement st = session.prepare(spec.getStatement());
                boundStatement = new BoundStatement(st);
            }

            @StartBundle
            public void startBundle(Context context) {

            }

            @ProcessElement
            public void processElement(ProcessContext context)
                    throws Exception {
                T record = context.element();
                spec.getBoundStatementSetter().setParameters(record,
                        boundStatement);
                finishBundle(context);
            }

            @FinishBundle
            public void finishBundle(Context context) throws Exception {
                session.execute(boundStatement);
            }

            @Teardown
            public void teardown() throws Exception {
                if (session != null) {
                    session.close();
                }
            }
        }
    }
}
