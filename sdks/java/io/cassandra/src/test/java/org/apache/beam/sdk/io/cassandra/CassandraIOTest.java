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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;
import com.datastax.driver.core.policies.LatencyAwarePolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;


/**
 * Test CassandraIO.
 */
@RunWith(JUnit4.class)
public class CassandraIOTest {

    @Rule
    public final transient TestPipeline p = TestPipeline.create();
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private static Cluster cluster;
    private CassandraIO.ClusterConfiguration clusterConfiguration;

    private static final String TESTKEYSPACE = "testbeam";

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    @BeforeClass
    public static void beforeClass() throws Exception {

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
        int cassandraPort = 9042;
        String cassandraHosts = "Cassandra-ip1";
        String cassandraUser = "admin";
        String cassandraPassword = "";
        String[] nodes = cassandraHosts.split(",");
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
        cluster = Cluster.builder().addContactPoints(nodes)
                .withPort(cassandraPort)
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
                .withCredentials(cassandraUser, cassandraPassword)
                .build();

    }

    @AfterClass
    public static void afterClass() throws Exception {
        if (cluster != null) {
            cluster.close();
        }
    }

    @Before
    public void setup() throws Exception {
        clusterConfiguration = CassandraIO.ClusterConfiguration.create(cluster);
        Session session = cluster.connect();
        session.execute("CREATE KEYSPACE IF NOT EXISTS " + TESTKEYSPACE
                + " WITH replication = {'class':'SimpleStrategy',"
                + "'replication_factor': 1};");
        session.execute("CREATE TABLE IF NOT EXISTS " + TESTKEYSPACE
                + ".person(name text, id INT, c INT, PRIMARY KEY(id));");
        session.execute("CREATE TABLE IF NOT EXISTS " + TESTKEYSPACE
                + ".atable(a1 text, a2 text,a3 text, PRIMARY KEY(a1));");
        RegularStatement insert = QueryBuilder.insertInto(TESTKEYSPACE, "person")
                .values(new String[] {"id", "name"}, new Object[] {1, "test"});
        session.execute(insert);
    }

    @Test
    public void testClusterConfiguration()
            throws Exception {

        try (Session conn = clusterConfiguration.getSession()) {
            assertFalse(conn.isClosed());
        }
    }

    @Test
    public void testReadBuildsCorrectly() {
        String query = "select * from test.person";
        CassandraIO.Read read = CassandraIO.read()
                .withClusterConfiguration(clusterConfiguration)
                .withQuery(query);
        assertEquals(query, read.getQuery());
       // assertNotNull("configuration", read.getClusterConfiguration());
    }

    @Test
    @Category(NeedsRunner.class)
    public void testRead() throws Exception {
        String query = "select * from test.person";
        PCollection<List<KV<String, String>>> output = pipeline
                .apply(CassandraIO.<List<KV<String, String>>> read()
                .withClusterConfiguration(clusterConfiguration)
                .withQuery(query)
                .withRowMapper(
new CassandraIO.RowMapper<List<KV<String, String>>>() {
private static final long serialVersionUID = 4304414480924473864L;

                            @Override
                            public List<KV<String, String>> mapRow(
com.datastax.driver.core.Row row)
                                    throws Exception {
List<KV<String, String>> rsult = new ArrayList<KV<String, String>>();
KV<String, String> kv = KV.of("name", row.getString("name"));
                                rsult.add(kv);
                                return rsult;
                            }
                        })
                .withCoder(ListCoder.of(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))));

        PAssert.thatSingleton(output.apply("Count All",
                Count.<List<KV<String, String>>> globally())).isEqualTo(1000L);
        pipeline.run();
    }
    @Test
    public void testWriteBuildsCorrectly() {
        String statement = "INSERT INTO test.atable (a1,a2,a3) VALUES (?,?,?) ";
        CassandraIO.Write write = CassandraIO.<List>write()
                .withClusterConfiguration(clusterConfiguration)
                .withStatement(statement);
        assertNotNull(statement, write.getStatement());
        assertNotNull("configuration", write.getClusterConfiguration());
    }

    @Test
    @Category(NeedsRunner.class)
    public void testWrite() {
        String statement = "INSERT INTO test.atable (a1,a2,a3) VALUES (?,?,?) ";

        List<List> data = new ArrayList<List>();
        for (int i = 0; i < 1000; i++) {
            List<String> kv = new ArrayList<String>();
            kv.add("a" + i);
            kv.add("b" + i);
            kv.add("c" + i);

            data.add(kv);
        }
        pipeline.apply(Create.of(data))
                .apply(CassandraIO.<List> write()
                .withClusterConfiguration(clusterConfiguration)
                .withStatement(statement)
                .withBoundStatementSetter(
                new CassandraIO.BoundStatementSetter<List>() {
                    @Override
                    public void setParameters(List element,
                            BoundStatement boundStatement)
                            throws Exception {
                        boundStatement.
                        bind(element.toArray());
                    }
                }));
        pipeline.run();
        try (Session session = cluster.connect()) {
            com.datastax.driver.core.ResultSet resultSet = session
                    .execute("select count(*) count from test.atable");
            List<Row> resultAll = resultSet.all();
            Assert.assertNotNull(resultAll);
            Assert.assertEquals(1, resultAll.size());
            int count = resultAll.get(0).getInt("count");
            Assert.assertEquals(1000, count);
        }
    }

}
