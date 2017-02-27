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
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.Coder;
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
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;




/**
 * Test CassandraIO.
 */
@RunWith(JUnit4.class)
public class CassandraIOTest implements Serializable{

    @Rule
    public final transient TestPipeline p = TestPipeline.create();

    private CassandraIO.ClusterConfiguration clusterConfiguration;

    private static final String TESTKEYSPACE = "testbeam";

    @SuppressWarnings("unused")
    private static Integer cassandraPort = 9042;
    @SuppressWarnings("unused")
    private static String cassandraHosts = "Cassandra-ip1";
    @SuppressWarnings("unused")
    private static String cassandraUsername = "admin";
    @SuppressWarnings("unused")
    private static String cassandraPasswd = "";

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    @BeforeClass
    public static void beforeClass() throws Exception {
        cassandraPort = 9042;
        cassandraHosts = "Cassandra-ip1";
        cassandraUsername = "admin";
        cassandraPasswd = "";
    }

    @AfterClass
    public static void afterClass() throws Exception {
    }

    @Before
    public void setup() throws Exception {
        clusterConfiguration = CassandraIO.ClusterConfiguration
            .create(cassandraHosts, cassandraPort);
        Session session = clusterConfiguration.getSession();
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
        String query = "select * from " + TESTKEYSPACE + ".person";
        CassandraIO.Read read = CassandraIO.read()
                .withClusterConfiguration(clusterConfiguration)
                .withQuery(query);
        assertEquals(query, read.getQuery());
       // assertNotNull("configuration", read.getClusterConfiguration());
    }

    @Test
    @Category(NeedsRunner.class)
    public void testRead() throws Exception {
        String query = "select * from " + TESTKEYSPACE + ".person";
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
                Count.<List<KV<String, String>>> globally())).isEqualTo(1L);
        pipeline.run();
    }
    @Test
    public void testWriteBuildsCorrectly() {
        String statement = "INSERT INTO " + TESTKEYSPACE + ".atable (a1,a2,a3) VALUES (?,?,?) ";
        CassandraIO.Write write = CassandraIO.<List>write()
                .withClusterConfiguration(clusterConfiguration)
                .withStatement(statement);
        assertNotNull(statement, write.getStatement());
        assertNotNull("configuration", write.getClusterConfiguration());
    }

    @Test
    @Category(NeedsRunner.class)
    public void testWrite() {
        String statement = "INSERT INTO " + TESTKEYSPACE + ".atable (a1,a2,a3) VALUES (?,?,?) ";

        List<List<KV<Integer, String>>> data = new ArrayList<List<KV<Integer, String>>>();
        int countAll = 20000;
        for (int i = 0; i < countAll; i++) {
            List<KV<Integer, String>> kv = new ArrayList<KV<Integer, String>>();
            kv.add(KV.of(i, "a" + i));
            kv.add(KV.of(i, "a" + i));
            kv.add(KV.of(i, "a" + i));
            data.add(kv);
        }
        Coder coder =  ListCoder.of(KvCoder.of(BigEndianIntegerCoder.of(), StringUtf8Coder.of()));
        pipeline.apply(Create.of(data).withCoder(coder))
                .apply(CassandraIO.<List<KV<Integer, String>>> write()
                .withClusterConfiguration(clusterConfiguration)
                .withStatement(statement)
                .withBoundStatementSetter(
                new CassandraIO.BoundStatementSetter< List<KV<Integer, String>>>() {
                    @Override
                    public void setParameters(List<KV<Integer, String>> element,
                            BoundStatement boundStatement)
                            throws Exception {
                        List<String> d = new ArrayList<String>();
                        for (KV<Integer, String> kv : element){
                            d.add(kv.getValue());
                        }
                        boundStatement.bind(d.toArray());
                    }
                }));
        pipeline.run();
        try (Session session = clusterConfiguration.getSession()) {
            com.datastax.driver.core.ResultSet resultSet = session
                    .execute("select count(*) from " + TESTKEYSPACE + ".atable");
            List<Row> resultAll = resultSet.all();
            Assert.assertNotNull(resultAll);
            Assert.assertEquals(1, resultAll.size());
            long count = resultAll.get(0).get(0, Long.class);
            Assert.assertEquals(countAll, count);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertNull(e);
        }
    }

}
