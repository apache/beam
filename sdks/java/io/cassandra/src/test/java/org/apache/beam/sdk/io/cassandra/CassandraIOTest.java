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

import static junit.framework.TestCase.assertTrue;
import static org.apache.beam.sdk.io.cassandra.CassandraIO.CassandraSource.distance;
import static org.apache.beam.sdk.io.cassandra.CassandraIO.CassandraSource.getEstimatedSizeBytesFromTokenRanges;
import static org.apache.beam.sdk.io.cassandra.CassandraIO.CassandraSource.getRingFraction;
import static org.apache.beam.sdk.io.cassandra.CassandraIO.CassandraSource.isMurmur3Partitioner;
import static org.apache.beam.sdk.testing.SourceTestUtils.readFromSource;
import static org.junit.Assert.assertEquals;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import java.io.Serializable;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.cassandra.CassandraIO.CassandraSource.TokenRange;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.SourceTestUtils;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v20_0.com.google.common.base.Objects;
import org.apache.cassandra.service.StorageServiceMBean;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Tests of {@link CassandraIO}. */
@RunWith(JUnit4.class)
public class CassandraIOTest implements Serializable {
  private static final long NUM_ROWS = 20L;
  private static final String CASSANDRA_KEYSPACE = "beam_ks";
  private static final String CASSANDRA_HOST = "127.0.0.1";
  private static final int CASSANDRA_PORT = 9142;
  private static final String CASSANDRA_USERNAME = "cassandra";
  private static final String CASSANDRA_ENCRYPTED_PASSWORD =
      "Y2Fzc2FuZHJh"; // Base64 encoded version of "cassandra"
  private static final String CASSANDRA_TABLE = "scientist";
  private static final Logger LOGGER = LoggerFactory.getLogger(CassandraIOTest.class);
  private static final String STORAGE_SERVICE_MBEAN = "org.apache.cassandra.db:type=StorageService";
  private static final String JMX_PORT = "7199";
  private static final long SIZE_ESTIMATES_UPDATE_INTERVAL = 5000L;
  private static final long STARTUP_TIMEOUT = 45000L;

  private static Cluster cluster;
  private static Session session;
  private static long startupTime;

  @Rule public transient TestPipeline pipeline = TestPipeline.create();

  @BeforeClass
  public static void startCassandra() throws Exception {
    System.setProperty("cassandra.jmx.local.port", JMX_PORT);
    startupTime = System.currentTimeMillis();
    EmbeddedCassandraServerHelper.startEmbeddedCassandra(STARTUP_TIMEOUT);
    cluster = EmbeddedCassandraServerHelper.getCluster();
    session = EmbeddedCassandraServerHelper.getSession();

    LOGGER.info("Creating the Cassandra keyspace");
    session.execute(
        "CREATE KEYSPACE IF NOT EXISTS "
            + CASSANDRA_KEYSPACE
            + " WITH REPLICATION = "
            + "{'class':'SimpleStrategy', 'replication_factor':3};");
    LOGGER.info(CASSANDRA_KEYSPACE + " keyspace created");

    LOGGER.info("Use the Cassandra keyspace");
    session.execute("USE " + CASSANDRA_KEYSPACE);

    LOGGER.info("Create Cassandra table");
    session.execute(
        String.format(
            "CREATE TABLE IF NOT EXISTS %s(person_id int, person_name text, PRIMARY KEY"
                + "(person_id));",
            CASSANDRA_TABLE));
  }

  @AfterClass
  public static void stopCassandra() {
    if (cluster != null && session != null) {
      EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
      session.close();
      cluster.close();
    } else {
      if (cluster != null) {
        cluster.close();
      }
    }
  }

  @Before
  public void purgeCassandra() {
    session.execute(String.format("TRUNCATE TABLE %s.%s", CASSANDRA_KEYSPACE, CASSANDRA_TABLE));
  }

  private static void insertRecords() throws Exception {
    LOGGER.info("Insert records");
    String[] scientists = {
      "Einstein",
      "Darwin",
      "Copernicus",
      "Pasteur",
      "Curie",
      "Faraday",
      "Newton",
      "Bohr",
      "Galilei",
      "Maxwell"
    };
    for (int i = 0; i < NUM_ROWS; i++) {
      int index = i % scientists.length;
      session.execute(
          String.format(
              "INSERT INTO %s.%s(person_id, person_name) values("
                  + i
                  + ", '"
                  + scientists[index]
                  + "');",
              CASSANDRA_KEYSPACE,
              CASSANDRA_TABLE));
    }
    flushMemTables();
  }

  /**
   * Force the flush of cassandra memTables to SSTables to update size_estimates.
   * https://wiki.apache.org/cassandra/MemtableSSTable This is what cassandra spark connector does
   * through nodetool binary call. See:
   * https://github.com/datastax/spark-cassandra-connector/blob/master/spark-cassandra-connector
   * /src/it/scala/com/datastax/spark/connector/rdd/partitioner/DataSizeEstimatesSpec.scala which
   * uses the same JMX service as bellow. See:
   * https://github.com/apache/cassandra/blob/cassandra-3.X
   * /src/java/org/apache/cassandra/tools/nodetool/Flush.java
   */
  private static void flushMemTables() throws Exception {
    JMXServiceURL url =
        new JMXServiceURL(
            String.format("service:jmx:rmi:///jndi/rmi://%s:%s/jmxrmi", CASSANDRA_HOST, JMX_PORT));
    JMXConnector jmxConnector = JMXConnectorFactory.connect(url, null);
    MBeanServerConnection mBeanServerConnection = jmxConnector.getMBeanServerConnection();
    ObjectName objectName = new ObjectName(STORAGE_SERVICE_MBEAN);
    StorageServiceMBean mBeanProxy =
        JMX.newMBeanProxy(mBeanServerConnection, objectName, StorageServiceMBean.class);
    mBeanProxy.forceKeyspaceFlush(CASSANDRA_KEYSPACE, CASSANDRA_TABLE);
    jmxConnector.close();
    // same method of waiting than cassandra spark connector
    long initialDelay = Math.max(startupTime + STARTUP_TIMEOUT - System.currentTimeMillis(), 0L);
    Thread.sleep(initialDelay + 2 * SIZE_ESTIMATES_UPDATE_INTERVAL);
  }

  @Test
  public void testEstimatedSizeBytes() throws Exception {
    insertRecords();
    PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
    CassandraIO.Read<Scientist> read =
        CassandraIO.<Scientist>read()
            .withHosts(Arrays.asList(CASSANDRA_HOST))
            .withPort(CASSANDRA_PORT)
            .withKeyspace(CASSANDRA_KEYSPACE)
            .withTable(CASSANDRA_TABLE);
    CassandraIO.CassandraSource<Scientist> source = new CassandraIO.CassandraSource<>(read, null);
    long estimatedSizeBytes = source.getEstimatedSizeBytes(pipelineOptions);
    // the size is non determanistic in Cassandra backend
    assertTrue((estimatedSizeBytes >= 4608L * 0.9f) && (estimatedSizeBytes <= 4608L * 1.1f));
  }

  @Test
  public void testRead() throws Exception {
    insertRecords();

    PCollection<Scientist> output =
        pipeline.apply(
            CassandraIO.<Scientist>read()
                .withHosts(Arrays.asList(CASSANDRA_HOST))
                .withPort(CASSANDRA_PORT)
                .withKeyspace(CASSANDRA_KEYSPACE)
                .withTable(CASSANDRA_TABLE)
                .withCoder(SerializableCoder.of(Scientist.class))
                .withEntity(Scientist.class));

    PAssert.thatSingleton(output.apply("Count", Count.globally())).isEqualTo(NUM_ROWS);

    PCollection<KV<String, Integer>> mapped =
        output.apply(
            MapElements.via(
                new SimpleFunction<Scientist, KV<String, Integer>>() {
                  @Override
                  public KV<String, Integer> apply(Scientist scientist) {
                    return KV.of(scientist.name, scientist.id);
                  }
                }));
    PAssert.that(mapped.apply("Count occurrences per scientist", Count.perKey()))
        .satisfies(
            input -> {
              for (KV<String, Long> element : input) {
                assertEquals(element.getKey(), NUM_ROWS / 10, element.getValue().longValue());
              }
              return null;
            });

    pipeline.run();
  }

  @Test
  public void testReadWithWhere() throws Exception {
    insertRecords();

    PCollection<Scientist> output =
        pipeline.apply(
            CassandraIO.<Scientist>read()
                .withHosts(Arrays.asList(CASSANDRA_HOST))
                .withPort(CASSANDRA_PORT)
                .withKeyspace(CASSANDRA_KEYSPACE)
                .withTable(CASSANDRA_TABLE)
                .withCoder(SerializableCoder.of(Scientist.class))
                .withEntity(Scientist.class)
                .withWhere("person_id=10"));

    PAssert.thatSingleton(output.apply("Count", Count.globally())).isEqualTo(1L);

    pipeline.run();
  }

  @Test
  public void testWrite() {
    ArrayList<Scientist> scientists = buildScientists(NUM_ROWS);

    pipeline
        .apply(Create.of(scientists))
        .apply(
            CassandraIO.<Scientist>write()
                .withHosts(Arrays.asList(CASSANDRA_HOST))
                .withPort(CASSANDRA_PORT)
                .withKeyspace(CASSANDRA_KEYSPACE)
                .withEntity(Scientist.class));
    // table to write to is specified in the entity in @Table annotation (in that cas person)
    pipeline.run();

    List<Row> results = getRows();
    assertEquals(NUM_ROWS, results.size());
    for (Row row : results) {
      assertTrue(row.getString("person_name").matches("Name (\\d*)"));
    }
  }

  @Test
  public void testReadWithPasswordDecryption() throws Exception {
    insertRecords();

    String sessionReadUID = "session-read-" + UUID.randomUUID();
    PasswordDecrypter readPwdDecrypter = new TestPasswordDecrypter(sessionReadUID);

    PCollection<Scientist> output =
        pipeline.apply(
            CassandraIO.<Scientist>read()
                .withHosts(Arrays.asList(CASSANDRA_HOST))
                .withPort(CASSANDRA_PORT)
                .withUsername(CASSANDRA_USERNAME)
                .withEncryptedPassword(CASSANDRA_ENCRYPTED_PASSWORD)
                .withPasswordDecrypter(readPwdDecrypter)
                .withKeyspace(CASSANDRA_KEYSPACE)
                .withTable(CASSANDRA_TABLE)
                .withCoder(SerializableCoder.of(Scientist.class))
                .withEntity(Scientist.class));

    PAssert.thatSingleton(output.apply("Count", Count.globally())).isEqualTo(NUM_ROWS);

    PCollection<KV<String, Integer>> mapped =
        output.apply(
            MapElements.via(
                new SimpleFunction<Scientist, KV<String, Integer>>() {
                  @Override
                  public KV<String, Integer> apply(Scientist scientist) {
                    return KV.of(scientist.name, scientist.id);
                  }
                }));
    PAssert.that(mapped.apply("Count occurrences per scientist", Count.perKey()))
        .satisfies(
            input -> {
              for (KV<String, Long> element : input) {
                assertEquals(element.getKey(), NUM_ROWS / 10, element.getValue().longValue());
              }
              return null;
            });

    pipeline.run();

    assertTrue(1L <= TestPasswordDecrypter.getNbCallsBySession(sessionReadUID));
  }

  @Test
  public void testWriteWithPasswordDecryption() {
    ArrayList<Scientist> scientists = buildScientists(NUM_ROWS);

    String sessionWriteUID = "session-write-" + UUID.randomUUID();
    PasswordDecrypter writePwdDecrypter = new TestPasswordDecrypter(sessionWriteUID);

    pipeline
        .apply(Create.of(scientists))
        .apply(
            CassandraIO.<Scientist>write()
                .withHosts(Arrays.asList(CASSANDRA_HOST))
                .withPort(CASSANDRA_PORT)
                .withUsername(CASSANDRA_USERNAME)
                .withEncryptedPassword(CASSANDRA_ENCRYPTED_PASSWORD)
                .withPasswordDecrypter(writePwdDecrypter)
                .withKeyspace(CASSANDRA_KEYSPACE)
                .withEntity(Scientist.class));

    pipeline.run();

    List<Row> results = getRows();
    assertEquals(NUM_ROWS, results.size());
    for (Row row : results) {
      assertTrue(row.getString("person_name").matches("Name (\\d*)"));
    }

    assertTrue(1L <= TestPasswordDecrypter.getNbCallsBySession(sessionWriteUID));
  }

  @Test
  public void testSplit() throws Exception {
    insertRecords();
    PipelineOptions options = PipelineOptionsFactory.create();
    CassandraIO.Read<Scientist> read =
        CassandraIO.<Scientist>read()
            .withHosts(Arrays.asList(CASSANDRA_HOST))
            .withPort(CASSANDRA_PORT)
            .withKeyspace(CASSANDRA_KEYSPACE)
            .withTable(CASSANDRA_TABLE)
            .withEntity(Scientist.class)
            .withCoder(SerializableCoder.of(Scientist.class));

    // initialSource will be read without splitting (which does not happen in production)
    // so we need to provide splitQueries to avoid NPE in source.reader.start()
    String splitQuery = QueryBuilder.select().from(CASSANDRA_KEYSPACE, CASSANDRA_TABLE).toString();
    CassandraIO.CassandraSource<Scientist> initialSource =
        new CassandraIO.CassandraSource<>(read, Collections.singletonList(splitQuery));

    int desiredBundleSizeBytes = 2000;
    List<BoundedSource<Scientist>> splits = initialSource.split(desiredBundleSizeBytes, options);
    SourceTestUtils.assertSourcesEqualReferenceSource(initialSource, splits, options);
    int expectedNumSplits =
        (int) initialSource.getEstimatedSizeBytes(options) / desiredBundleSizeBytes;
    assertEquals(expectedNumSplits, splits.size());
    int nonEmptySplits = 0;
    for (BoundedSource<Scientist> subSource : splits) {
      if (readFromSource(subSource, options).size() > 0) {
        nonEmptySplits += 1;
      }
    }
    assertEquals("Wrong number of empty splits", expectedNumSplits, nonEmptySplits);
  }

  private ArrayList<Scientist> buildScientists(long nbRows) {
    ArrayList<Scientist> scientists = new ArrayList<>();
    for (int i = 0; i < nbRows; i++) {
      Scientist scientist = new Scientist();
      scientist.id = i;
      scientist.name = "Name " + i;
      scientists.add(scientist);
    }
    return scientists;
  }

  private List<Row> getRows() {
    ResultSet result =
        session.execute(
            String.format(
                "select person_id,person_name from %s.%s", CASSANDRA_KEYSPACE, CASSANDRA_TABLE));
    return result.all();
  }

  @Test
  public void testDelete() throws Exception {
    insertRecords();
    List<Row> results = getRows();
    assertEquals(NUM_ROWS, results.size());

    Scientist einstein = new Scientist();
    einstein.id = 0;
    einstein.name = "Einstein";
    pipeline
        .apply(Create.of(einstein))
        .apply(
            CassandraIO.<Scientist>delete()
                .withHosts(Arrays.asList(CASSANDRA_HOST))
                .withPort(CASSANDRA_PORT)
                .withKeyspace(CASSANDRA_KEYSPACE)
                .withEntity(Scientist.class));

    pipeline.run();
    results = getRows();
    assertEquals(NUM_ROWS - 1, results.size());
  }

  @Test
  public void testValidPartitioner() {
    Assert.assertTrue(isMurmur3Partitioner(cluster));
  }

  @Test
  public void testDistance() {
    BigInteger distance = distance(new BigInteger("10"), new BigInteger("100"));
    assertEquals(BigInteger.valueOf(90), distance);

    distance = distance(new BigInteger("100"), new BigInteger("10"));
    assertEquals(new BigInteger("18446744073709551526"), distance);
  }

  @Test
  public void testRingFraction() {
    // simulate a first range taking "half" of the available tokens
    List<TokenRange> tokenRanges = new ArrayList<>();
    tokenRanges.add(new TokenRange(1, 1, BigInteger.valueOf(Long.MIN_VALUE), new BigInteger("0")));
    assertEquals(0.5, getRingFraction(tokenRanges), 0);

    // add a second range to cover all tokens available
    tokenRanges.add(new TokenRange(1, 1, new BigInteger("0"), BigInteger.valueOf(Long.MAX_VALUE)));
    assertEquals(1.0, getRingFraction(tokenRanges), 0);
  }

  @Test
  public void testEstimatedSizeBytesFromTokenRanges() {
    List<TokenRange> tokenRanges = new ArrayList<>();
    // one partition containing all tokens, the size is actually the size of the partition
    tokenRanges.add(
        new TokenRange(
            1, 1000, BigInteger.valueOf(Long.MIN_VALUE), BigInteger.valueOf(Long.MAX_VALUE)));
    assertEquals(1000, getEstimatedSizeBytesFromTokenRanges(tokenRanges));

    // one partition with half of the tokens, we estimate the size to the double of this partition
    tokenRanges = new ArrayList<>();
    tokenRanges.add(
        new TokenRange(1, 1000, BigInteger.valueOf(Long.MIN_VALUE), new BigInteger("0")));
    assertEquals(2000, getEstimatedSizeBytesFromTokenRanges(tokenRanges));

    // we have three partitions covering all tokens, the size is the sum of partition size *
    // partition count
    tokenRanges = new ArrayList<>();
    tokenRanges.add(
        new TokenRange(1, 1000, BigInteger.valueOf(Long.MIN_VALUE), new BigInteger("-3")));
    tokenRanges.add(new TokenRange(1, 1000, new BigInteger("-2"), new BigInteger("10000")));
    tokenRanges.add(
        new TokenRange(2, 3000, new BigInteger("10001"), BigInteger.valueOf(Long.MAX_VALUE)));
    assertEquals(8000, getEstimatedSizeBytesFromTokenRanges(tokenRanges));
  }

  /** Simple Cassandra entity used in test. */
  @Table(name = CASSANDRA_TABLE, keyspace = CASSANDRA_KEYSPACE)
  static class Scientist implements Serializable {

    @Column(name = "person_name")
    String name;

    @PartitionKey()
    @Column(name = "person_id")
    int id;

    @Override
    public String toString() {
      return id + ":" + name;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Scientist scientist = (Scientist) o;
      return id == scientist.id && Objects.equal(name, scientist.name);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(name, id);
    }
  }
}
