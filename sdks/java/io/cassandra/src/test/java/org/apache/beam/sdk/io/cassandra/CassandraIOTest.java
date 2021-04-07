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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertEquals;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.mapper.annotations.Computed;
import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
import com.datastax.oss.driver.api.mapper.annotations.DaoKeyspace;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import info.archinnov.achilles.embedded.CassandraEmbeddedServerBuilder;
import info.archinnov.achilles.embedded.CassandraShutDownHook;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.cassandra.CassandraIO.CassandraSource.TokenRange;
import org.apache.beam.sdk.io.common.NetworkTestHelper;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.SourceTestUtils;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Objects;
import org.apache.cassandra.service.StorageServiceMBean;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Tests of {@link CassandraIO}. */
@RunWith(JUnit4.class)
@SuppressWarnings({
  "rawtypes", // TODO(https://issues.apache.org/jira/browse/BEAM-10556)
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class CassandraIOTest implements Serializable {
  private static final long NUM_ROWS = 20L;
  private static final String CASSANDRA_KEYSPACE = "beam_ks";
  private static final String CASSANDRA_HOST = "127.0.0.1";
  private static final String CASSANDRA_TABLE = "scientist";
  private static final Logger LOG = LoggerFactory.getLogger(CassandraIOTest.class);
  private static final String STORAGE_SERVICE_MBEAN = "org.apache.cassandra.db:type=StorageService";
  private static final float ACCEPTABLE_EMPTY_SPLITS_PERCENTAGE = 0.5f;
  private static final int FLUSH_TIMEOUT = 30000;
  private static final int JMX_CONF_TIMEOUT = 1000;
  private static int jmxPort;
  private static int cassandraPort;

  private static Cluster cluster;
  private static CqlSession session;

  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();
  @Rule public transient TestPipeline pipeline = TestPipeline.create();
  private static CassandraShutDownHook shutdownHook;

  @BeforeClass
  public static void beforeClass() throws Exception {
    jmxPort = NetworkTestHelper.getAvailableLocalPort();
    shutdownHook = new CassandraShutDownHook();
    String data = TEMPORARY_FOLDER.newFolder("data").getAbsolutePath();
    String commitLog = TEMPORARY_FOLDER.newFolder("commit-log").getAbsolutePath();
    String cdcRaw = TEMPORARY_FOLDER.newFolder("cdc-raw").getAbsolutePath();
    String hints = TEMPORARY_FOLDER.newFolder("hints").getAbsolutePath();
    String savedCache = TEMPORARY_FOLDER.newFolder("saved-cache").getAbsolutePath();
    Files.createDirectories(Paths.get(savedCache));
    CassandraEmbeddedServerBuilder builder =
        CassandraEmbeddedServerBuilder.builder()
            .withKeyspaceName(CASSANDRA_KEYSPACE)
            .withDataFolder(data)
            .withCommitLogFolder(commitLog)
            .withCdcRawFolder(cdcRaw)
            .withHintsFolder(hints)
            .withSavedCachesFolder(savedCache)
            .withShutdownHook(shutdownHook)
            // randomized CQL port at startup
            .withJMXPort(jmxPort)
            .cleanDataFilesAtStartup(false);

    // under load we get a NoHostAvailable exception at cluster creation,
    // so retry to create it every 1 sec up to 3 times.
    cluster = buildCluster(builder);

    cassandraPort = cluster.getConfiguration().getProtocolOptions().getPort();

    CqlSessionBuilder cqlSessionBuilder =
        CqlSession.builder()
            .addContactPoint(new InetSocketAddress(CASSANDRA_HOST, cassandraPort))
            .withLocalDatacenter("datacenter1");

    session = cqlSessionBuilder.build();

    insertData();
    disableAutoCompaction();
  }

  private static Cluster buildCluster(CassandraEmbeddedServerBuilder builder) {
    int tried = 0;
    int delay = 5000;
    Exception exception = null;
    while (tried < 5) {
      try {
        return builder.buildNativeCluster();
      } catch (NoHostAvailableException e) {
        if (exception == null) {
          exception = e;
        } else {
          exception.addSuppressed(e);
        }
        tried++;
        try {
          Thread.sleep(delay);
        } catch (InterruptedException e1) {
          Thread thread = Thread.currentThread();
          thread.interrupt();
          throw new RuntimeException(String.format("Thread %s was interrupted", thread.getName()));
        }
      }
    }
    throw new RuntimeException(
        String.format(
            "Unable to create embedded Cassandra cluster: tried %d times with %d delay",
            tried, delay),
        exception);
  }

  @AfterClass
  public static void afterClass() throws InterruptedException, IOException {
    shutdownHook.shutDownNow();
  }

  private static void insertData() throws Exception {
    LOG.info("Create Cassandra tables");
    session.execute(
        String.format(
            "CREATE TABLE IF NOT EXISTS %s.%s(person_id int, person_name text, PRIMARY KEY"
                + "(person_id));",
            CASSANDRA_KEYSPACE, CASSANDRA_TABLE));
    session.execute(
        String.format(
            "CREATE TABLE IF NOT EXISTS %s.%s(person_id int, person_name text, PRIMARY KEY"
                + "(person_id));",
            CASSANDRA_KEYSPACE, CASSANDRA_TABLE_WRITE));

    LOG.info("Insert records");
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
    flushMemTablesAndRefreshSizeEstimates();
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
  @SuppressWarnings("unused")
  private static void flushMemTablesAndRefreshSizeEstimates() throws Exception {
    JMXServiceURL url =
        new JMXServiceURL(
            String.format(
                "service:jmx:rmi://%s/jndi/rmi://%s:%s/jmxrmi",
                CASSANDRA_HOST, CASSANDRA_HOST, jmxPort));
    JMXConnector jmxConnector = JMXConnectorFactory.connect(url, null);
    MBeanServerConnection mBeanServerConnection = jmxConnector.getMBeanServerConnection();
    ObjectName objectName = new ObjectName(STORAGE_SERVICE_MBEAN);
    StorageServiceMBean mBeanProxy =
        JMX.newMBeanProxy(mBeanServerConnection, objectName, StorageServiceMBean.class);
    mBeanProxy.forceKeyspaceFlush(CASSANDRA_KEYSPACE, CASSANDRA_TABLE);
    mBeanProxy.refreshSizeEstimates();
    jmxConnector.close();
    Thread.sleep(FLUSH_TIMEOUT);
  }

  /**
   * Disable auto compaction on embedded cassandra host, to avoid race condition in temporary files
   * cleaning.
   */
  @SuppressWarnings("unused")
  private static void disableAutoCompaction() throws Exception {
    JMXServiceURL url =
        new JMXServiceURL(
            String.format(
                "service:jmx:rmi://%s/jndi/rmi://%s:%s/jmxrmi",
                CASSANDRA_HOST, CASSANDRA_HOST, jmxPort));
    JMXConnector jmxConnector = JMXConnectorFactory.connect(url, null);
    MBeanServerConnection mBeanServerConnection = jmxConnector.getMBeanServerConnection();
    ObjectName objectName = new ObjectName(STORAGE_SERVICE_MBEAN);
    StorageServiceMBean mBeanProxy =
        JMX.newMBeanProxy(mBeanServerConnection, objectName, StorageServiceMBean.class);
    mBeanProxy.disableAutoCompaction(CASSANDRA_KEYSPACE, CASSANDRA_TABLE);
    jmxConnector.close();
    Thread.sleep(JMX_CONF_TIMEOUT);
  }

  @Test
  public void testEstimatedSizeBytes() throws Exception {
    SerializableFunction<CqlSession, BaseDao<Scientist>> daoMapperFunction =
        (session) -> {
          ScientistMapper scientistMapper =
              new CassandraIOTest_ScientistMapperBuilder(session).build();
          ScientistDao dao =
              scientistMapper.scientistDao(CqlIdentifier.fromCql(CASSANDRA_KEYSPACE));
          return dao;
        };

    PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
    CassandraIO.Read<Scientist> read =
        CassandraIO.<Scientist>read()
            .withHosts(Collections.singletonList(CASSANDRA_HOST))
            .withPort(cassandraPort)
            .withKeyspace(CASSANDRA_KEYSPACE)
            .withMapperFactoryFn(daoMapperFunction)
            .withLocalDc("datacenter1")
            .withConnectTimeout(5000)
            .withTable(CASSANDRA_TABLE);
    CassandraIO.CassandraSource<Scientist> source = new CassandraIO.CassandraSource<>(read, null);
    long estimatedSizeBytes = source.getEstimatedSizeBytes(pipelineOptions);
    // the size is non determanistic in Cassandra backend: checks that estimatedSizeBytes >= 12960L
    // -20%  && estimatedSizeBytes <= 12960L +20%
    assertThat(
        "wrong estimated size in " + CASSANDRA_KEYSPACE + "/" + CASSANDRA_TABLE,
        estimatedSizeBytes,
        greaterThan(0L));
  }

  @Test
  public void testRead() throws Exception {
    SerializableFunction<CqlSession, BaseDao<Scientist>> daoMapperFunction =
        (session) -> {
          ScientistMapper scientistMapper =
              new CassandraIOTest_ScientistMapperBuilder(session).build();
          ScientistDao dao =
              scientistMapper.scientistDao(CqlIdentifier.fromCql(CASSANDRA_KEYSPACE));
          return dao;
        };

    PCollection<Scientist> output =
        pipeline.apply(
            CassandraIO.<Scientist>read()
                .withHosts(Collections.singletonList(CASSANDRA_HOST))
                .withPort(cassandraPort)
                .withKeyspace(CASSANDRA_KEYSPACE)
                .withTable(CASSANDRA_TABLE)
                .withMapperFactoryFn(daoMapperFunction)
                .withLocalDc("datacenter1")
                .withConnectTimeout(5000)
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
  /*
    @Test
    public void testReadWithQuery() throws Exception {
      SerializableFunction<CqlSession, BaseDao<Scientist>> daoMapperFunction =
          (session) -> {
            ScientistMapper scientistMapper =
                new CassandraIOTest_ScientistMapperBuilder(session).build();
            ScientistDao dao =
                scientistMapper.scientistDao(CqlIdentifier.fromCql(CASSANDRA_KEYSPACE));
            return dao;
          };

      PCollection<Scientist> output =
          pipeline.apply(
              CassandraIO.<Scientist>read()
                  .withHosts(Collections.singletonList(CASSANDRA_HOST))
                  .withPort(cassandraPort)
                  .withKeyspace(CASSANDRA_KEYSPACE)
                  .withTable(CASSANDRA_TABLE)
                  .withQuery(
                      "select person_id,writetime(person_name) as name_ts from beam_ks.scientist where person_id=10")
                  .withMapperFactoryFn(daoMapperFunction)
                  .withLocalDc("datacenter1")
                  .withConnectTimeout(5000)
                  .withCoder(SerializableCoder.of(Scientist.class))
                  .withEntity(Scientist.class));

      PAssert.thatSingleton(output.apply("Count", Count.globally())).isEqualTo(1L);
      PAssert.that(output)
          .satisfies(
              input -> {
                for (Scientist sci : input) {
                  LOG.info("TO CHECK:" + sci.name + ":" + sci.id + ":" + sci.nameTs);
                  assertNull(sci.name);
                  assertTrue(sci.nameTs > 0);
                }
                return null;
              });

      pipeline.run();
    }
  */
  @Test
  public void testWrite() {
    ArrayList<ScientistWrite> data = new ArrayList<>();
    for (int i = 0; i < NUM_ROWS; i++) {
      ScientistWrite scientist = new ScientistWrite();
      scientist.id = i;
      scientist.name = "Name " + i;
      data.add(scientist);
    }
    SerializableFunction<CqlSession, BaseDao<ScientistWrite>> daoMapperFunction =
        (session) -> {
          ScientistWriteMapper scientistWriteMapper =
              new CassandraIOTest_ScientistWriteMapperBuilder(session).build();
          ScientistWriteDao dao =
              scientistWriteMapper.scientistWriteDao(CqlIdentifier.fromCql(CASSANDRA_KEYSPACE));
          return dao;
        };
    pipeline
        .apply(Create.of(data))
        .apply(
            CassandraIO.<ScientistWrite>write()
                .withHosts(Collections.singletonList(CASSANDRA_HOST))
                .withPort(cassandraPort)
                .withKeyspace(CASSANDRA_KEYSPACE)
                .withMapperFactoryFn(daoMapperFunction)
                .withLocalDc("datacenter1")
                .withConnectTimeout(5000)
                .withEntity(ScientistWrite.class));
    // table to write to is specified in the entity in @Table annotation (in that case
    // scientist_write)
    pipeline.run();

    List<Row> results = getRows(CASSANDRA_TABLE_WRITE);
    assertEquals(NUM_ROWS, results.size());
    for (Row row : results) {
      assertTrue(row.getString("person_name").matches("Name (\\d*)"));
    }
  }

  @Test
  public void testSplit() throws Exception {
    SerializableFunction<CqlSession, BaseDao<Scientist>> daoMapperFunction =
        (session) -> {
          ScientistMapper scientistMapper =
              new CassandraIOTest_ScientistMapperBuilder(session).build();
          ScientistDao dao =
              scientistMapper.scientistDao(CqlIdentifier.fromCql(CASSANDRA_KEYSPACE));
          return dao;
        };

    PipelineOptions options = PipelineOptionsFactory.create();
    CassandraIO.Read<Scientist> read =
        CassandraIO.<Scientist>read()
            .withHosts(Collections.singletonList(CASSANDRA_HOST))
            .withPort(cassandraPort)
            .withKeyspace(CASSANDRA_KEYSPACE)
            .withTable(CASSANDRA_TABLE)
            .withMapperFactoryFn(daoMapperFunction)
            .withLocalDc("datacenter1")
            .withConnectTimeout(5000)
            .withEntity(Scientist.class)
            .withCoder(SerializableCoder.of(Scientist.class));

    // initialSource will be read without splitting (which does not happen in production)
    // so we need to provide splitQueries to avoid NPE in source.reader.start()
    String splitQuery =
        QueryBuilder.selectFrom(CASSANDRA_KEYSPACE, CASSANDRA_TABLE).all().toString();

    CassandraIO.CassandraSource<Scientist> initialSource =
        new CassandraIO.CassandraSource<>(read, Collections.singletonList(splitQuery));
    int desiredBundleSizeBytes = 2048;
    long estimatedSize = initialSource.getEstimatedSizeBytes(options);
    List<BoundedSource<Scientist>> splits = initialSource.split(desiredBundleSizeBytes, options);
    SourceTestUtils.assertSourcesEqualReferenceSource(initialSource, splits, options);
    float expectedNumSplitsloat =
        (float) initialSource.getEstimatedSizeBytes(options) / desiredBundleSizeBytes;
    long sum = 0;

    for (BoundedSource<Scientist> subSource : splits) {
      sum += subSource.getEstimatedSizeBytes(options);
    }

    // due to division and cast estimateSize != sum but will be close. Exact equals checked below
    assertEquals((long) (estimatedSize / splits.size()) * splits.size(), sum);

    int expectedNumSplits = (int) Math.ceil(expectedNumSplitsloat);
    assertEquals("Wrong number of splits", expectedNumSplits, splits.size());
    int emptySplits = 0;
    for (BoundedSource<Scientist> subSource : splits) {
      if (readFromSource(subSource, options).isEmpty()) {
        emptySplits += 1;
      }
    }
    assertThat(
        "There are too many empty splits, parallelism is sub-optimal",
        emptySplits,
        lessThan((int) (ACCEPTABLE_EMPTY_SPLITS_PERCENTAGE * splits.size())));
  }

  private List<Row> getRows(String table) {
    ResultSet result =
        session.execute(
            String.format("select person_id,person_name from %s.%s", CASSANDRA_KEYSPACE, table));
    return result.all();
  }

  @Test
  public void testDelete() throws Exception {
    List<Row> results = getRows(CASSANDRA_TABLE);
    assertEquals(NUM_ROWS, results.size());

    Scientist einstein = new Scientist();
    einstein.id = 0;
    einstein.name = "Einstein";

    SerializableFunction<CqlSession, BaseDao<Scientist>> daoMapperFunction =
        (session) -> {
          ScientistMapper scientistMapper =
              new CassandraIOTest_ScientistMapperBuilder(session).build();
          ScientistDao dao =
              scientistMapper.scientistDao(CqlIdentifier.fromCql(CASSANDRA_KEYSPACE));
          return dao;
        };

    pipeline
        .apply(Create.of(einstein))
        .apply(
            CassandraIO.<Scientist>delete()
                .withHosts(Collections.singletonList(CASSANDRA_HOST))
                .withPort(cassandraPort)
                .withMapperFactoryFn(daoMapperFunction)
                .withConnectTimeout(5000)
                .withLocalDc("datacenter1")
                .withKeyspace(CASSANDRA_KEYSPACE)
                .withEntity(Scientist.class));

    pipeline.run();
    results = getRows(CASSANDRA_TABLE);
    assertEquals(NUM_ROWS - 1, results.size());
    // re-insert suppressed doc to make the test autonomous
    session.execute(
        String.format(
            "INSERT INTO %s.%s(person_id, person_name) values("
                + einstein.id
                + ", '"
                + einstein.name
                + "');",
            CASSANDRA_KEYSPACE,
            CASSANDRA_TABLE));
  }

  @Test
  public void testValidPartitioner() {
    Assert.assertTrue(isMurmur3Partitioner(session));
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

  /** Simple Cassandra entity used in read tests. */
  @Entity(defaultKeyspace = CASSANDRA_KEYSPACE)
  @CqlName(CASSANDRA_TABLE)
  public static class Scientist implements Serializable {

    @CqlName("person_name")
    public String name;

    @Computed("writetime(person_name)")
    @CqlName("name_ts")
    public Long nameTs;

    @CqlName("person_id")
    @PartitionKey
    public Integer id;

    public Scientist() {}

    public Scientist(int id, String name) {
      super();
      this.id = id;
      this.name = name;
    }

    public Integer getId() {
      return id;
    }

    public void setId(Integer id) {
      this.id = id;
    }

    /*    public Long getNameTs() {
      return nameTs;
    }

    public void setNameTs(Long nameTs) {
      this.nameTs = nameTs;
    }*/

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    @Override
    public String toString() {
      return id + ":" + name;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Scientist scientist = (Scientist) o;
      return id.equals(scientist.id) && Objects.equal(name, scientist.name);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(name, id);
    }
  }

  @Dao
  public interface ScientistDao extends BaseDao<Scientist> {}

  @Mapper
  public interface ScientistMapper {
    @DaoFactory
    ScientistDao scientistDao(@DaoKeyspace CqlIdentifier keyspace);
  }

  private static final String CASSANDRA_TABLE_WRITE = "scientist_write";

  /** Simple Cassandra entity used in write tests. */
  @Entity(defaultKeyspace = CASSANDRA_KEYSPACE)
  @CqlName(CASSANDRA_TABLE_WRITE)
  public static class ScientistWrite extends Scientist implements Serializable {
    public ScientistWrite() {}
  }

  @Dao
  public interface ScientistWriteDao extends BaseDao<ScientistWrite> {}

  @Mapper
  public interface ScientistWriteMapper {
    @DaoFactory
    ScientistWriteDao scientistWriteDao(@DaoKeyspace CqlIdentifier keyspace);
  }
}
