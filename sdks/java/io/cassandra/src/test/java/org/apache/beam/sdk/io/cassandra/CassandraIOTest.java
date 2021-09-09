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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Computed;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import info.archinnov.achilles.embedded.CassandraEmbeddedServerBuilder;
import info.archinnov.achilles.embedded.CassandraShutDownHook;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.common.NetworkTestHelper;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Objects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.MoreExecutors;
import org.apache.cassandra.service.StorageServiceMBean;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.AfterClass;
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
})
public class CassandraIOTest implements Serializable {
  private static final long NUM_ROWS = 22L;
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
  private static Session session;

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
    session = CassandraIOTest.cluster.newSession();
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
            "CREATE TABLE IF NOT EXISTS %s.%s(person_department text, person_id int, person_name text, PRIMARY KEY"
                + "((person_department), person_id));",
            CASSANDRA_KEYSPACE, CASSANDRA_TABLE));
    session.execute(
        String.format(
            "CREATE TABLE IF NOT EXISTS %s.%s(person_department text, person_id int, person_name text, PRIMARY KEY"
                + "((person_department), person_id));",
            CASSANDRA_KEYSPACE, CASSANDRA_TABLE_WRITE));

    LOG.info("Insert records");
    String[][] scientists = {
      new String[] {"phys", "Einstein"},
      new String[] {"bio", "Darwin"},
      new String[] {"phys", "Copernicus"},
      new String[] {"bio", "Pasteur"},
      new String[] {"bio", "Curie"},
      new String[] {"phys", "Faraday"},
      new String[] {"math", "Newton"},
      new String[] {"phys", "Bohr"},
      new String[] {"phys", "Galileo"},
      new String[] {"math", "Maxwell"},
      new String[] {"logic", "Russel"},
    };
    for (int i = 0; i < NUM_ROWS; i++) {
      int index = i % scientists.length;
      String insertStr =
          String.format(
              "INSERT INTO %s.%s(person_department, person_id, person_name) values("
                  + "'"
                  + scientists[index][0]
                  + "', "
                  + i
                  + ", '"
                  + scientists[index][1]
                  + "');",
              CASSANDRA_KEYSPACE,
              CASSANDRA_TABLE);
      session.execute(insertStr);
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
  public void testRead() throws Exception {
    PCollection<Scientist> output =
        pipeline.apply(
            CassandraIO.<Scientist>read()
                .withHosts(Collections.singletonList(CASSANDRA_HOST))
                .withPort(cassandraPort)
                .withKeyspace(CASSANDRA_KEYSPACE)
                .withTable(CASSANDRA_TABLE)
                .withMinNumberOfSplits(50)
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
              int count = 0;
              for (KV<String, Long> element : input) {
                count++;
                assertEquals(element.getKey(), NUM_ROWS / 10, element.getValue().longValue());
              }
              assertEquals(11, count);
              return null;
            });

    pipeline.run();
  }

  private CassandraIO.Read<Scientist> getReadWithRingRange(RingRange... rr) {
    return CassandraIO.<Scientist>read()
        .withHosts(Collections.singletonList(CASSANDRA_HOST))
        .withPort(cassandraPort)
        .withRingRanges(new HashSet<>(Arrays.asList(rr)))
        .withKeyspace(CASSANDRA_KEYSPACE)
        .withTable(CASSANDRA_TABLE)
        .withCoder(SerializableCoder.of(Scientist.class))
        .withEntity(Scientist.class);
  }

  private CassandraIO.Read<Scientist> getReadWithQuery(String query) {
    return CassandraIO.<Scientist>read()
        .withHosts(Collections.singletonList(CASSANDRA_HOST))
        .withPort(cassandraPort)
        .withQuery(query)
        .withKeyspace(CASSANDRA_KEYSPACE)
        .withTable(CASSANDRA_TABLE)
        .withCoder(SerializableCoder.of(Scientist.class))
        .withEntity(Scientist.class);
  }

  @Test
  public void testReadAllQuery() {
    String physQuery =
        String.format(
            "SELECT * From %s.%s WHERE person_department='phys' AND person_id=0;",
            CASSANDRA_KEYSPACE, CASSANDRA_TABLE);

    String mathQuery =
        String.format(
            "SELECT * From %s.%s WHERE person_department='math' AND person_id=6;",
            CASSANDRA_KEYSPACE, CASSANDRA_TABLE);

    PCollection<Scientist> output =
        pipeline
            .apply(Create.of(getReadWithQuery(physQuery), getReadWithQuery(mathQuery)))
            .apply(
                CassandraIO.<Scientist>readAll().withCoder(SerializableCoder.of(Scientist.class)));

    PCollection<String> mapped =
        output.apply(
            MapElements.via(
                new SimpleFunction<Scientist, String>() {
                  @Override
                  public String apply(Scientist scientist) {
                    return scientist.name;
                  }
                }));
    PAssert.that(mapped).containsInAnyOrder("Einstein", "Newton");
    PAssert.thatSingleton(output.apply("count", Count.globally())).isEqualTo(2L);
    pipeline.run();
  }

  @Test
  public void testReadAllRingRange() {
    RingRange physRR =
        fromEncodedKey(
            cluster.getMetadata(), TypeCodec.varchar().serialize("phys", ProtocolVersion.V3));

    RingRange mathRR =
        fromEncodedKey(
            cluster.getMetadata(), TypeCodec.varchar().serialize("math", ProtocolVersion.V3));

    RingRange logicRR =
        fromEncodedKey(
            cluster.getMetadata(), TypeCodec.varchar().serialize("logic", ProtocolVersion.V3));

    PCollection<Scientist> output =
        pipeline
            .apply(Create.of(getReadWithRingRange(physRR), getReadWithRingRange(mathRR, logicRR)))
            .apply(
                CassandraIO.<Scientist>readAll().withCoder(SerializableCoder.of(Scientist.class)));

    PCollection<KV<String, Integer>> mapped =
        output.apply(
            MapElements.via(
                new SimpleFunction<Scientist, KV<String, Integer>>() {
                  @Override
                  public KV<String, Integer> apply(Scientist scientist) {
                    return KV.of(scientist.department, scientist.id);
                  }
                }));

    PAssert.that(mapped.apply("Count occurrences per department", Count.perKey()))
        .satisfies(
            input -> {
              HashMap<String, Long> map = new HashMap<>();
              for (KV<String, Long> element : input) {
                map.put(element.getKey(), element.getValue());
              }
              assertEquals(3, map.size()); // do we have all three departments
              assertEquals(map.get("phys"), 10L, 0L);
              assertEquals(map.get("math"), 4L, 0L);
              assertEquals(map.get("logic"), 2L, 0L);
              return null;
            });

    pipeline.run();
  }

  @Test
  public void testReadWithQuery() throws Exception {
    PCollection<Scientist> output =
        pipeline.apply(
            CassandraIO.<Scientist>read()
                .withHosts(Collections.singletonList(CASSANDRA_HOST))
                .withPort(cassandraPort)
                .withKeyspace(CASSANDRA_KEYSPACE)
                .withTable(CASSANDRA_TABLE)
                .withMinNumberOfSplits(20)
                .withQuery(
                    "select person_id, writetime(person_name) from beam_ks.scientist where person_id=10 AND person_department='logic'")
                .withCoder(SerializableCoder.of(Scientist.class))
                .withEntity(Scientist.class));

    PAssert.thatSingleton(output.apply("Count", Count.globally())).isEqualTo(1L);
    PAssert.that(output)
        .satisfies(
            input -> {
              for (Scientist sci : input) {
                assertNull(sci.name);
                assertTrue(sci.nameTs != null && sci.nameTs > 0);
              }
              return null;
            });

    pipeline.run();
  }

  @Test
  public void testWrite() {
    ArrayList<ScientistWrite> data = new ArrayList<>();
    for (int i = 0; i < NUM_ROWS; i++) {
      ScientistWrite scientist = new ScientistWrite();
      scientist.id = i;
      scientist.name = "Name " + i;
      scientist.department = "bio";
      data.add(scientist);
    }

    pipeline
        .apply(Create.of(data))
        .apply(
            CassandraIO.<ScientistWrite>write()
                .withHosts(Collections.singletonList(CASSANDRA_HOST))
                .withPort(cassandraPort)
                .withKeyspace(CASSANDRA_KEYSPACE)
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

  private static final AtomicInteger counter = new AtomicInteger();

  private static class NOOPMapperFactory implements SerializableFunction<Session, Mapper> {

    @Override
    public Mapper apply(Session input) {
      return new NOOPMapper();
    }
  }

  private static class NOOPMapper implements Mapper<String>, Serializable {

    private final ListeningExecutorService executor =
        MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(10));

    final Callable<Void> asyncTask = () -> (null);

    @Override
    public Iterator map(ResultSet resultSet) {
      if (!resultSet.isExhausted()) {
        resultSet.iterator().forEachRemaining(r -> counter.getAndIncrement());
      }
      return Collections.emptyIterator();
    }

    @Override
    public Future<Void> deleteAsync(String entity) {
      counter.incrementAndGet();
      return executor.submit(asyncTask);
    }

    @Override
    public Future<Void> saveAsync(String entity) {
      counter.incrementAndGet();
      return executor.submit(asyncTask);
    }
  }

  @Test
  public void testReadWithMapper() throws Exception {
    counter.set(0);

    SerializableFunction<Session, Mapper> factory = new NOOPMapperFactory();

    pipeline.apply(
        CassandraIO.<String>read()
            .withHosts(Collections.singletonList(CASSANDRA_HOST))
            .withPort(cassandraPort)
            .withKeyspace(CASSANDRA_KEYSPACE)
            .withTable(CASSANDRA_TABLE)
            .withCoder(SerializableCoder.of(String.class))
            .withEntity(String.class)
            .withMapperFactoryFn(factory));
    pipeline.run();

    assertEquals(NUM_ROWS, counter.intValue());
  }

  @Test
  public void testCustomMapperImplWrite() throws Exception {
    counter.set(0);

    SerializableFunction<Session, Mapper> factory = new NOOPMapperFactory();

    pipeline
        .apply(Create.of(""))
        .apply(
            CassandraIO.<String>write()
                .withHosts(Collections.singletonList(CASSANDRA_HOST))
                .withPort(cassandraPort)
                .withKeyspace(CASSANDRA_KEYSPACE)
                .withMapperFactoryFn(factory)
                .withEntity(String.class));
    pipeline.run();

    assertEquals(1, counter.intValue());
  }

  @Test
  public void testCustomMapperImplDelete() {
    counter.set(0);

    SerializableFunction<Session, Mapper> factory = new NOOPMapperFactory();

    pipeline
        .apply(Create.of(""))
        .apply(
            CassandraIO.<String>delete()
                .withHosts(Collections.singletonList(CASSANDRA_HOST))
                .withPort(cassandraPort)
                .withKeyspace(CASSANDRA_KEYSPACE)
                .withMapperFactoryFn(factory)
                .withEntity(String.class));
    pipeline.run();

    assertEquals(1, counter.intValue());
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
    einstein.department = "phys";
    einstein.name = "Einstein";
    pipeline
        .apply(Create.of(einstein))
        .apply(
            CassandraIO.<Scientist>delete()
                .withHosts(Collections.singletonList(CASSANDRA_HOST))
                .withPort(cassandraPort)
                .withKeyspace(CASSANDRA_KEYSPACE)
                .withEntity(Scientist.class));

    pipeline.run();
    results = getRows(CASSANDRA_TABLE);
    assertEquals(NUM_ROWS - 1, results.size());
    // re-insert suppressed doc to make the test autonomous
    session.execute(
        String.format(
            "INSERT INTO %s.%s(person_department, person_id, person_name) values("
                + "'phys', "
                + einstein.id
                + ", '"
                + einstein.name
                + "');",
            CASSANDRA_KEYSPACE,
            CASSANDRA_TABLE));
  }

  /** Simple Cassandra entity used in read tests. */
  @Table(name = CASSANDRA_TABLE, keyspace = CASSANDRA_KEYSPACE)
  static class Scientist implements Serializable {

    @Column(name = "person_name")
    String name;

    @Computed("writetime(person_name)")
    Long nameTs;

    @ClusteringColumn()
    @Column(name = "person_id")
    int id;

    @PartitionKey
    @Column(name = "person_department")
    String department;

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
      return id == scientist.id
          && Objects.equal(name, scientist.name)
          && Objects.equal(department, scientist.department);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(name, id);
    }
  }

  private static RingRange fromEncodedKey(Metadata metadata, ByteBuffer... bb) {
    BigInteger bi = BigInteger.valueOf((long) metadata.newToken(bb).getValue());
    return RingRange.of(bi, bi.add(BigInteger.valueOf(1L)));
  }

  private static final String CASSANDRA_TABLE_WRITE = "scientist_write";
  /** Simple Cassandra entity used in write tests. */
  @Table(name = CASSANDRA_TABLE_WRITE, keyspace = CASSANDRA_KEYSPACE)
  static class ScientistWrite extends Scientist {}
}
