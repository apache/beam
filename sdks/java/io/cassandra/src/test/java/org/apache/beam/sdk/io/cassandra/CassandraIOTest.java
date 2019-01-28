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

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.BoundedSource.BoundedReader;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v20_0.com.google.common.base.Objects;
import org.apache.cassandra.service.StorageServiceMBean;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Tests of {@link CassandraIO}. */
public class CassandraIOTest implements Serializable {
  private static final long NUM_ROWS = 1000L;
  private static final String CASSANDRA_KEYSPACE = "beam_ks";
  private static final String CASSANDRA_HOST = "127.0.0.1";
  private static final String CASSANDRA_TABLE = "scientist";
  private static final Logger LOGGER = LoggerFactory.getLogger(CassandraIOTest.class);
  private static final String STORAGE_SERVICE_MBEAN = "org.apache.cassandra.db:type=StorageService";
  private static final String JMX_PORT = "7199";
  private static final long SIZE_ESTIMATES_UPDATE_INTERVAL = 5000L;
  private static final long STARTUP_TIMEOUT = 45000L;

  private static Cluster cluster;
  private static Session session;
  private static long startupTime;

  @Rule
  public transient TestPipeline pipeline = TestPipeline.create();

  @BeforeClass
  public static void startCassandra() throws Exception {
    System.setProperty("cassandra.jmx.local.port", JMX_PORT);
    startupTime = System.currentTimeMillis();
    EmbeddedCassandraServerHelper.startEmbeddedCassandra(
        "/cassandra.yaml", "target/cassandra", 30000);

    cluster = Cluster.builder().addContactPoint(CASSANDRA_HOST).withClusterName("beam").build();
    session = cluster.connect();

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
    EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
    session.close();
    cluster.close();
  }

  @Before
  public void purgeCassandra() throws Exception {
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
  public void testEstimatedSizeBytes() {
    final FakeCassandraService service = new FakeCassandraService();
    service.load();

    PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
    CassandraIO.Read<Scientist> spec = CassandraIO.<Scientist>read().withCassandraService(service);
    CassandraIO.CassandraSource<Scientist> source = new CassandraIO.CassandraSource<>(spec, null);
    long estimatedSizeBytes = source.getEstimatedSizeBytes(pipelineOptions);
    // the size is the sum of the bytes size of the String representation of a scientist in the map
    assertEquals(113890, estimatedSizeBytes);
  }

  @Test
  public void testRead() {
    FakeCassandraService service = new FakeCassandraService();
    service.load();

    PCollection<Scientist> output =
        pipeline.apply(
            CassandraIO.<Scientist>read()
                .withCassandraService(service)
                .withKeyspace("beam")
                .withTable("scientist")
                .withCoder(SerializableCoder.of(Scientist.class))
                .withEntity(Scientist.class));

    PAssert.thatSingleton(output.apply("Count", Count.globally())).isEqualTo(10000L);

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
                assertEquals(element.getKey(), 1000, element.getValue().longValue());
              }
              return null;
            });

    pipeline.run();
  }

  @Test
  public void testWrite() {
    FakeCassandraService service = new FakeCassandraService();

    ArrayList<Scientist> data = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      Scientist scientist = new Scientist();
      scientist.id = i;
      scientist.name = "Name " + i;
      data.add(scientist);
    }

    pipeline
        .apply(Create.of(data))
        .apply(
            CassandraIO.<Scientist>write()
                .withCassandraService(service)
                .withKeyspace("beam")
                .withEntity(Scientist.class));
    pipeline.run();

    assertEquals(1000, service.getTable().size());
    for (Scientist scientist : service.getTable().values()) {
      assertTrue(scientist.name.matches("Name (\\d*)"));
    }
  }

  @Test
  public void testDelete() {
    FakeCassandraService service = new FakeCassandraService();
    service.load();

    assertEquals(10000, service.getTable().size());

    pipeline
        .apply(
            CassandraIO.<Scientist>read()
                .withCassandraService(service)
                .withKeyspace("beam")
                .withTable("scientist")
                .withCoder(SerializableCoder.of(Scientist.class))
                .withEntity(Scientist.class))
        .apply(
            CassandraIO.<Scientist>delete()
                .withCassandraService(service)
                .withKeyspace("beam")
                .withEntity(Scientist.class));

    pipeline.run();

    assertEquals(0, service.getTable().size());
  }

  /** A {@link CassandraService} implementation that stores the entity in memory. */
  private static class FakeCassandraService implements CassandraService<Scientist> {
    private static final Map<Integer, Scientist> table = new ConcurrentHashMap<>();

    void load() {
      table.clear();
      String[] scientists = {
        "Lovelace",
        "Franklin",
        "Meitner",
        "Hopper",
        "Curie",
        "Faraday",
        "Newton",
        "Bohr",
        "Galilei",
        "Maxwell"
      };
      for (int i = 0; i < 10000; i++) {
        int index = i % scientists.length;
        Scientist scientist = new Scientist();
        scientist.id = i;
        scientist.name = scientists[index];
        table.put(scientist.id, scientist);
      }
    }

    Map<Integer, Scientist> getTable() {
      return table;
    }

    @Override
    public BoundedReader<Scientist> createReader(CassandraIO.CassandraSource<Scientist> source) {
      return new FakeCassandraReader(source);
    }

    private static class FakeCassandraReader extends BoundedSource.BoundedReader<Scientist> {
      private final CassandraIO.CassandraSource<Scientist> source;

      private Iterator<Scientist> iterator;
      private Scientist current;

      FakeCassandraReader(CassandraIO.CassandraSource<Scientist> source) {
        this.source = source;
      }

      @Override
      public boolean start() {
        iterator = table.values().iterator();
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
        iterator = null;
        current = null;
      }

      @Override
      public Scientist getCurrent() throws NoSuchElementException {
        if (current == null) {
          throw new NoSuchElementException();
        }
        return current;
      }

      @Override
      public CassandraIO.CassandraSource<Scientist> getCurrentSource() {
        return this.source;
      }
    }

    @Override
    public long getEstimatedSizeBytes(CassandraIO.Read spec) {
      long size = 0L;
      for (Scientist scientist : table.values()) {
        size = size + scientist.toString().getBytes(StandardCharsets.UTF_8).length;
      }
      return size;
    }

    @Override
    public List<BoundedSource<Scientist>> split(
        CassandraIO.Read<Scientist> spec, long desiredBundleSizeBytes) {
      return Collections.singletonList(new CassandraIO.CassandraSource<>(spec, null));
    }

    private static class FakeCassandraWriter implements Writer<Scientist> {
      @Override
      public void write(Scientist scientist) {
        table.put(scientist.id, scientist);
      }

      @Override
      public void close() {
        // nothing to do
      }
    }

    @Override
    public FakeCassandraWriter createWriter(CassandraIO.Mutate<Scientist> spec) {
      return new FakeCassandraWriter();
    }

    private static class FakeCassandraDeleter implements Deleter<Scientist> {
      @Override
      public void delete(Scientist scientist) {
        table.remove(scientist.id);
      }

      @Override
      public void close() {
        // nothing to do
      }
    }

    @Override
    public FakeCassandraDeleter createDeleter(CassandraIO.Mutate<Scientist> spec) {
      return new FakeCassandraDeleter();
    }
  }

  /** Simple Cassandra entity used in test. */
  @Table(name = "scientist", keyspace = "beam")
  static class Scientist implements Serializable {
    @Column(name = "person_name")
    String name;

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
