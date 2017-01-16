package org.apache.beam.sdk.io.hadoop.inputformat.unit.tests;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIO;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.MethodSorters;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;

/**
 *
 * Tests to validate HadoopInputFormatIO for embedded Cassandra instance.
 *
 */
@RunWith(JUnit4.class)
@FixMethodOrder(MethodSorters.JVM)
public class HIFIOWithCassandraTest implements Serializable {
  private static final long serialVersionUID = 1L;
  private static final String CASSANDRA_KEYSPACE = "hif_keyspace";
  private static final String CASSANDRA_HOST = "127.0.0.1";
  private static final String CASSANDRA_TABLE = "person";
  private static transient Cluster cluster;
  private static transient Session session;
  @Rule
  public final transient TestPipeline p = TestPipeline.create();

  @BeforeClass
  public static void startCassandra() throws Exception {
    EmbeddedCassandraServerHelper.startEmbeddedCassandra("/cassandra.yaml", "target/cassandra",
        Long.MAX_VALUE);
    cluster = Cluster.builder().addContactPoint(CASSANDRA_HOST).withClusterName("beam").build();
    session = cluster.connect();
  }

  @Before
  public void createTable() throws Exception {
    session.execute("CREATE KEYSPACE " + CASSANDRA_KEYSPACE
        + " WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':1};");
    session.execute("USE " + CASSANDRA_KEYSPACE);
    session
        .execute("CREATE TABLE person(person_id int, person_name text, PRIMARY KEY(person_id));");
    session.execute("INSERT INTO person(person_id, person_name) values(0, 'John Foo');");
    session.execute("INSERT INTO person(person_id, person_name) values(1, 'David Bar');");
  }

  /**
   * Test to read data from embedded Cassandra instance and verify whether data is read
   * successfully.
   *
   * @throws Exception
   */
  @Test
  public void testHIFReadForCassandra() throws Exception {
    Pipeline p = TestPipeline.create();
    Configuration conf = getConfiguration();
    SimpleFunction<Row, String> myValueTranslate = new SimpleFunction<Row, String>() {
      private static final long serialVersionUID = 1L;

      @Override
      public String apply(Row input) {
        return input.getString("person_name");
      }
    };
    PCollection<KV<Long, String>> cassandraData =
        p.apply(HadoopInputFormatIO.<Long, String>read().withConfiguration(conf)
            .withValueTranslation(myValueTranslate));
    PAssert.thatSingleton(cassandraData.apply("Count", Count.<KV<Long, String>>globally()))
        .isEqualTo(2L);
    List<KV<Long, String>> expectedResults =
        Arrays.asList(KV.of(2L, "John Foo"), KV.of(1L, "David Bar"));
    PAssert.that(cassandraData).containsInAnyOrder(expectedResults);
    p.run();
  }

  /**
   * Test to read data from embedded Cassandra instance based on query and verify whether data is
   * read successfully.
   * @throws Exception
   */
  @Test
  public void testHIFReadForCassandraQuery() throws Exception {
    Pipeline p = TestPipeline.create();
    Configuration conf = getConfiguration();
    conf.set(
        "cassandra.input.cql",
        "select * from hif_keyspace.person where token(person_id) > ? and token(person_id) <= ? and person_name='David Bar' allow filtering");
    SimpleFunction<Row, String> myValueTranslate = new SimpleFunction<Row, String>() {
      private static final long serialVersionUID = 1L;

      @Override
      public String apply(Row input) {
        return input.getString("person_name");
      }
    };
    PCollection<KV<Long, String>> cassandraData =
        p.apply(HadoopInputFormatIO.<Long, String>read().withConfiguration(conf)
            .withValueTranslation(myValueTranslate));
    PAssert.thatSingleton(cassandraData.apply("Count", Count.<KV<Long, String>>globally()))
        .isEqualTo(1L);

    p.run();
  }

  @After
  public void dropTable() throws Exception {
    session.execute("Drop TABLE " + CASSANDRA_TABLE);
    session.execute("Drop KEYSPACE " + CASSANDRA_KEYSPACE);
  }

  @AfterClass
  public static void stopCassandra() throws Exception {
    EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
  }

  @Table(name = "person", keyspace = CASSANDRA_KEYSPACE)
  public static class Person implements Serializable {
    private static final long serialVersionUID = 1L;
    @Column(name = "person_name")
    private String name;
    @Column(name = "person_id")
    private int id;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public int getId() {
      return id;
    }

    public void setId(int id) {
      this.id = id;
    }

    public String toString() {
      return id + ":" + name;
    }
  }

  public Configuration getConfiguration() {
    Configuration conf = new Configuration();
    conf.set("cassandra.input.thrift.port", "9061");
    conf.set("cassandra.input.thrift.address", CASSANDRA_HOST);
    conf.set("cassandra.input.partitioner.class", "Murmur3Partitioner");
    conf.set("cassandra.input.keyspace", CASSANDRA_KEYSPACE);
    conf.set("cassandra.input.columnfamily", CASSANDRA_TABLE);
    conf.setClass("mapreduce.job.inputformat.class",
        org.apache.cassandra.hadoop.cql3.CqlInputFormat.class, InputFormat.class);
    conf.setClass("key.class", java.lang.Long.class, Object.class);
    conf.setClass("value.class", com.datastax.driver.core.Row.class, Object.class);
    return conf;
  }

}
