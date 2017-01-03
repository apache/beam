package org.apache.beam.sdk.io.hadoop.inputformat.integration.tests;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
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
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;


public class HIFIOWithCassandraIT implements Serializable {
	private static final long serialVersionUID = 1L;
	private static final String CASSANDRA_KEYSPACE = "hif_keyspace";
	private static final String CASSANDRA_HOST = "127.0.0.1";
	private static final String CASSANDRA_TABLE = "person";
	private static final Logger LOGGER = LoggerFactory.getLogger(HIFIOWithCassandraIT.class);
	private transient Cluster cluster;
	private transient Session session;

	@Before
	public void startCassandra() throws Exception {
		EmbeddedCassandraServerHelper.startEmbeddedCassandra("/cassandra.yaml","target/cassandra", 999999999);
		cluster = Cluster.builder().addContactPoint(CASSANDRA_HOST).withClusterName("beam").build();
		session = cluster.connect();
		LOGGER.info("Creating the Cassandra keyspace");
		session.execute("CREATE KEYSPACE " + CASSANDRA_KEYSPACE + " WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':3};");
		LOGGER.info(CASSANDRA_KEYSPACE + " keyspace created");
		LOGGER.info("Use the Cassandra keyspace");
		session.execute("USE " + CASSANDRA_KEYSPACE);
		LOGGER.info("Create Cassandra table");
		session.execute("CREATE TABLE person(person_id int, person_name text, PRIMARY KEY(person_id));");
		LOGGER.info("Insert records");
		session.execute("INSERT INTO person(person_id, person_name) values(0, 'John Foo');");
		session.execute("INSERT INTO person(person_id, person_name) values(1, 'David Bar');");
	}


	@Test
	public void testHIFReadForCassandra() throws IOException {
		Pipeline p = TestPipeline.create();
		Configuration conf = new Configuration();
		conf.set("cassandra.input.thrift.port","9061");
		conf.set("cassandra.input.thrift.address", CASSANDRA_HOST);
		conf.set("cassandra.input.partitioner.class", "Murmur3Partitioner");
		conf.set("cassandra.input.keyspace", CASSANDRA_KEYSPACE);
		conf.set("cassandra.input.columnfamily", CASSANDRA_TABLE);
		conf.setClass("mapreduce.job.inputformat.class",org.apache.cassandra.hadoop.cql3.CqlInputFormat.class, InputFormat.class);
		conf.setClass("key.class", java.lang.Long.class, Object.class);
		conf.setClass("value.class", com.datastax.driver.core.Row.class, Object.class);
		SimpleFunction<Row, String> myValueTranslate = new SimpleFunction<Row, String>() {
			private static final long serialVersionUID = 1L;
			@Override
			public String apply(Row input) {
				return input.getString("person_name");
			}
		};
		PCollection<KV<Long,String>> cassandraData =(PCollection<KV<Long, String>>)  p.apply(HadoopInputFormatIO
				.<Long, String> read().withConfiguration(conf)
				.withValueTranslation(myValueTranslate));
		PAssert.thatSingleton(cassandraData.apply("Count", Count.<KV<Long,String>>globally())).isEqualTo(2L);
		List<KV<Long,String>> expectedResults=Arrays.asList(KV.of(2L,"John Foo"),KV.of(1L, "David Bar"));
		PAssert.that(cassandraData).containsInAnyOrder(expectedResults);
		p.run().waitUntilFinish();
	}


	@After
	public void stopCassandra() throws Exception {
		EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
	}
	
	@DefaultCoder(AvroCoder.class)
	class MyCassandraRow implements Serializable{
		private static final long serialVersionUID = 1L;
		private String person_name;
		public MyCassandraRow(String person_name) {
			this.person_name = person_name;
		}
		public String getPerson_name() {
			return person_name;
		}
		public void setPerson_name(String person_name) {
			this.person_name = person_name;
		}

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

}
