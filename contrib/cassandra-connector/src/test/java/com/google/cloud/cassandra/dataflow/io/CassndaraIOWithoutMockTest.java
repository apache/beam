package com.google.cloud.cassandra.dataflow.io;

import static org.junit.Assert.assertNotNull;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.AlreadyExistsException;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.PipelineResult;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PDone;

/**
 * To test write to cassandara.
 * Ton run this test, cassandra instance is required in local/google cloud. This test requires a
 * columnFamily (table) "Person" in cassandra database. under "dev_keyspace".
 * Following is the structure of "Person" table CREATE TABLE dev_keyspace.person
 * (person_id int PRIMARY KEY, person_name text);
 *
 */
public class CassndaraIOWithoutMockTest {

	private CassandraIO.Write.Bound<String> CassndaraIOWriter;
	private String[] hosts;
	private String keyspace;
	private int port;
	private Cluster cluster;
	private Session session;


	/** set hosts,keyspace,port
	 * Connect to Cassandra
	 * Create Keyspace
	 * Use Created Keyspace
	 * Create Column family
	 * Create CassandraIO Writer
	 **/
	
	@Before
	public void setup()  {
		this.hosts = new String[] { "localhost" };
		this.keyspace = "dev_keyspace";
		this.port = 9042;
		try{
		connect(getCassandaraHostAndPort());
		createKeyspace();
		useKeyspace();
		createColumnFamily();
		CassndaraIOWriter = new CassandraIO.Write.Bound<String>(hosts, keyspace, port);
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	/** Connect to Cassandra */
	public void connect(Collection<InetSocketAddress> inetSocketAddresses) {		
		cluster = Cluster.builder()
				.addContactPointsWithPorts(inetSocketAddresses).build();
		session = cluster.connect();			
	}

	/** Create Keyspace Keyspace,ignore exception if already exists */
	private void createKeyspace() {
		try {
			session.execute("CREATE KEYSPACE " + keyspace
					+ " WITH replication "
					+ "= {'class':'SimpleStrategy', 'replication_factor':3};");
		} catch (AlreadyExistsException ex) {
		}
	}

	/** Use Created Keyspace */
	private void useKeyspace() {
		session.execute("USE " + keyspace);
	}

	/** Create Column Family ,ignore exception if already exists */
	private void createColumnFamily() {
		try {
			session.execute("CREATE TABLE " + "person("
					+ "person_id int PRIMARY KEY" + "," + "person_name text);");
		} catch (AlreadyExistsException ex) {
		}
	}

	
	/** Create Cassandra ip socket address collection */
	private Collection<InetSocketAddress> getCassandaraHostAndPort() {
		Collection<InetSocketAddress> addresses = new ArrayList<InetSocketAddress>();
		for (String host : hosts) {
			addresses.add(new InetSocketAddress(host, port));
		}
		return addresses;
	}

	/** To Test Write to Cassandra **/
	@Test
	public void cassandraWriteTest() {
		try{
		PipelineOptions options = PipelineOptionsFactory.create();
		Pipeline p = Pipeline.create(options);
		@SuppressWarnings("rawtypes")
		PCollection pcollection = p.apply(Create.of(Arrays
				.asList(getCloumnFamilyRows())));
		@SuppressWarnings("unused")
		PDone pDone = CassndaraIOWriter.apply(pcollection);
		PipelineResult result=null;
		result= p.run();
		assertNotNull(result);
		}catch(Exception e){
			Assert.fail("Test failed");
		}		
	}

	/** Get entities to persist */
	private Person[] getCloumnFamilyRows() {
		Random rn = new Random();
		int range = 10000;
		int pId = rn.nextInt(range);
		CassndaraIOWithoutMockTest.Person person = new CassndaraIOWithoutMockTest.Person();
		person.setPersonId(pId);
		person.setName("PERSON-" + pId);
		Person[] persons = { person };
		return persons;
	}

	@Table(name = "person")
	public static class Person implements Serializable {
		private static final long serialVersionUID = 1L;
		@Column(name = "PERSON_NAME")
		private String name;
		@Column(name = "PERSON_ID")
		private int personId;

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public int getPersonId() {
			return personId;
		}

		public void setPersonId(int personId) {
			this.personId = personId;
		}
	}

	@After
	public void closeResources() {
		try{
		cluster.close();
		session.close();
		}catch(Exception e){
			e.printStackTrace();
		}
	}

}
