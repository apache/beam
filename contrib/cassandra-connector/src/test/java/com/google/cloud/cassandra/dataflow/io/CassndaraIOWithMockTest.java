package com.google.cloud.cassandra.dataflow.io;

import static org.junit.Assert.assertNotNull;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
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
 * Class to test CassndraIO.Write using mock objects
 */

public class CassndaraIOWithMockTest {
	private CassandraIO.Write.Bound<String> mockCassndaraIOWriter;
	private PCollection pcollection;
	private Pipeline pipeline;
	
	/**
	 * Create mocked objects for CassandraIO.Write.Bound
	 * set method returns for mocked object
	 **/
	@Before
	public void setup() {
		mockCassndaraIOWriter = Mockito.mock(CassandraIO.Write.Bound.class);
		Mockito.when(mockCassndaraIOWriter.getHosts()).thenReturn(new String[] { "localhost"});
		Mockito.when(mockCassndaraIOWriter.getPort()).thenReturn(9042);
		Mockito.when(mockCassndaraIOWriter.getKeyspace()).thenReturn("dev_keyspace");

		PipelineOptions options = PipelineOptionsFactory.create();
		pipeline = Pipeline.create(options);
		pcollection = pipeline.apply(Create.of(Arrays
				.asList(getCloumnFamilyRows())));
		
		Mockito.when(mockCassndaraIOWriter.apply(pcollection)).thenReturn(
				PDone.in(pipeline));
	}

	/** To Test Write to Cassandra **/
	@Test
	public void cassandraWriteTest() {
		PDone pdone = mockCassndaraIOWriter.apply(pcollection);
		System.out.println("Ponde "+pdone);
		PipelineResult result = pipeline.run();
		assertNotNull(result);
	}

	/** Get entities to persist */
	private Person[] getCloumnFamilyRows() {
		Random rn = new Random();
		int range = 10000;
		int pId = rn.nextInt(range);
		CassndaraIOWithMockTest.Person person = new CassndaraIOWithMockTest.Person();
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

}
