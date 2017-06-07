package com.google.cloud.cassandra.dataflow.io;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.datastax.driver.mapping.annotations.Table;
import com.google.cloud.cassandra.dataflow.io.CassandraReadIO.Source;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BoundedSource;
import com.google.cloud.dataflow.sdk.io.Read;
import com.google.cloud.dataflow.sdk.io.Read.Bounded;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.Flatten;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionList;
import com.google.cloud.dataflow.sdk.values.POutput;

public class CassandraReadIOWithMockTest {
	private static CassandraReadIO.Source mockCassandraRead;
	private long desiredBundleSizeBytes = 64 * (1 << 20);
	private static Iterator mockIterator;
	private static List<BoundedSource> mockSplitedSourceList;

	private static PipelineOptions mockPipelineOptions;
	private static Pipeline mockPipeline;

	/**
	 * Initial setup for mocking objects
	 */
	@Before
	public void setUp() {
		mockPipelineOptions =  Mockito.spy(PipelineOptions.class);
		mockPipeline = Mockito.mock(Pipeline.class);
		mockCassandraRead = Mockito.mock(CassandraReadIO.Source.class,Mockito.withSettings().serializable());
		mockSplitedSourceList = Mockito.spy(ArrayList.class);
		mockIterator =  Mockito.spy(Iterator.class);
	}
	/**
	 * Test for checking single source split and PCollection object
	 */
	@Test
	public void testToGetSingleSource() {
		try {
			
			mockSplitedSourceList.add(mockCassandraRead);
			Mockito.when(mockCassandraRead.splitIntoBundles(desiredBundleSizeBytes, mockPipelineOptions)).thenReturn(mockSplitedSourceList);			
			Assert.assertEquals(1, mockSplitedSourceList.size());
			Mockito.when(mockSplitedSourceList.iterator()).thenReturn(mockIterator);
			PCollection mockPCollection =  Mockito.mock(PCollection.class);
			Mockito.when(mockPipeline.apply(Mockito.mock(Bounded.class))).thenReturn(mockPCollection);
			mockPipeline.run();
			Assert.assertNotNull(mockPCollection);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Test for checking multiple source splits and PCollection Object
	 */
	@Test
	public void testToGetMultipleSplitedSource() {
		try {
			desiredBundleSizeBytes = 1024;
			mockSplitedSourceList.add(mockCassandraRead);
			mockSplitedSourceList.add(mockCassandraRead);
			Mockito.when(mockCassandraRead.splitIntoBundles(desiredBundleSizeBytes, mockPipelineOptions)).thenReturn(mockSplitedSourceList);
			
			Assert.assertNotNull(mockSplitedSourceList.size());
			PCollectionList mockPCollectionList =  Mockito.mock(PCollectionList.class);
			Mockito.when(mockSplitedSourceList.iterator()).thenReturn(mockIterator);
			Mockito.when(mockIterator.hasNext()).thenReturn(true,false);
			Mockito.when(mockPipeline.apply(Mockito.mock(Bounded.class))).thenReturn(mockPCollectionList);
			
			PCollection mockMergedPColl =  Mockito.mock(PCollection.class);
			Mockito.when(mockPCollectionList.apply(Flatten.pCollections())).thenReturn(mockMergedPColl);
			mockPipeline.run();
			Assert.assertNotNull(mockMergedPColl);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * static inner class contains employee details
	 */

	@Table(name = "emp_info1", keyspace = "demo1")
	public static class EmployeeDetails implements Serializable {
		
		private static final long serialVersionUID = 1L;
		private int emp_id;
		private String emp_first;
		private String emp_last;
		private String emp_address;
		private String emp_dept;

		public int getEmp_id() {
			return emp_id;
		}

		public void setEmp_id(int emp_id) {
			this.emp_id = emp_id;
		}

		public String getEmp_first() {
			return emp_first;
		}

		public void setEmp_first(String emp_first) {
			this.emp_first = emp_first;
		}

		public String getEmp_last() {
			return emp_last;
		}

		public void setEmp_last(String emp_last) {
			this.emp_last = emp_last;
		}

		public String getEmp_address() {
			return emp_address;
		}

		public void setEmp_address(String emp_address) {
			this.emp_address = emp_address;
		}

		public String getEmp_dept() {
			return emp_dept;
		}

		public void setEmp_dept(String emp_dept) {
			this.emp_dept = emp_dept;
		}

	}

}
