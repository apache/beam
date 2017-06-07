package com.google.cloud.cassandra.dataflow.io;

import static com.google.common.base.Strings.isNullOrEmpty;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.regex.Pattern;

import javax.swing.text.html.parser.Entity;

import org.apache.cassandra.tools.NodeProbe;
import org.joda.time.Instant;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.SerializableCoder;
import com.google.cloud.dataflow.sdk.io.BoundedSource;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineWorkerPoolOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.common.base.Preconditions;

public class CassandraReadIO {
	//A Bounded source to read from Cassandra
	static class Source extends BoundedSource {
		private CassandraReadConfiguration configuration;
		private static final String SSTABLE_DISK_SPACE = "LiveDiskSpaceUsed";
		private static final String MEMTABLE_SPACE = "MemtableLiveDataSize";
		private static final int DEFAULT_JMX_PORT = 7199;
		private static String DEFAULT_ROW_COUNT_QUERY = null;
		private static final String FROM_KEYWORD = "from";
		private static final String WHERE_CLAUSE = "where";

		Source(CassandraReadConfiguration config) throws IOException {
			this.configuration = config;
			validate();
		}

		@Override
		public Coder<Entity> getDefaultOutputCoder() {
			return (Coder<Entity>) SerializableCoder.of(configuration
					.get_entityName());
		}

		@Override
		public void validate() {
			Preconditions.checkNotNull(configuration.getHost(), "host");
			Preconditions.checkNotNull(configuration.getPort(), "port");
			Preconditions.checkNotNull(configuration.getKeypace(), "keypace");
			Preconditions.checkNotNull(configuration.getTable(), "table");
			Preconditions.checkNotNull(configuration.getRowKey(),"rowKey/partionKey");
		}

		private static class Reader<T> extends BoundedReader {
			CassandraReadConfiguration configuration;
			static Cluster cluster;
			static Session session;
			ResultSet rs;
			Iterator itr;
			Object currentEntity;

			Reader(CassandraReadConfiguration configuration) {
				this.configuration = configuration;
			}

			/*
			 * advances the reader to the next record
			 */
			@Override
			public boolean advance() throws IOException {
				if (itr.hasNext()) {
					currentEntity = itr.next();
					return true;
				} else {
					return false;
				}
			}

			/*
			 * Creates a cassandra connection, session and resultSet and map the
			 * entity with resultSet and advances to the next result.
			 * Initializes the reader and advances the reader to the first
			 * record.
			 * 
			 * @return true, if a record was read else false if there is no more
			 * input available.
			 */
			@Override
			public boolean start() throws IOException {
				cluster = Cluster.builder()
						.addContactPoints(configuration.getHost())
						.withPort(configuration.getPort()).build();
				session = cluster.connect();
				rs = session.execute(configuration.getQuery());
				final MappingManager _manager = new MappingManager(session);
				Mapper _mapper = null;
				if (_mapper == null) {
					_mapper = (Mapper) _manager.mapper(configuration
							.get_entityName());
				}
				itr = _mapper.map(rs).iterator();
				return advance();
			}

			@Override
			public void close() throws IOException {
				session.close();
				cluster.close();
			}

			@Override
			public Object getCurrent() throws NoSuchElementException {
				return currentEntity;
			}

			@Override
			public Instant getCurrentTimestamp() throws NoSuchElementException {
				return Instant.now();
			}

			@Override
			public BoundedSource getCurrentSource() {
				return null;
			}

		}

		/*
		 * method splits a source into desired number of sources. (non-Javadoc)
		 * 
		 * @see
		 * com.google.cloud.dataflow.sdk.io.BoundedSource#splitIntoBundles(long,
		 * com.google.cloud.dataflow.sdk.options.PipelineOptions)
		 * 
		 * Cassandra cluster token range is split into desired number of smaller
		 * ranges. queries are built from start and end token ranges split
		 * sources are built by providing token range queries and other
		 * configurations rowkey is the partitionkey of the cassandara table
		 */
		@Override
		public List<BoundedSource> splitIntoBundles(long desiredBundleSizeBytes,
				PipelineOptions options) throws Exception {
			int exponent = 63;
			long numSplits=10;
			long startToken,endToken = 0l;
			List<BoundedSource> sourceList = new ArrayList<BoundedSource>();
			try{
				if(desiredBundleSizeBytes >0){
					numSplits = getEstimatedSizeBytes(options)/desiredBundleSizeBytes;
				}			
			} catch (Exception e) {
				// set numSplits equal to Dataflow's numWorkers
				DataflowPipelineWorkerPoolOptions poolOptions =
						options.as(DataflowPipelineWorkerPoolOptions.class);
				if (poolOptions.getNumWorkers() > 0) {
					numSplits = poolOptions.getNumWorkers();
				} else {
					//Default to 10 , equals to Cassandra maxConnectionPerHost
					numSplits = 10;
				}
			}

			if(numSplits <=0){
				numSplits =1;			
			}
			// If the desiredBundleSize or number of workers results in 1 split, simply return current source		    
			if (numSplits == 1) {
				sourceList.add(this);
				return sourceList;
			}

			BigInteger startRange = new BigInteger(String.valueOf(-(long) Math.pow(2, exponent)));
			BigInteger endRange= new BigInteger(String.valueOf((long) Math.pow(2, exponent)));
			endToken = startRange.longValue();
			BigInteger incrmentValue = (endRange.subtract(startRange)).divide(new BigInteger(String.valueOf(numSplits)));
			String splitQuery = null;
			for (int splitCount=1; splitCount <=numSplits; splitCount++) {			
				startToken = endToken;
				endToken = startToken+incrmentValue.longValue();						
				if(splitCount == numSplits){					 
					endToken = (long) Math.pow(2, exponent);  //set end token to max token value;
				}
				splitQuery = queryBuilder(startToken, endToken);
				configuration.setQuery(splitQuery);
				sourceList.add(new CassandraReadIO.Source(configuration));

			}
			return sourceList;
		}

		/*
		 * builds query using startToken and endToken
		 */
		public String queryBuilder(long startToken, long endToken) {
			String query = QueryBuilder
					.select()
					.from(configuration.getKeypace(), configuration.getTable())
					.where(QueryBuilder.gte(
							"token(" + configuration.getRowKey() + ")",
							startToken))
							.and(QueryBuilder.lt("token(" + configuration.getRowKey()
									+ ")", endToken)).toString();
			return query;
		}

		@Override
		// Cassandra provides no direct way to get the estimate of a query
		// result size in bytes
		// As a rough approximation,
		//1. We fetch the byte count for entire column family
		//2. we get the row count for column family and row count for supplied query
		//3. we calculate the query resultSet byte size
		public long getEstimatedSizeBytes(PipelineOptions options)
				throws Exception {
			NodeProbe probe = null;
			Long estimatedByteSize = 0l;
			Long totalCfByteSize = 0l;
			DEFAULT_ROW_COUNT_QUERY = "select count(*) from" + "\t"
					+ configuration.getKeypace() + "."
					+ configuration.getTable();
			try {
				if (configuration.getJmxPort() == 0) {
					probe = new NodeProbe(configuration.getHost()[0],
							DEFAULT_JMX_PORT);
				} else {
					probe = new NodeProbe(configuration.getHost()[0],
							configuration.getJmxPort());
				}
				totalCfByteSize = (Long) probe.getColumnFamilyMetric(
						configuration.getKeypace(), configuration.getTable(),
						SSTABLE_DISK_SPACE) + (Long) probe.getColumnFamilyMetric(
								configuration.getKeypace(), configuration.getTable(),
								MEMTABLE_SPACE) ;

				// start
				if (configuration.getQuery() != null
						&& !configuration.getQuery().isEmpty()
						&& Pattern
						.compile(
								WHERE_CLAUSE,
								Pattern.CASE_INSENSITIVE
								+ Pattern.LITERAL)
								.matcher(configuration.getQuery()).find()) {
					estimatedByteSize = getQueryResultBytesSize(totalCfByteSize);
					if (estimatedByteSize == 0) {
						estimatedByteSize = 1l;
					}
				} else {
					estimatedByteSize = totalCfByteSize;
				}

			} catch (IOException e) {
				throw e;
			} finally {
				if (probe != null)
					probe.close();
			}
			return estimatedByteSize;
		}

		// Get the query to fetch row count
		// for entire column family or for supplied query
		private String getQueryForRowCount() {
			String rowCountQuery;
			if ((configuration.getQuery() != null && !configuration.getQuery()
					.isEmpty())
					&& Pattern
					.compile(WHERE_CLAUSE,
							Pattern.CASE_INSENSITIVE + Pattern.LITERAL)
							.matcher(configuration.getQuery()).find()) {
				rowCountQuery = "select count(*)\t"
						+ configuration.getQuery().substring(
								configuration.getQuery().indexOf(FROM_KEYWORD));
			} else {
				rowCountQuery = DEFAULT_ROW_COUNT_QUERY;
			}
			return rowCountQuery;
		}

		//Get the resultSet size in bytes for the supplied query
		private long getQueryResultBytesSize(long cfByteSize) throws Exception {
			long CoumanFamilyByteSize = cfByteSize;
			long totalQueryByteSize = 0l;
			long totalCfRowCount = getRowCount(DEFAULT_ROW_COUNT_QUERY); 
			long queryRowCount = getRowCount(getQueryForRowCount()); 
			int percentage = (int) ((queryRowCount * 100) / totalCfRowCount);
			totalQueryByteSize = CoumanFamilyByteSize * percentage / 100;
			return totalQueryByteSize;
		}
		//Get row count for the supplied query
		private long getRowCount(String query) {
			Cluster cluster = null;
			Session session = null;
			long rowCount = 0l;
			try {
				cluster = Cluster.builder()
						.addContactPoints(configuration.getHost())
						.withPort(configuration.getPort()).build();
				session = cluster.connect();
				ResultSet queryResult = session.execute(query);
				Row row = queryResult.one();
				rowCount = row.getLong("count");
			} catch (Exception ex) {
				throw ex;
			} finally {
				if (session != null) {
					session.close();
				}
			}
			return rowCount;
		}

		@Override
		public boolean producesSortedKeys(PipelineOptions paramPipelineOptions)
				throws Exception {
			return false;
		}

		@Override
		public BoundedReader createReader(PipelineOptions paramPipelineOptions)
				throws IOException {
			return new Reader(configuration);
		}
	}

	public static BoundedSource read(CassandraReadConfiguration config)
			throws IOException {
		return new Source(config);
	}

}
