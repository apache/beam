package com.google.cloud.cassandra.dataflow.io;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import javax.swing.text.html.parser.Entity;

import org.joda.time.Instant;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.SerializableCoder;
import com.google.cloud.dataflow.sdk.io.BoundedSource;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;

public class CassandraReadIO {

	static class Source extends BoundedSource{
		CassandraReadConfiguration configuration;

		Source(CassandraReadConfiguration config) throws IOException{
			this.configuration = config;
		}

		@Override
		public Coder<Entity> getDefaultOutputCoder() {
			return (Coder<Entity>) SerializableCoder.of(configuration.get_entityName());
		}

		@Override
		public void validate(){}
		
		private static class Reader<T> extends BoundedReader{
			CassandraReadConfiguration configuration;
			static Cluster cluster;
			static Session session;
			ResultSet rs;
			Iterator itr;
			Object currentEntity;
			
			Reader(CassandraReadConfiguration configuration){
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

			/* Creates a cassandra connection, session and resultSet and 
			 * map the entity with resultSet and advances to the next result.
			 * Initializes the reader and advances the reader to the first record.
			 * @return true, if a record was read else false if there is no more input available.
			 */
			@Override
			public boolean start() throws IOException {				
				cluster = Cluster.builder().addContactPoints(configuration.getHost()).withPort(configuration.getPort()).build();
				session = cluster.connect();
				rs = session.execute(configuration.getQuery());
				final MappingManager _manager =new MappingManager(session);
				Mapper _mapper = null;			
				if(_mapper==null){
					_mapper = (Mapper) _manager.mapper(configuration.get_entityName());
				}						
				itr=_mapper.map(rs).iterator();						
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

		/* method splits a source into desired number of sources.
		 * (non-Javadoc)
		 * @see com.google.cloud.dataflow.sdk.io.BoundedSource#splitIntoBundles(long, com.google.cloud.dataflow.sdk.options.PipelineOptions)
		 * 
		 * Cassandra cluster token range is split into desired number of smaller ranges
		 * queries are built from start and end token ranges
		 * split sources are built by providing token range queries and other configurations
		 * rowkey is the partitionkey of the cassandara table
		 */
		@Override
		public List<BoundedSource> splitIntoBundles(long desiredSplits,
				PipelineOptions paramPipelineOptions) throws Exception {
			int max_exponent =127;		
			long split_exponent = max_exponent/desiredSplits;
			long token_split_range = (long) Math.pow(2, (split_exponent));
			long min_start_token = -(long) Math.pow(2, max_exponent);
			long start_token=0L,end_token = 0;
			List<BoundedSource> sourceList = new ArrayList<>();
			if(desiredSplits==1){
				sourceList.add(this);
				return sourceList;
			}
			String query = null;
			for(int split =1;split<=desiredSplits;split++){
				if(split==1){			//for first split will query token range starting from -2**(127-1)
					 start_token = min_start_token;	
					 end_token =  token_split_range;
					 query = queryBuilder(start_token, end_token);
					 configuration.setQuery(query);
					 sourceList.add(new 	CassandraReadIO.Source(
								new CassandraReadConfiguration(configuration.getHost(), configuration.getKeypace(),
										configuration.getPort(), configuration.getTable(),configuration.getQuery(),configuration.getRowKey(),configuration.get_entityName())));
				}else{
					token_split_range = (long) Math.pow(2, (split_exponent));
					 start_token = end_token;
					
					 end_token =  token_split_range;
					 query = queryBuilder(start_token, end_token);
					 configuration.setQuery(query);
					 sourceList.add(new 	CassandraReadIO.Source(
								new CassandraReadConfiguration(configuration.getHost(), configuration.getKeypace(),
										configuration.getPort(), configuration.getTable(),configuration.getQuery(),configuration.getRowKey(),configuration.get_entityName())));
				}		
				split_exponent = split_exponent*2;
			}
			return sourceList;
		}
		/*
		 * builds query using starttoken and endtoken
		 */
		public String queryBuilder(long startToken,long endToken){
			String query= QueryBuilder.select().from(configuration.getKeypace(),configuration.getTable()).where(QueryBuilder.gte("token("+configuration.getRowKey()+")",startToken)).and(QueryBuilder.lt("token("+configuration.getRowKey()+")",endToken)).toString();
			return query;
		}
	
		
		@Override
		public long getEstimatedSizeBytes(PipelineOptions paramPipelineOptions)
				throws Exception {
			return 0;
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
	
	  public static BoundedSource read(
			  CassandraReadConfiguration config) throws IOException {
		    return new Source(config);
		  }

		
}	
