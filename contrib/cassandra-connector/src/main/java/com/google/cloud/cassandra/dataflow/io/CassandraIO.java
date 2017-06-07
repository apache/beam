package com.google.cloud.cassandra.dataflow.io;

/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.DriverInternalError;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.SerializableCoder;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.View;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.PDone;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Uninterruptibles;

public class CassandraIO {

	/**
	 * Write sink for Cassandra.
	 */
	public static class Write {

		/** create CassandraIO.Write.Unbound with hosts */
		public static Unbound hosts(String... hosts) {
			return new Unbound().hosts(hosts);
		}

		/** create CassandraIO.Write.Unbound with keyspace */
		public static Unbound keyspace(String keyspace) {
			return new Unbound().keyspace(keyspace);
		}

		/** create CassandraIO.Write.Unbound with port */
		public static Unbound port(int port) {
			return new Unbound().port(port);
		}

		/** Set cassandra hosts,keyspace and port */
		public static class Unbound {

			private final String[] _hosts;
			private final String _keyspace;
			private final int _port;

			Unbound() {
				_hosts = null;
				_keyspace = null;
				_port = 9042;
			}

			Unbound(String[] hosts, String keyspace, int port) {
				_hosts = hosts;
				_keyspace = keyspace;
				_port = port;
			}

			/** create CassandraIO.Write.Bound */
			public <T> Bound<T> bind() {
				return new Bound<T>(_hosts, _keyspace, _port);
			}

			/** create CassandraIO.Write.Unbound with hosts */

			public Unbound hosts(String... hosts) {
				return new Unbound(hosts, _keyspace, _port);
			}

			/** create CassandraIO.Write.Unbound with keyspace */
			public Unbound keyspace(String keyspace) {
				return new Unbound(_hosts, keyspace, _port);
			}

			/** create CassandraIO.Write.Unbound with port */
			public Unbound port(int port) {
				return new Unbound(_hosts, _keyspace, port);
			}
		}

		/**
		 * Set Cassandra hosts,keyspace and port Apply transformation
		 */
		public static class Bound<T> extends PTransform<PCollection<T>, PDone> {

			private static final long serialVersionUID = 0;

			private final String[] _hosts;
			private final String _keyspace;
			private final int _port;

			Bound(String[] hosts, String keyspace, int port) {
				_hosts = hosts;
				_keyspace = keyspace;
				_port = port;
			}

			public String[] getHosts() {
				return _hosts;
			}

			public String getKeyspace() {
				return _keyspace;
			}

			public int getPort() {
				return _port;
			}

			/**
			 * Create CassandraWriteOperation Create Coder write entity
			 */
			@SuppressWarnings("unchecked")
			public PDone apply(PCollection<T> input) {
				Pipeline p = input.getPipeline();
				CassandraWriteOperation<T> op = new CassandraWriteOperation<T>(
						this);

				Coder<CassandraWriteOperation<T>> coder = (Coder<CassandraWriteOperation<T>>) SerializableCoder
						.of(op.getClass());

				PCollection<CassandraWriteOperation<T>> opSingleton = p
						.apply(Create.<CassandraWriteOperation<T>> of(op)
								.withCoder(coder));
				final PCollectionView<CassandraWriteOperation<T>> opSingletonView = opSingleton
						.apply(View.<CassandraWriteOperation<T>> asSingleton());

				PCollection<Void> results = input.apply(ParDo.of(
						new DoFn<T, Void>() {

							private static final long serialVersionUID = 0;
							private CassandraWriter<T> writer = null;

							@Override
							public void processElement(ProcessContext c)
									throws Exception {
								if (writer == null) {
									CassandraWriteOperation<T> op = c
											.sideInput(opSingletonView);
									writer = op.createWriter();
								}

								try {
									writer.write(c.element());
								} catch (Exception e) {
									try {
										writer.flush();
									} catch (Exception ec) {

									}
									throw e;
								}
							}

							@Override
							public void finishBundle(Context c)
									throws Exception {
								if (writer != null)
									writer.flush();
							}
						}).withSideInputs(opSingletonView));

				PCollectionView<Iterable<Void>> voidView = results.apply(View
						.<Void> asIterable());

				opSingleton.apply(ParDo.of(
						new DoFn<CassandraWriteOperation<T>, Void>() {
							private static final long serialVersionUID = 0;

							@Override
							public void processElement(ProcessContext c) {
								CassandraWriteOperation<T> op = c.element();
								op.finalize();
							}

						}).withSideInputs(voidView));
				return PDone.in(p);
			}
		}

		/**
		 * Create Cluster object Create Session object Create entity mapper
		 * **/
		static class CassandraWriteOperation<T> implements java.io.Serializable {

			private static final long serialVersionUID = 0;
			private final String[] _hosts;
			private final int _port;
			private final String _keyspace;

			private transient Cluster _cluster;
			private transient Session _session;
			private transient MappingManager _manager;

			private synchronized Cluster getCluster() {
				if (_cluster == null) {
					_cluster = Cluster.builder().addContactPoints(_hosts)
							.withPort(_port).withoutMetrics()
							.withoutJMXReporting().build();
				}

				return _cluster;
			}

			private synchronized Session getSession() {

				if (_session == null) {
					Cluster cluster = getCluster();
					_session = cluster.connect(_keyspace);
				}
				return _session;
			}

			private synchronized MappingManager getManager() {
				if (_manager == null) {
					Session session = getSession();
					_manager = new MappingManager(_session);
				}
				return _manager;
			}

			public CassandraWriteOperation(Bound<T> bound) {

				_hosts = bound.getHosts();
				_port = bound.getPort();
				_keyspace = bound.getKeyspace();
			}

			public CassandraWriter<T> createWriter() {
				return new CassandraWriter<T>(this, getManager());
			}

			public void finalize() {
				getSession().close();
				getCluster().close();
			}
		}

		/** Create cassandra writer **/
		private static class CassandraWriter<T> {
			private static int BATCH_SIZE = 20000;
			private final CassandraWriteOperation _op;
			private final MappingManager _manager;
			private final List<ListenableFuture<Void>> _results = new ArrayList<ListenableFuture<Void>>();
			private Mapper<T> _mapper;

			public CassandraWriter(CassandraWriteOperation op,
					MappingManager manager) {
				_op = op;
				_manager = manager;
			}

			public void flush() {
				for (ListenableFuture<Void> result : _results) {
					try {
						Uninterruptibles.getUninterruptibly(result);
					} catch (ExecutionException e) {
						if (e.getCause() instanceof DriverException)
							throw ((DriverException) e.getCause()).copy();
						else
							throw new DriverInternalError(
									"Unexpected exception thrown", e.getCause());
					}
				}
				_results.clear();
			}

			/** Get CassandraWriteOperation **/
			public CassandraWriteOperation getWriteOperation() {
				return _op;
			}

			/** Write entity to the database table **/
			@SuppressWarnings("unchecked")
			public void write(T entity) {
				if (_mapper == null)
					_mapper = (Mapper<T>) _manager.mapper(entity.getClass());
				if (_results.size() >= BATCH_SIZE)
					flush();
				_results.add(_mapper.saveAsync((T) entity));
			}
		}
	}
}
