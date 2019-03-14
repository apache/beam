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

import com.datastax.driver.core.ResultSet;
import java.util.Iterator;
import java.util.concurrent.Future;

/**
 * The default behavior of {@link CassandraIO} is to use {@link DefaultObjectMapper} which in turn
 * uses <a
 * href="https://docs.datastax.com/en/developer/java-driver/3.1/manual/object_mapper">Cassandra
 * Object Mapper</a> to map POJOs to CRUD events in Cassandra. This interface allows you to
 * implement a custom mapper.
 *
 * <p>To Implement a custom mapper you need to: <br>
 * 1) Create a concrete implementation of {@link Mapper} <br>
 * 2) Create a factory that implements SerializableFunction as in (see {@link
 * DefaultObjectMapperFactory}) <br>
 * 3) Pass the mapper-factory in 2) through withMapperFactoryFn() in the CassandraIO builder.
 * Example:
 *
 * <pre>{@code
 * SerializableFunction<Session, Mapper> factory = new MyCustomFactory();
 * pipeline
 *    .apply(...)
 *    .apply(CassandraIO.<>read()
 *        .withMapperFactoryFn(factory));
 * }</pre>
 */
public interface Mapper<T> {

  /**
   * This method is called when reading data from Cassandra. The resultset will contain a subset of
   * rows from the table specified in {@link CassandraIO}. If a where-clause is specified through
   * withWhere() in {@link CassandraIO} the rows in the resultset would also be filtered given by
   * the provided where statement.
   *
   * @param resultSet A resultset containing rows given by withTable() and withWhere() specified in
   *     {@link CassandraIO}.
   * @return Return an iterator containing the objects that you want to provide to your downstream
   *     Beam pipeline.
   */
  Iterator<T> map(ResultSet resultSet);

  /**
   * This method gets called for each delete event. The input argument is the Object that should be
   * deleted in Cassandra. The return value should be a future that completes when the delete action
   * is completed.
   *
   * @param entity Entity to be deleted.
   */
  Future<Void> deleteAsync(T entity);

  /**
   * This method gets called for each save event. The input argument is the Object that should be
   * saved or updated in Cassandra. The return value should be a future that completes when the save
   * action is completed.
   *
   * @param entity Entity to be saved.
   */
  Future<Void> saveAsync(T entity);
}
