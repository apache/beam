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
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.transforms.SerializableFunction;

/**
 * This interface allows you to implement a custom mapper to read and persist elements from/to
 * Cassandra.
 *
 * <p>To Implement a custom mapper you need to: 1) Create an implementation of {@link Mapper}. 2)
 * Create a {@link SerializableFunction} that instantiates the {@link Mapper} for a given Session,
 * for an example see {@link DefaultObjectMapperFactory}). 3) Pass this function to {@link
 * CassandraIO.Read#withMapperFactoryFn(SerializableFunction)} in the CassandraIO builder. <br>
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
@Experimental(Kind.SOURCE_SINK)
public interface Mapper<T> {

  /**
   * This method is called when reading data from Cassandra. It should map a ResultSet into the
   * corresponding Java objects.
   *
   * @param resultSet A resultset containing rows.
   * @return An iterator containing the objects that you want to provide to your downstream
   *     pipeline.
   */
  Iterator<T> map(ResultSet resultSet);

  /**
   * This method is called for each delete event. The input argument is the Object that should be
   * deleted in Cassandra. The return value should be a Future that completes when the delete action
   * is completed.
   *
   * @param entity Entity to be deleted.
   */
  Future<Void> deleteAsync(T entity);

  /**
   * This method is called for each save event. The input argument is the Object that should be
   * saved or updated in Cassandra. The return value should be a future that completes when the save
   * action is completed.
   *
   * @param entity Entity to be saved.
   */
  Future<Void> saveAsync(T entity);
}
