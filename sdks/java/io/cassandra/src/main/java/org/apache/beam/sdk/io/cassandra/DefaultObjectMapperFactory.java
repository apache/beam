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

import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.MappingManager;
import org.apache.beam.sdk.transforms.SerializableFunction;

/**
 * Factory implementation that CassandraIO uses to initialize the Default Object Mapper for mapping
 * POJOs to CRUD events in Cassandra.
 *
 * @see org.apache.beam.sdk.io.cassandra.DefaultObjectMapper
 */
class DefaultObjectMapperFactory<T> implements SerializableFunction<Session, Mapper> {

  private transient MappingManager mappingManager;
  Class<T> entity;

  DefaultObjectMapperFactory(Class<T> entity) {
    this.entity = entity;
  }

  @Override
  public Mapper apply(Session session) {
    if (mappingManager == null) {
      this.mappingManager = new MappingManager(session);
    }

    return new DefaultObjectMapper<T>(mappingManager.mapper(entity));
  }
}
