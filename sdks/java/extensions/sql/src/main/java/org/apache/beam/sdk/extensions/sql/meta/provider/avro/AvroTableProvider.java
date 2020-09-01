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
package org.apache.beam.sdk.extensions.sql.meta.provider.avro;

import com.google.auto.service.AutoService;
import org.apache.beam.sdk.extensions.avro.io.AvroIO;
import org.apache.beam.sdk.extensions.avro.io.AvroSchemaIOProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.SchemaIOTableProviderWrapper;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;
import org.apache.beam.sdk.schemas.io.SchemaIOProvider;

/**
 * {@link TableProvider} for {@link AvroIO} for consumption by Beam SQL.
 *
 * <p>Passes the {@link AvroSchemaIOProvider} to the generalized table provider wrapper, {@link
 * SchemaIOTableProviderWrapper}, for Avro specific behavior.
 *
 * <p>A sample of avro table is:
 *
 * <pre>{@code
 * CREATE EXTERNAL TABLE ORDERS(
 *   name VARCHAR,
 *   favorite_color VARCHAR,
 *   favorite_numbers ARRAY<INTEGER>
 * )
 * TYPE 'avro'
 * LOCATION '/tmp/persons.avro'
 * }</pre>
 */
@AutoService(TableProvider.class)
public class AvroTableProvider extends SchemaIOTableProviderWrapper {
  @Override
  public SchemaIOProvider getSchemaIOProvider() {
    return new AvroSchemaIOProvider();
  }

  // TODO[BEAM-10516]: remove this override after TableProvider problem is fixed
  @Override
  public String getTableType() {
    return "avro";
  }
}
