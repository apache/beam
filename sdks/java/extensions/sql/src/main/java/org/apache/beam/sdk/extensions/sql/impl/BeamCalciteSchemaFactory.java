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
package org.apache.beam.sdk.extensions.sql.impl;

import java.util.Map;
import java.util.ServiceLoader;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;
import org.apache.beam.sdk.extensions.sql.meta.store.InMemoryMetaStore;
import org.apache.beam.sdk.extensions.sql.meta.store.MetaStore;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

/** Factory that creates a {@link BeamCalciteSchema}. */
public class BeamCalciteSchemaFactory implements SchemaFactory {
  public static final BeamCalciteSchemaFactory INSTANCE = new BeamCalciteSchemaFactory();

  private BeamCalciteSchemaFactory() {}

  @Override
  public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
    MetaStore metaStore = new InMemoryMetaStore();
    for (TableProvider provider :
        ServiceLoader.load(TableProvider.class, getClass().getClassLoader())) {
      metaStore.registerProvider(provider);
    }
    return new BeamCalciteSchema(metaStore);
  }
}
