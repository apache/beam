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
package org.apache.beam.sdk.io.hcatalog;

import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Optional;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Adapter from HCatalog table schema to Beam {@link org.apache.beam.sdk.schemas.Schema}.
 *
 * <p>Loads a table schema from Hive Metastore specified in properties map, similar to {@link
 * HCatalogIO}.
 *
 * <p>One of the use cases is to perform the schema conversion without leaking any HCatalog types.
 */
@Experimental(Kind.SCHEMAS)
public class HCatalogBeamSchema {

  private @Nullable final IMetaStoreClient metastore;

  private HCatalogBeamSchema(IMetaStoreClient metastore) {
    this.metastore = metastore;
  }

  /**
   * Create the schema adapter.
   *
   * <p>Config map is used to construct the {@link HiveMetaStoreClient}.
   */
  public static HCatalogBeamSchema create(Map<String, String> config) {
    try {
      HiveConf hiveConf = new HiveConf();
      config.forEach(hiveConf::set);
      return new HCatalogBeamSchema(new HiveMetaStoreClient(hiveConf));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /** Checks if metastore client has the specified database. */
  public boolean hasDatabase(String dbName) {
    try {
      metastore.getDatabase(dbName);
      return true;
    } catch (NoSuchObjectException e) {
      return false;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /** Gets the table schema, or absent optional if the table doesn't exist in the database. */
  public Optional<Schema> getTableSchema(String db, String table) {
    try {
      org.apache.hadoop.hive.metastore.api.Table metastoreTable = metastore.getTable(db, table);
      List<FieldSchema> fields = Lists.newArrayList(metastoreTable.getSd().getCols());
      fields.addAll(metastoreTable.getPartitionKeys());
      Schema schema = SchemaUtils.toBeamSchema(fields);
      return Optional.of(schema);
    } catch (NoSuchObjectException e) {
      return Optional.absent();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
