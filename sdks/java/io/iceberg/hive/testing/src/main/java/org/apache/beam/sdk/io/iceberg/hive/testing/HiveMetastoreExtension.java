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
package org.apache.beam.sdk.io.iceberg.hive.testing;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A class that interacts with {@link TestHiveMetastore}.
 *
 * <p>Trimmed down from <a
 * href="https://github.com/apache/iceberg/blob/main/hive-metastore/src/test/java/org/apache/iceberg/hive/HiveMetastoreExtension.java">Iceberg's
 * integration testing util</a>
 */
// @SuppressWarnings("all")
public class HiveMetastoreExtension {
  private @Nullable HiveMetaStoreClient metastoreClient;
  private @Nullable TestHiveMetastore metastore;

  public HiveMetastoreExtension(String warehousePath) throws Exception {
    HiveConf hiveConf = new HiveConf(TestHiveMetastore.class);
    metastore = new TestHiveMetastore(warehousePath, hiveConf);
    metastoreClient = new HiveMetaStoreClient(hiveConf);
  }

  public void cleanup() throws Exception {
    if (metastoreClient != null) {
      metastoreClient.close();
    }

    if (metastore != null) {
      metastore.reset();
      checkNotNull(metastore).stop();
    }

    metastoreClient = null;
    metastore = null;
  }

  public Configuration hiveConf() {
    return checkNotNull(metastore).hiveConf();
  }

  public String metastoreUri() {
    return checkNotNull(metastore).hiveConf().getVar(HiveConf.ConfVars.METASTOREURIS);
  }

  public void createDatabase(String name) throws Exception {
    String dbPath = checkNotNull(metastore).getDatabasePath(name);
    Database db = new Database(name, "description", dbPath, new HashMap<>());
    checkNotNull(metastoreClient).createDatabase(db);
  }
}
