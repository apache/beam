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
package org.apache.beam.sdk.io.iceberg.catalog.hiveutils;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;

/**
 * A class that interacts with {@link TestHiveMetastore}.
 *
 * <p>Trimmed down from <a
 * href="https://github.com/apache/iceberg/blob/main/hive-metastore/src/test/java/org/apache/iceberg/hive/HiveMetastoreExtension.java">Iceberg's
 * integration testing util</a>
 */
public class HiveMetastoreExtension {
  private HiveMetaStoreClient metastoreClient;
  private TestHiveMetastore metastore;

  public HiveMetastoreExtension(String warehousePath) throws MetaException {
    metastore = new TestHiveMetastore(warehousePath);
    HiveConf hiveConf = new HiveConf(TestHiveMetastore.class);

    metastore.start(hiveConf);
    metastoreClient = new HiveMetaStoreClient(hiveConf);
  }

  public void cleanup() throws Exception {
    if (metastoreClient != null) {
      metastoreClient.close();
    }

    if (metastore != null) {
      metastore.reset();
      metastore.stop();
    }

    metastoreClient = null;
    metastore = null;
  }

  public HiveMetaStoreClient metastoreClient() {
    return metastoreClient;
  }

  public HiveConf hiveConf() {
    return metastore.hiveConf();
  }

  public TestHiveMetastore metastore() {
    return metastore;
  }
}
