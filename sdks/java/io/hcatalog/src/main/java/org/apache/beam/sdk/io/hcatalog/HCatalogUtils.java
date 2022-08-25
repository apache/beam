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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.io.hcatalog.HCatalogIO.Read;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.stats.StatsUtils;
import org.apache.hive.hcatalog.common.HCatUtil;

/** Utility classes to enable meta store conf/client creation. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class HCatalogUtils {

  private static final int DESIRED_BUNDLE_SIZE_BYTES = 134217728; // 128 MB

  static IMetaStoreClient createMetaStoreClient(Configuration conf)
      throws IOException, MetaException {
    final HiveConf hiveConf = HCatUtil.getHiveConf(conf);
    return HCatUtil.getHiveMetastoreClient(hiveConf);
  }

  static HiveConf createHiveConf(Read readRequest) throws IOException {
    Configuration conf = createConfiguration(readRequest.getConfigProperties());
    return HCatUtil.getHiveConf(conf);
  }

  static int getSplitCount(Read readRequest, Partition partitionToRead) throws Exception {
    int desiredSplitCount = 1;
    long estimatedSizeBytes = getFileSizeForPartition(readRequest, partitionToRead);
    if (estimatedSizeBytes > 0) {
      desiredSplitCount = (int) Math.ceil((double) estimatedSizeBytes / DESIRED_BUNDLE_SIZE_BYTES);
    }
    return desiredSplitCount;
  }

  static Configuration createConfiguration(Map<String, String> configProperties) {
    Configuration conf = new Configuration();
    for (Map.Entry<String, String> entry : configProperties.entrySet()) {
      conf.set(entry.getKey(), entry.getValue());
    }
    return conf;
  }

  private static long getFileSizeForPartition(Read readRequest, Partition partitionToRead)
      throws Exception {
    IMetaStoreClient client = null;
    try {
      HiveConf hiveConf = HCatalogUtils.createHiveConf(readRequest);
      client = HCatalogUtils.createMetaStoreClient(hiveConf);
      List<org.apache.hadoop.hive.ql.metadata.Partition> p = new ArrayList<>();
      Table table = HCatUtil.getTable(client, readRequest.getDatabase(), readRequest.getTable());
      final org.apache.hadoop.hive.ql.metadata.Partition partition =
          new org.apache.hadoop.hive.ql.metadata.Partition(table, partitionToRead);
      p.add(partition);
      final List<Long> fileSizeForPartitions = StatsUtils.getFileSizeForPartitions(hiveConf, p);
      return fileSizeForPartitions.get(0);
    } finally {
      // IMetaStoreClient is not AutoCloseable, closing it manually
      if (client != null) {
        client.close();
      }
    }
  }
}
