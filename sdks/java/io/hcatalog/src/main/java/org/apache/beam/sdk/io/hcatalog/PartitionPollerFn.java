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
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.hcatalog.HCatalogIO.Read;
import org.apache.beam.sdk.transforms.DoFn.Setup;
import org.apache.beam.sdk.transforms.DoFn.Teardown;
import org.apache.beam.sdk.transforms.Watch.Growth.PollFn;
import org.apache.beam.sdk.transforms.Watch.Growth.PollResult;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.joda.time.Instant;

/** Unbounded poller to listen for new partitions. */
public class PartitionPollerFn extends PollFn<Read, Read> {
  private transient IMetaStoreClient metaStoreClient;
  private Map<String, String> configProperties;

  public PartitionPollerFn(Map<String, String> configProperties) {
    this.configProperties = configProperties;
  }

  @Override
  public PollResult<Read> apply(Read element, Context c) throws Exception {
    Instant now = Instant.now();
    return PollResult.incomplete(now, getPartitions(element)).withWatermark(now);
  }

  private List<Read> getPartitions(Read read) throws Exception {
    return metaStoreClient.listPartitions(read.getDatabase(), read.getTable(), Short.MAX_VALUE)
        .stream()
        .map(partition -> read.withPartitionToRead(partition))
        .collect(Collectors.toList());
  }

  @Setup
  @SuppressWarnings("unused")
  public void setup() throws Exception {
    Configuration conf = new Configuration();
    for (Map.Entry<String, String> entry : configProperties.entrySet()) {
      conf.set(entry.getKey(), entry.getValue());
    }
    metaStoreClient = HCatalogUtils.createMetaStoreClient(conf);
  }

  @Teardown
  @SuppressWarnings("unused")
  public void teardown() {
    if (metaStoreClient != null) {
      metaStoreClient.close();
    }
  }
}
