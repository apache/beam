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
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.sdk.io.hcatalog.HCatalogIO.Read;
import org.apache.beam.sdk.transforms.Watch.Growth.PollFn;
import org.apache.beam.sdk.transforms.Watch.Growth.PollResult;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.joda.time.Instant;

/** Return the list of current partitions present. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
class PartitionPollerFn extends PollFn<Read, Integer> {
  private transient IMetaStoreClient metaStoreClient;

  @Override
  public PollResult<Integer> apply(Read element, Context c) throws Exception {
    final Configuration conf = HCatalogUtils.createConfiguration(element.getConfigProperties());
    metaStoreClient = HCatalogUtils.createMetaStoreClient(conf);
    final Instant now = Instant.now();
    final PollResult<Integer> pollResult =
        PollResult.incomplete(now, getPartitionIndices(element)).withWatermark(now);
    if (metaStoreClient != null) {
      metaStoreClient.close();
    }
    return pollResult;
  }

  private List<Integer> getPartitionIndices(Read read) throws Exception {
    return IntStream.range(
            0,
            metaStoreClient
                .listPartitions(read.getDatabase(), read.getTable(), Short.MAX_VALUE)
                .size())
        .boxed()
        .collect(Collectors.toList());
  }
}
