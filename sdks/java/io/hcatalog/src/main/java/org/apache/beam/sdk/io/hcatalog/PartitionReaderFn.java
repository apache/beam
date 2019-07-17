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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.io.hcatalog.HCatalogIO.Read;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.transfer.DataTransferFactory;
import org.apache.hive.hcatalog.data.transfer.HCatReader;
import org.apache.hive.hcatalog.data.transfer.ReadEntity;
import org.apache.hive.hcatalog.data.transfer.ReaderContext;

/** Reads partition at a given index. */
class PartitionReaderFn extends DoFn<KV<Read, Integer>, HCatRecord> {
  private transient IMetaStoreClient metaStoreClient;
  private Map<String, String> configProperties;

  public PartitionReaderFn(Map<String, String> configProperties) {
    this.configProperties = configProperties;
  }

  private ReaderContext getReaderContext(Read readRequest, Integer partitionIndexToRead)
      throws Exception {
    final List<Partition> partitions =
        metaStoreClient.listPartitions(
            readRequest.getDatabase(), readRequest.getTable(), Short.MAX_VALUE);
    final Partition partition = partitions.get(partitionIndexToRead);
    checkArgument(
        partition != null, "Unable to find a partition to read at index " + partitionIndexToRead);

    final int desiredSplitCount = HCatalogUtils.getSplitCount(readRequest, partition);
    final List<String> values = partition.getValues();
    final List<String> partitionCols = readRequest.getPartitionCols();
    checkArgument(
        values.size() == partitionCols.size(),
        "Number of input partitions should be equal to the values of list partition values.");

    List<String> filter = new ArrayList<>();
    for (int i = 0; i < partitionCols.size(); i++) {
      filter.add(partitionCols.get(i) + "=" + "'" + values.get(i) + "'");
    }
    final String filterString = String.join(" and ", filter);

    ReadEntity entity =
        new ReadEntity.Builder()
            .withDatabase(readRequest.getDatabase())
            .withFilter(filterString)
            .withTable(readRequest.getTable())
            .build();
    // pass the 'desired' split count as an hint to the API
    Map<String, String> configProps = new HashMap<>(readRequest.getConfigProperties());
    configProps.put(
        HCatConstants.HCAT_DESIRED_PARTITION_NUM_SPLITS, String.valueOf(desiredSplitCount));
    return DataTransferFactory.getHCatReader(entity, configProps).prepareRead();
  }

  @ProcessElement
  public void processElement(ProcessContext c) throws Exception {
    final Read readRequest = c.element().getKey();
    final Integer partitionIndexToRead = c.element().getValue();
    ReaderContext readerContext = getReaderContext(readRequest, partitionIndexToRead);
    for (int i = 0; i < readerContext.numSplits(); i++) {
      HCatReader reader = DataTransferFactory.getHCatReader(readerContext, i);
      Iterator<HCatRecord> hcatIterator = reader.read();
      while (hcatIterator.hasNext()) {
        final HCatRecord record = hcatIterator.next();
        c.output(record);
      }
    }
  }

  @Setup
  public void setup() throws Exception {
    final Configuration conf = HCatalogUtils.createConfiguration(configProperties);
    metaStoreClient = HCatalogUtils.createMetaStoreClient(conf);
  }

  @Teardown
  public void teardown() {
    if (metaStoreClient != null) {
      metaStoreClient.close();
    }
  }
}
