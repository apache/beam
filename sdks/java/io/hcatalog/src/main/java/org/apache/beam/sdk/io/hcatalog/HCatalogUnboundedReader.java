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

import java.util.Map;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hive.hcatalog.data.HCatRecord;

/**
 * Unbounded IO to read data using HCatalog.
 *
 * <h3>Reading using HCatalog</h3>
 *
 * <p>HCatalog source supports reading of HCatRecord from a HCatalog managed source, for eg. Hive.
 *
 * <p>To configure a HCatalog source, you must specify a metastore URI and a table name. Other
 * optional parameters are database &amp; filter For instance:
 *
 * <pre>{@code
 * Map<String, String> configProperties = new HashMap<>();
 * configProperties.put("hive.metastore.uris","thrift://metastore-host:port");
 *
 * Unbounded IO uses splittable pardo(s) to do the reads. There are two components that get used
 * 1) Unbounded Partition Poller which keeps polling for newer partitions based on a polling
 * interval specified by the user
 * 2) Record Reader used to read data
 *
 * To use the unbounded reader:
 *
 * 1) Create the reader
 * final HCatalogUnboundedReader reader = HCatalogUnboundedReader.of();
 * reader.setConfig(configProperties)
 *
 * 2) Create the input request object to specify database and table to read from
 *     List<PartitionPoller.ReadRequest> expected = new ArrayList<>();
 *     final HCatalogIO.Read readRequest = HCatalogIO.read()
 *       .withConfigProperties(configProperties)
 *       .withDatabase("default") //optional, assumes default if none specified
 *       .withTable("employee")
 *       .withFilter(filterString) //optional, may be specified if the table is partitioned
 *
 *     //Provide the list of partition cols
 *     final ImmutableList<String> partitions = ImmutableList.of("load_date", "product_type");
 *
 *     final PartitionPoller.ReadRequest readRequest = new PartitionPoller.ReadRequest(
 *         Duration.millis(30000),    //How often should we poll for new partitions?
 *         readRequest,
 *         new PartitionComparator(), //How to sort partitions?
 *         partitions,
 *         "load_date");              //Partition column that has a watermark. We use the value
 *                                    //from this col to advance the watermark. Values should follow
 *                                    //ISO Date Time Format.
 *     expected.add(readRequest);
 *
 * pipeline
 *   .apply(Create.of(readRequest)
 *   .apply(reader)
 * }</pre>
 */
public class HCatalogUnboundedReader
    extends PTransform<PCollection<PartitionPoller.ReadRequest>, PCollection<HCatRecord>> {

  private Map<String, String> configMap;
  private String database;
  private String table;

  public void setShouldTreatSourceAsBounded(Boolean shouldTreatSourceAsBounded) {
    this.shouldTreatSourceAsBounded = shouldTreatSourceAsBounded;
  }

  private Boolean shouldTreatSourceAsBounded;

  public static HCatalogUnboundedReader of() {
    return new HCatalogUnboundedReader();
  }

  public void setConfig(Map<String, String> config) {
    this.configMap = config;
  }

  public void setDatabase(String db) {
    this.database = db;
  }

  public void setTable(String table) {
    this.table = table;
  }

  @Override
  public PCollection<HCatRecord> expand(PCollection<PartitionPoller.ReadRequest> input) {
    return input
        .apply(
            ParDo.of(new PartitionPoller(configMap, database, table, shouldTreatSourceAsBounded)))
        .apply(ParDo.of(new HCatRecordReader()));
  }
}
