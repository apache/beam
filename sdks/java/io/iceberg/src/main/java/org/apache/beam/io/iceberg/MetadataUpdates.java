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
package org.apache.beam.io.iceberg;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;

public class MetadataUpdates<IdentifierT>
    extends DoFn<KV<IdentifierT, Iterable<MetadataUpdate>>, KV<IdentifierT, Snapshot>> {

  final TableFactory<IdentifierT> tableFactory;

  public MetadataUpdates(TableFactory<IdentifierT> tableFactory) {
    this.tableFactory = tableFactory;
  }

  @ProcessElement
  public void processElement(
      ProcessContext c,
      @Element KV<IdentifierT, Iterable<MetadataUpdate>> element,
      BoundedWindow window) {
    Table table = tableFactory.getTable(element.getKey());
    AppendFiles update = table.newAppend();
    Iterable<MetadataUpdate> metadataUpdates = element.getValue();
    if (metadataUpdates != null) {
      for (MetadataUpdate metadata : metadataUpdates) {
        for (DataFile file : metadata.getDataFiles()) {
          update.appendFile(file);
        }
      }
      update.commit();
      c.outputWithTimestamp(
          KV.of(element.getKey(), table.currentSnapshot()), window.maxTimestamp());
    }
  }
}
