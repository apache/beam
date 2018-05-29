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

package org.apache.beam.sdk.io.gcp.bigquery;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.ShardedKey;

/**
 * Receives elements grouped by their (sharded) destination, and writes them out to a file.
 * Since all the elements in the {@link Iterable} are destined to the same table, they are all
 * written to the same file. Ensures that only one {@link TableRowWriter} is active per bundle.
 */
class WriteGroupedRecordsToFiles<DestinationT>
    extends DoFn<KV<ShardedKey<DestinationT>, Iterable<TableRow>>,
    WriteBundlesToFiles.Result<DestinationT>> {

  private final PCollectionView<String> tempFilePrefix;
  private final long maxFileSize;

  WriteGroupedRecordsToFiles(PCollectionView<String> tempFilePrefix, long maxFileSize) {
    this.tempFilePrefix = tempFilePrefix;
    this.maxFileSize = maxFileSize;
  }

  @ProcessElement
  public void processElement(ProcessContext c) throws Exception {
    String tempFilePrefix = c.sideInput(this.tempFilePrefix);
    TableRowWriter writer = new TableRowWriter(tempFilePrefix);
    try {
      for (TableRow tableRow : c.element().getValue()) {
        if (writer.getByteSize() > maxFileSize) {
          writer.close();
          writer = new TableRowWriter(tempFilePrefix);
          TableRowWriter.Result result = writer.getResult();
          c.output(new WriteBundlesToFiles.Result<>(
              result.resourceId.toString(), result.byteSize, c.element().getKey().getKey()));
        }
        writer.write(tableRow);
      }
    } finally {
      writer.close();
    }

    TableRowWriter.Result result = writer.getResult();
    c.output(
        new WriteBundlesToFiles.Result<>(
            result.resourceId.toString(), result.byteSize, c.element().getKey().getKey()));
  }

}
