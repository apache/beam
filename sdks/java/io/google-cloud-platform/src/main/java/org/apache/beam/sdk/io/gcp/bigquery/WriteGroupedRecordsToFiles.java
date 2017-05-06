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
import java.util.UUID;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

/** Created by relax on 4/29/17. */
class WriteGroupedRecordsToFiles<DestinationT>
    extends DoFn<KV<ShardedKey<DestinationT>, Iterable<TableRow>>,
    WriteBundlesToFiles.Result<DestinationT>> {
  private final String tempFilePrefix;

  WriteGroupedRecordsToFiles(String tempFilePrefix) {
    this.tempFilePrefix = tempFilePrefix;
  }

  @ProcessElement
  public void processElement(ProcessContext c) throws Exception {
    TableRowWriter writer = createWriter();
    for (TableRow tableRow : c.element().getValue()) {
      if (writer.getByteSize() > Write.MAX_FILE_SIZE) {
        TableRowWriter.Result result = writer.close();
        c.output(new WriteBundlesToFiles.Result<>(
            result.resourceId.toString(), result.byteSize, c.element().getKey().getKey()));
        writer = createWriter();
      }
      writer.write(tableRow);
    }
    TableRowWriter.Result result = writer.close();
    c.output(new WriteBundlesToFiles.Result<>(
        result.resourceId.toString(), result.byteSize, c.element().getKey().getKey()));
  }

  TableRowWriter createWriter() throws Exception {
    TableRowWriter writer = new TableRowWriter(tempFilePrefix);
    writer.open(UUID.randomUUID().toString());
    return writer;
  }
}
