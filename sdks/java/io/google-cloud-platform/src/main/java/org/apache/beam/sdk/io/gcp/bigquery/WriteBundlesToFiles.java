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

import java.util.Map;
import java.util.UUID;

import com.google.common.collect.Maps;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writes each bundle of {@link TableRow} elements out to a separate file using
 * {@link TableRowWriter}.
 */
class WriteBundlesToFiles extends DoFn<KV<TableDestination, TableRow>, WriteBundlesToFiles.Result> {
  private static final Logger LOG = LoggerFactory.getLogger(WriteBundlesToFiles.class);

  // Map from tablespec to a writer for that table.
  private transient Map<TableDestination, TableRowWriter> writers;
  private final String tempFilePrefix;

  public static class Result {
    public String filename;
    public Long fileByteSize;
    public TableDestination tableDestination;

    public Result(String filename, Long fileByteSize, TableDestination tableDestination) {
      this.filename = filename;
      this.fileByteSize = fileByteSize;
      this.tableDestination = tableDestination;
    }
  }
  WriteBundlesToFiles(String tempFilePrefix) {
    this.tempFilePrefix = tempFilePrefix;
    this.writers = Maps.newHashMap();
  }

  @ProcessElement
  public void processElement(ProcessContext c) throws Exception {
    // ??? can we assume Java8?
    TableRowWriter writer = writers.getOrDefault(c.element().getKey(), null);
    if (writer == null) {
      writer = new TableRowWriter(tempFilePrefix);
      writer.open(UUID.randomUUID().toString());
      writers.put(c.element().getKey(), writer);
      LOG.debug("Done opening writer {}", writer);
    }
    try {
      writer.write(c.element().getValue());
    } catch (Exception e) {
      // Discard write result and close the write.
      try {
        writer.close();
        // The writer does not need to be reset, as this DoFn cannot be reused.
      } catch (Exception closeException) {
        // Do not mask the exception that caused the write to fail.
        e.addSuppressed(closeException);
      }
      throw e;
    }
  }

  @FinishBundle
  public void finishBundle(Context c) throws Exception {
    for (Map.Entry<TableDestination, TableRowWriter> entry : writers.entrySet()) {
      TableRowWriter.Result result = entry.getValue().close();
      c.output(new Result(result.filename, result.byteSize, entry.getKey()));
    }
    writers.clear();
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);

    builder
        .addIfNotNull(DisplayData.item("tempFilePrefix", tempFilePrefix)
            .withLabel("Temporary File Prefix"));
  }
}
