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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import com.google.api.services.bigquery.model.TableRow;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.Coder.Context;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.CountingOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Writes {@link TableRow} objects out to a file. Used when doing batch load jobs into BigQuery. */
class TableRowWriter implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(TableRowWriter.class);

  private static final Coder<TableRow> CODER = TableRowJsonCoder.of();
  private static final byte[] NEWLINE = "\n".getBytes(StandardCharsets.UTF_8);
  private ResourceId resourceId;
  private WritableByteChannel channel;
  private CountingOutputStream out;

  private boolean isClosed = false;

  static final class Result {
    final ResourceId resourceId;
    final long byteSize;

    public Result(ResourceId resourceId, long byteSize) {
      this.resourceId = resourceId;
      this.byteSize = byteSize;
    }
  }

  TableRowWriter(String basename) throws Exception {
    String uId = UUID.randomUUID().toString();
    resourceId = FileSystems.matchNewResource(basename + uId, false);
    LOG.info("Opening TableRowWriter to {}.", resourceId);
    channel = FileSystems.create(resourceId, MimeTypes.TEXT);
    out = new CountingOutputStream(Channels.newOutputStream(channel));
  }

  void write(TableRow value) throws Exception {
    CODER.encode(value, out, Context.OUTER);
    out.write(NEWLINE);
  }

  long getByteSize() {
    return out.getCount();
  }

  @Override
  public void close() throws IOException {
    checkState(!isClosed, "Already closed");
    isClosed = true;
    channel.close();
  }

  Result getResult() {
    checkState(isClosed, "Not yet closed");
    return new Result(resourceId, out.getCount());
  }
}
