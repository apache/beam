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
import com.google.common.io.CountingOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.Coder.Context;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.util.MimeTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Writes {@TableRow} objects out to a file. Used when doing batch load jobs into BigQuery. */
class TableRowWriter {
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryIO.class);

  private static final Coder<TableRow> CODER = TableRowJsonCoder.of();
  private static final byte[] NEWLINE = "\n".getBytes(StandardCharsets.UTF_8);
  private final String tempFilePrefix;
  private String id;
  private ResourceId resourceId;
  private WritableByteChannel channel;
  protected String mimeType = MimeTypes.TEXT;
  private CountingOutputStream out;

  public static final class Result {
    final ResourceId resourceId;
    final long byteSize;

    public Result(ResourceId resourceId, long byteSize) {
      this.resourceId = resourceId;
      this.byteSize = byteSize;
    }
  }

  TableRowWriter(String basename) {
    this.tempFilePrefix = basename;
  }

  public final void open(String uId) throws Exception {
    id = uId;
    resourceId = FileSystems.matchNewResource(tempFilePrefix + id, false);
    LOG.debug("Opening {}.", resourceId);
    channel = FileSystems.create(resourceId, mimeType);
    try {
      out = new CountingOutputStream(Channels.newOutputStream(channel));
      LOG.debug("Writing header to {}.", resourceId);
    } catch (Exception e) {
      try {
        LOG.error("Writing header to {} failed, closing channel.", resourceId);
        channel.close();
      } catch (IOException closeException) {
        LOG.error("Closing channel for {} failed", resourceId);
      }
      throw e;
    }
    LOG.debug("Starting write of bundle {} to {}.", this.id, resourceId);
  }

  public void write(TableRow value) throws Exception {
    CODER.encode(value, out, Context.OUTER);
    out.write(NEWLINE);
  }

  public final Result close() throws IOException {
    channel.close();
    return new Result(resourceId, out.getCount());
  }
}
