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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import com.google.api.services.bigquery.model.TableRow;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.UUID;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.CountingOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Writes {@link TableRow} objects out to a file. Used when doing batch load jobs into BigQuery. */
abstract class BigQueryRowWriter<T> implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryRowWriter.class);

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

  BigQueryRowWriter(String basename, String mimeType) throws Exception {
    String uId = UUID.randomUUID().toString();
    resourceId = FileSystems.matchNewResource(basename + uId, false);
    LOG.info("Opening {} to {}.", this.getClass().getSimpleName(), resourceId);
    channel = FileSystems.create(resourceId, mimeType);
    out = new CountingOutputStream(Channels.newOutputStream(channel));
  }

  protected OutputStream getOutputStream() {
    return out;
  }

  abstract void write(T value) throws IOException, BigQueryRowSerializationException;

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

  public static class BigQueryRowSerializationException extends Exception {

    public BigQueryRowSerializationException(Exception e) {
      super(e);
    }
  }
}
