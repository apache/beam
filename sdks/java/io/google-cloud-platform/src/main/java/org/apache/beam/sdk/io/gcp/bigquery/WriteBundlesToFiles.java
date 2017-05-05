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

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.resolveTempLocation;

import com.google.api.services.bigquery.model.TableRow;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writes each bundle of {@link TableRow} elements out to a separate file using {@link
 * TableRowWriter}.
 */
class WriteBundlesToFiles<DestinationT>
    extends DoFn<KV<DestinationT, TableRow>, WriteBundlesToFiles.Result<DestinationT>> {
  private static final Logger LOG = LoggerFactory.getLogger(WriteBundlesToFiles.class);

  // Map from tablespec to a writer for that table.
  private transient Map<DestinationT, TableRowWriter> writers;
  private transient Map<DestinationT, BoundedWindow> writerWindows;
  private final String stepUuid;


  /**
   * The result of the {@link WriteBundlesToFiles} transform. Corresponds to a single output file,
   * and encapsulates the table it is destined to as well as the file byte size.
   */
  public static final class Result<DestinationT> implements Serializable {
    private static final long serialVersionUID = 1L;
    public final String filename;
    public final Long fileByteSize;
    public final DestinationT destination;

    public Result(String filename, Long fileByteSize, DestinationT destination) {
      this.filename = filename;
      this.fileByteSize = fileByteSize;
      this.destination = destination;
    }
  }

  /** a coder for the {@link Result} class. */
  public static class ResultCoder<DestinationT> extends StructuredCoder<Result<DestinationT>> {
    private static final StringUtf8Coder stringCoder = StringUtf8Coder.of();
    private static final VarLongCoder longCoder = VarLongCoder.of();
    private final Coder<DestinationT> destinationCoder;

    public static <DestinationT> ResultCoder<DestinationT> of(
        Coder<DestinationT> destinationCoder) {
      return new ResultCoder<>(destinationCoder);
    }

    ResultCoder(Coder<DestinationT> destinationCoder) {
      this.destinationCoder = destinationCoder;
    }

    @Override
    public void encode(Result<DestinationT> value, OutputStream outStream, Context context)
        throws IOException {
      if (value == null) {
        throw new CoderException("cannot encode a null value");
      }
      stringCoder.encode(value.filename, outStream, context.nested());
      longCoder.encode(value.fileByteSize, outStream, context.nested());
      destinationCoder.encode(value.destination, outStream, context.nested());
    }

    @Override
    public Result<DestinationT> decode(InputStream inStream, Context context) throws IOException {
      String filename = stringCoder.decode(inStream, context.nested());
      long fileByteSize = longCoder.decode(inStream, context.nested());
      DestinationT destination = destinationCoder.decode(inStream, context.nested());
      return new Result<>(filename, fileByteSize, destination);
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return Collections.singletonList(destinationCoder);
    }

    @Override
    public void verifyDeterministic() {}
  }

  WriteBundlesToFiles(String stepUuid) {
    this.stepUuid = stepUuid;
  }

  @StartBundle
  public void startBundle() {
    // This must be done each bundle, as by default the {@link DoFn} might be reused between
    // bundles.
    this.writers = Maps.newHashMap();
    this.writerWindows = Maps.newHashMap();
  }

  @ProcessElement
  public void processElement(ProcessContext c, BoundedWindow window) throws Exception {
    String tempFilePrefix = resolveTempLocation(
        c.getPipelineOptions().getTempLocation(), "BigQueryWriteTemp", stepUuid);
    TableRowWriter writer = writers.get(c.element().getKey());
    if (writer == null) {
      writer = new TableRowWriter(tempFilePrefix);
      writer.open(UUID.randomUUID().toString());
      writers.put(c.element().getKey(), writer);
      writerWindows.put(c.element().getKey(), window);
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
  public void finishBundle(FinishBundleContext c) throws Exception {
    for (Map.Entry<DestinationT, TableRowWriter> entry : writers.entrySet()) {
      TableRowWriter.Result result = entry.getValue().close();
      c.output(
          new Result<>(result.resourceId.toString(), result.byteSize, entry.getKey()),
          writerWindows.get(entry.getKey()).maxTimestamp(),
          writerWindows.get(entry.getKey()));
    }
    writers.clear();
    writerWindows.clear();
  }
}
