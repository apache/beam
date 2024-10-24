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
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryRowWriter.BigQueryRowSerializationException;
import org.apache.beam.sdk.io.gcp.bigquery.WriteBundlesToFiles.Result;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.errorhandling.BadRecordRouter;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.ShardedKey;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Writes each bundle of {@link TableRow} elements out to separate file using {@link
 * TableRowWriter}. Elements destined to different destinations are written to separate files. The
 * transform will not write an element to a file if it is already writing to {@link
 * #maxNumWritersPerBundle} files and the element is destined to a new destination. In this case,
 * the element will be spilled into the output, and the {@link WriteGroupedRecordsToFiles} transform
 * will take care of writing it to a file.
 */
class WriteBundlesToFiles<DestinationT extends @NonNull Object, ElementT>
    extends DoFn<KV<DestinationT, ElementT>, Result<DestinationT>> {

  // When we spill records, shard the output keys to prevent hotspots. Experiments running up to
  // 10TB of data have shown a sharding of 10 to be a good choice.
  private static final int SPILLED_RECORD_SHARDING_FACTOR = 10;

  // Map from tablespec to a writer for that table.
  private transient @Nullable Map<DestinationT, BigQueryRowWriter<ElementT>> writers = null;
  private transient @Nullable Map<DestinationT, BoundedWindow> writerWindows = null;

  private final PCollectionView<String> tempFilePrefixView;
  private final TupleTag<KV<ShardedKey<DestinationT>, ElementT>> unwrittenRecordsTag;
  private final int maxNumWritersPerBundle;
  private final long maxFileSize;
  private final RowWriterFactory<ElementT, DestinationT> rowWriterFactory;
  private final Coder<KV<DestinationT, ElementT>> coder;
  private final BadRecordRouter badRecordRouter;
  private int spilledShardNumber;

  /**
   * The result of the {@link WriteBundlesToFiles} transform. Corresponds to a single output file,
   * and encapsulates the table it is destined to as well as the file byte size.
   */
  static final class Result<DestinationT> implements Serializable {
    private static final long serialVersionUID = 1L;
    public final String filename;
    public final Long fileByteSize;
    public final DestinationT destination;

    public Result(String filename, Long fileByteSize, DestinationT destination) {
      Preconditions.checkArgumentNotNull(destination);
      this.filename = filename;
      this.fileByteSize = fileByteSize;
      this.destination = destination;
    }

    @Override
    public boolean equals(@Nullable Object other) {
      if (other instanceof Result) {
        Result<DestinationT> o = (Result<DestinationT>) other;
        return Objects.equals(this.filename, o.filename)
            && Objects.equals(this.fileByteSize, o.fileByteSize)
            && Objects.equals(this.destination, o.destination);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return Objects.hash(filename, fileByteSize, destination);
    }

    @Override
    public String toString() {
      return "Result{"
          + "filename='"
          + filename
          + '\''
          + ", fileByteSize="
          + fileByteSize
          + ", destination="
          + destination
          + '}';
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
    public void encode(Result<DestinationT> value, OutputStream outStream) throws IOException {
      if (value == null) {
        throw new CoderException("cannot encode a null value");
      }
      stringCoder.encode(value.filename, outStream);
      longCoder.encode(value.fileByteSize, outStream);
      destinationCoder.encode(value.destination, outStream);
    }

    @Override
    public Result<DestinationT> decode(InputStream inStream) throws IOException {
      String filename = stringCoder.decode(inStream);
      long fileByteSize = longCoder.decode(inStream);
      DestinationT destination = destinationCoder.decode(inStream);
      return new Result<>(filename, fileByteSize, destination);
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return Collections.singletonList(destinationCoder);
    }

    @Override
    public void verifyDeterministic() {}
  }

  WriteBundlesToFiles(
      PCollectionView<String> tempFilePrefixView,
      TupleTag<KV<ShardedKey<DestinationT>, ElementT>> unwrittenRecordsTag,
      int maxNumWritersPerBundle,
      long maxFileSize,
      RowWriterFactory<ElementT, DestinationT> rowWriterFactory,
      Coder<KV<DestinationT, ElementT>> coder,
      BadRecordRouter badRecordRouter) {
    this.tempFilePrefixView = tempFilePrefixView;
    this.unwrittenRecordsTag = unwrittenRecordsTag;
    this.maxNumWritersPerBundle = maxNumWritersPerBundle;
    this.maxFileSize = maxFileSize;
    this.rowWriterFactory = rowWriterFactory;
    this.coder = coder;
    this.badRecordRouter = badRecordRouter;
  }

  @StartBundle
  public void startBundle() {
    // This must be done for each bundle, as by default the {@link DoFn} might be reused between
    // bundles.
    this.writerWindows = Maps.newHashMap();
    this.writers = Maps.newHashMap();
    this.spilledShardNumber = ThreadLocalRandom.current().nextInt(SPILLED_RECORD_SHARDING_FACTOR);
  }

  BigQueryRowWriter<ElementT> createAndInsertWriter(
      DestinationT destination, String tempFilePrefix, BoundedWindow window) throws Exception {
    Map<DestinationT, BoundedWindow> writerWindows =
        Preconditions.checkStateNotNull(this.writerWindows);
    Map<DestinationT, BigQueryRowWriter<ElementT>> writers =
        Preconditions.checkStateNotNull(this.writers);
    BigQueryRowWriter<ElementT> writer =
        rowWriterFactory.createRowWriter(tempFilePrefix, destination);
    writerWindows.put(destination, window);
    writers.put(destination, writer);
    return writer;
  }

  @ProcessElement
  public void processElement(
      ProcessContext c,
      @Element KV<DestinationT, ElementT> element,
      BoundedWindow window,
      MultiOutputReceiver outputReceiver)
      throws Exception {
    Map<DestinationT, BigQueryRowWriter<ElementT>> writers =
        Preconditions.checkStateNotNull(this.writers);
    String tempFilePrefix = c.sideInput(tempFilePrefixView);
    DestinationT destination = c.element().getKey();

    BigQueryRowWriter<ElementT> writer;
    if (writers.containsKey(destination)) {
      writer = writers.get(destination);
    } else {
      // Only create a new writer if we have fewer than maxNumWritersPerBundle already in this
      // bundle.
      if (writers.size() <= maxNumWritersPerBundle) {
        writer = createAndInsertWriter(destination, tempFilePrefix, window);
      } else {
        // This means that we already had too many writers open in this bundle. "spill" this record
        // into the output. It will be grouped and written to a file in a subsequent stage.
        c.output(
            unwrittenRecordsTag,
            KV.of(
                ShardedKey.of(destination, ++spilledShardNumber % SPILLED_RECORD_SHARDING_FACTOR),
                element.getValue()));
        return;
      }
    }

    if (writer.getByteSize() > maxFileSize) {
      // File is too big. Close it and open a new file.
      writer.close();
      BigQueryRowWriter.Result result = writer.getResult();
      c.output(new Result<>(result.resourceId.toString(), result.byteSize, destination));
      writer = createAndInsertWriter(destination, tempFilePrefix, window);
    }

    try {
      writer.write(element.getValue());
    } catch (BigQueryRowSerializationException e) {
      try {
        badRecordRouter.route(
            outputReceiver,
            element,
            coder,
            e,
            "Unable to Write BQ Record to File because serialization to TableRow failed");
      } catch (Exception e2) {
        cleanupWriter(writer, e2);
      }
    } catch (Exception e) {
      cleanupWriter(writer, e);
    }
  }

  private void cleanupWriter(BigQueryRowWriter<ElementT> writer, Exception e) throws Exception {
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

  @FinishBundle
  public void finishBundle(FinishBundleContext c) throws Exception {
    Map<DestinationT, BigQueryRowWriter<ElementT>> writers =
        Preconditions.checkStateNotNull(this.writers);
    Map<DestinationT, BoundedWindow> writerWindows =
        Preconditions.checkStateNotNull(this.writerWindows);

    List<Exception> exceptionList = Lists.newArrayList();
    for (BigQueryRowWriter<ElementT> writer : writers.values()) {
      try {
        writer.close();
      } catch (Exception e) {
        exceptionList.add(e);
      }
    }
    if (!exceptionList.isEmpty()) {
      Exception e = new IOException("Failed to close some writers");
      for (Exception thrown : exceptionList) {
        e.addSuppressed(thrown);
      }
      throw e;
    }

    for (Map.Entry<DestinationT, BigQueryRowWriter<ElementT>> entry : writers.entrySet()) {
      try {
        DestinationT destination = entry.getKey();
        BigQueryRowWriter<ElementT> writer = entry.getValue();
        BigQueryRowWriter.Result result = writer.getResult();
        BoundedWindow window = writerWindows.get(destination);
        Preconditions.checkStateNotNull(window);
        c.output(
            new Result<>(result.resourceId.toString(), result.byteSize, destination),
            window.maxTimestamp(),
            window);
      } catch (Exception e) {
        exceptionList.add(e);
      }
    }
    writers.clear();
  }
}
