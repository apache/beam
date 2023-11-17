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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.io.iceberg.WriteBundlesToFiles.Result;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.ShardedKey;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Objects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

@SuppressWarnings("all") // TODO: Remove this once development is stable.
public class WriteBundlesToFiles<DestinationT extends Object, ElementT>
    extends DoFn<KV<DestinationT, ElementT>, Result<DestinationT>> {

  private transient Map<DestinationT, RecordWriter<ElementT>> writers;
  private transient Map<DestinationT, BoundedWindow> windows;

  private static final int SPILLED_RECORD_SHARDING_FACTOR = 10;

  private final PCollectionView<String> locationPrefixView;

  private final TupleTag<KV<ShardedKey<DestinationT>, ElementT>> successfulWritesTag;
  private final TupleTag<KV<ShardedKey<DestinationT>, ElementT>> unwrittenRecordsTag;
  private final int maxWritersPerBundle;
  private final long maxFileSize;

  private final RecordWriterFactory<ElementT, DestinationT> recordWriterFactory;

  private int spilledShardNumber;

  static final class Result<DestinationT> implements Serializable {
    private static final long serialVersionUID = 1L;

    public final String tableId;
    public final String location;

    public final PartitionSpec partitionSpec;

    public final MetadataUpdate update;

    public final DestinationT destination;

    public Result(
        String tableId,
        String location,
        DataFile dataFile,
        PartitionSpec partitionSpec,
        DestinationT destination) {
      this.tableId = tableId;
      this.location = location;
      this.update = MetadataUpdate.of(partitionSpec, dataFile);
      this.partitionSpec = partitionSpec;
      this.destination = destination;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof Result) {
        Result result = (Result) obj;
        return Objects.equal(result.tableId, tableId)
            && Objects.equal(result.location, location)
            && Objects.equal(result.partitionSpec, partitionSpec)
            && Objects.equal(result.update.getDataFiles().get(0), update.getDataFiles().get(0))
            && Objects.equal(destination, result.destination);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(
          tableId, location, update.getDataFiles().get(0), partitionSpec, destination);
    }

    @Override
    public String toString() {
      return "Result{"
          + "table='"
          + tableId
          + '\''
          + "location='"
          + location
          + '\''
          + ", fileByteSize="
          + update.getDataFiles().get(0).fileSizeInBytes()
          + ", destination="
          + destination
          + '}';
    }
  }

  public static class ResultCoder<DestinationT> extends StructuredCoder<Result<DestinationT>> {
    private static final StringUtf8Coder stringCoder = StringUtf8Coder.of();
    private static final Coder<MetadataUpdate> metadataCoder = MetadataUpdate.coder();

    private static final SerializableCoder<PartitionSpec> partitionSpecCoder =
        SerializableCoder.of(PartitionSpec.class);

    private final Coder<DestinationT> destinationCoder;

    public ResultCoder(Coder<DestinationT> destinationCoder) {
      this.destinationCoder = destinationCoder;
    }

    @Override
    public void encode(
        Result<DestinationT> value, @UnknownKeyFor @NonNull @Initialized OutputStream outStream)
        throws @UnknownKeyFor @NonNull @Initialized CoderException, @UnknownKeyFor @NonNull
            @Initialized IOException {

      // Convert most everything to Avro for serialization

      // Table id and location are strings
      stringCoder.encode(value.tableId, outStream);
      stringCoder.encode(value.location, outStream);
      // PartitionSpec is Java serialized because we need it to decode DataFile
      destinationCoder.encode(value.destination, outStream);
      metadataCoder.encode(value.update, outStream);
      partitionSpecCoder.encode(value.partitionSpec, outStream);
    }

    @Override
    public Result<DestinationT> decode(InputStream inStream) throws CoderException, IOException {
      String tableId = stringCoder.decode(inStream);
      String location = stringCoder.decode(inStream);
      DestinationT dest = destinationCoder.decode(inStream);
      MetadataUpdate update = metadataCoder.decode(inStream);
      PartitionSpec spec = partitionSpecCoder.decode(inStream);
      return new Result<>(tableId, location, update.getDataFiles().get(0), spec, dest);
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return Collections.singletonList(destinationCoder);
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {}

    public static <DestinationT> ResultCoder<DestinationT> of(
        Coder<DestinationT> destinationCoder) {
      return new ResultCoder<>(destinationCoder);
    }
  }

  public WriteBundlesToFiles(
      PCollectionView<String> locationPrefixView,
      TupleTag<KV<ShardedKey<DestinationT>, ElementT>> successfulWritesTag,
      TupleTag<KV<ShardedKey<DestinationT>, ElementT>> unwrittenRecordsTag,
      int maximumWritersPerBundle,
      long maxFileSize,
      RecordWriterFactory<ElementT, DestinationT> recordWriterFactory) {
    this.locationPrefixView = locationPrefixView;
    this.successfulWritesTag = successfulWritesTag;
    this.unwrittenRecordsTag = unwrittenRecordsTag;
    this.maxWritersPerBundle = maximumWritersPerBundle;
    this.maxFileSize = maxFileSize;
    this.recordWriterFactory = recordWriterFactory;
  }

  @StartBundle
  public void startBundle() {
    this.writers = Maps.newHashMap();
    this.windows = Maps.newHashMap();
    this.spilledShardNumber = ThreadLocalRandom.current().nextInt(SPILLED_RECORD_SHARDING_FACTOR);
  }

  RecordWriter<ElementT> createWriter(
      DestinationT destination, String location, BoundedWindow window) throws Exception {
    Map<DestinationT, BoundedWindow> windows = Preconditions.checkNotNull(this.windows);
    Map<DestinationT, RecordWriter<ElementT>> writers = Preconditions.checkNotNull(this.writers);
    RecordWriter<ElementT> writer = recordWriterFactory.createWriter(location, destination);
    windows.put(destination, window);
    writers.put(destination, writer);
    return writer;
  }

  @ProcessElement
  public void processElement(
      ProcessContext c, @Element KV<DestinationT, ElementT> element, BoundedWindow window)
      throws Exception {
    Map<DestinationT, RecordWriter<ElementT>> writers = Preconditions.checkNotNull(this.writers);
    String locationPrefix = c.sideInput(locationPrefixView);
    DestinationT destination = element.getKey();
    RecordWriter<ElementT> writer;
    if (writers.containsKey(destination)) {
      writer = writers.get(destination);
    } else {
      if (writers.size() <= maxWritersPerBundle) {
        writer = createWriter(destination, locationPrefix, window);
      } else {
        c.output(
            unwrittenRecordsTag,
            KV.of(
                ShardedKey.of(destination, ++spilledShardNumber % SPILLED_RECORD_SHARDING_FACTOR),
                element.getValue()));
        return;
      }
    }

    if (writer.bytesWritten() > maxFileSize) {
      writer.close();
      Table t = writer.table();

      c.output(new Result<>(t.name(), writer.location(), writer.dataFile(), t.spec(), destination));
      writer = createWriter(destination, locationPrefix, window);
    }

    try {
      writer.write(element.getValue());
      c.output(
          successfulWritesTag,
          KV.of(
              ShardedKey.of(destination, spilledShardNumber % SPILLED_RECORD_SHARDING_FACTOR),
              element.getValue()));
    } catch (Exception e) {
      try {
        writer.close();
      } catch (Exception closeException) {
        e.addSuppressed(closeException);
      }
      throw e;
    }
  }

  @FinishBundle
  public void finishBundle(FinishBundleContext c) throws Exception {
    Map<DestinationT, BoundedWindow> windows = Preconditions.checkNotNull(this.windows);
    Map<DestinationT, RecordWriter<ElementT>> writers = Preconditions.checkNotNull(this.writers);
    List<Exception> exceptionList = Lists.newArrayList();
    for (RecordWriter<ElementT> writer : writers.values()) {
      try {
        writer.close();
      } catch (Exception e) {
        exceptionList.add(e);
      }
    }
    if (!exceptionList.isEmpty()) {
      Exception e = new IOException("Exception closing some writers.");
      for (Exception thrown : exceptionList) {
        e.addSuppressed(thrown);
      }
      throw e;
    }

    exceptionList.clear();
    for (Map.Entry<DestinationT, RecordWriter<ElementT>> entry : writers.entrySet()) {
      try {
        DestinationT destination = entry.getKey();

        RecordWriter<ElementT> writer = entry.getValue();
        BoundedWindow window = windows.get(destination);
        Preconditions.checkNotNull(window);
        Table t = writer.table();
        c.output(
            new Result<>(t.name(), writer.location(), writer.dataFile(), t.spec(), destination),
            window.maxTimestamp(),
            window);
      } catch (Exception e) {
        exceptionList.add(e);
      }
    }
    writers.clear();
    if (!exceptionList.isEmpty()) {
      Exception e = new IOException("Exception emitting writer metadata.");
      for (Exception thrown : exceptionList) {
        e.addSuppressed(thrown);
      }
      throw e;
    }
  }
}
