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
package org.apache.beam.sdk.io;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.NoSuchElementException;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.io.Read.Bounded;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

/**
 * {@link PTransform}s for reading and writing TensorFlow TFRecord files.
 */
public class TFRecordIO {
  /** The default coder, which returns each record of the input file as a byte array. */
  public static final Coder<byte[]> DEFAULT_BYTE_ARRAY_CODER = ByteArrayCoder.of();

  /**
   * A {@link PTransform} that reads from a TFRecord file (or multiple TFRecord
   * files matching a pattern) and returns a {@link PCollection} containing
   * the decoding of each of the records of the TFRecord file(s) as a byte array.
   */
  public static Read read() {
    return new AutoValue_TFRecordIO_Read.Builder()
        .setValidate(true)
        .setCompressionType(CompressionType.AUTO)
        .build();
  }

  /**
   * A {@link PTransform} that writes a {@link PCollection} to TFRecord file (or
   * multiple TFRecord files matching a sharding pattern), with each
   * element of the input collection encoded into its own record.
   */
  public static Write write() {
    return new AutoValue_TFRecordIO_Write.Builder()
        .setShardTemplate(null)
        .setFilenameSuffix(null)
        .setNumShards(0)
        .setCompressionType(CompressionType.NONE)
        .build();
  }

  /** Implementation of {@link #read}. */
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<byte[]>> {
    @Nullable
    abstract ValueProvider<String> getFilepattern();

    abstract boolean getValidate();

    abstract CompressionType getCompressionType();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setFilepattern(ValueProvider<String> filepattern);
      abstract Builder setValidate(boolean validate);
      abstract Builder setCompressionType(CompressionType compressionType);

      abstract Read build();
    }

    /**
     * Returns a transform for reading TFRecord files that reads from the file(s)
     * with the given filename or filename pattern. This can be a local path (if running locally),
     * or a Google Cloud Storage filename or filename pattern of the form
     * {@code "gs://<bucket>/<filepath>"} (if running locally or using remote
     * execution). Standard <a href="http://docs.oracle.com/javase/tutorial/essential/io/find.html"
     * >Java Filesystem glob patterns</a> ("*", "?", "[..]") are supported.
     */
    public Read from(String filepattern) {
      return from(StaticValueProvider.of(filepattern));
    }

    /**
     * Same as {@code from(filepattern)}, but accepting a {@link ValueProvider}.
     */
    public Read from(ValueProvider<String> filepattern) {
      return toBuilder().setFilepattern(filepattern).build();
    }

    /**
     * Returns a transform for reading TFRecord files that has GCS path validation on
     * pipeline creation disabled.
     *
     * <p>This can be useful in the case where the GCS input does not
     * exist at the pipeline creation time, but is expected to be
     * available at execution time.
     */
    public Read withoutValidation() {
      return toBuilder().setValidate(false).build();
    }

    /**
     * Returns a transform for reading TFRecord files that decompresses all input files
     * using the specified compression type.
     *
     * <p>If no compression type is specified, the default is
     * {@link TFRecordIO.CompressionType#AUTO}.
     * In this mode, the compression type of the file is determined by its extension
     * (e.g., {@code *.gz} is gzipped, {@code *.zlib} is zlib compressed, and all other
     * extensions are uncompressed).
     */
    public Read withCompressionType(TFRecordIO.CompressionType compressionType) {
      return toBuilder().setCompressionType(compressionType).build();
    }

    @Override
    public PCollection<byte[]> expand(PBegin input) {
      if (getFilepattern() == null) {
        throw new IllegalStateException(
            "Need to set the filepattern of a TFRecordIO.Read transform");
      }

      if (getValidate()) {
        checkState(getFilepattern().isAccessible(), "Cannot validate with a RVP.");
        try {
          MatchResult matches = FileSystems.match(getFilepattern().get());
          checkState(
              !matches.metadata().isEmpty(),
              "Unable to find any files matching %s",
              getFilepattern().get());
        } catch (IOException e) {
          throw new IllegalStateException(
              String.format("Failed to validate %s", getFilepattern().get()), e);
        }
      }

      final Bounded<byte[]> read = org.apache.beam.sdk.io.Read.from(getSource());
      PCollection<byte[]> pcol = input.getPipeline().apply("Read", read);
      // Honor the default output coder that would have been used by this PTransform.
      pcol.setCoder(getDefaultOutputCoder());
      return pcol;
    }

    // Helper to create a source specific to the requested compression type.
    protected FileBasedSource<byte[]> getSource() {
      switch (getCompressionType()) {
        case NONE:
          return new TFRecordSource(getFilepattern());
        case AUTO:
          return CompressedSource.from(new TFRecordSource(getFilepattern()));
        case GZIP:
          return
              CompressedSource.from(new TFRecordSource(getFilepattern()))
                  .withDecompression(CompressedSource.CompressionMode.GZIP);
        case ZLIB:
          return
              CompressedSource.from(new TFRecordSource(getFilepattern()))
                  .withDecompression(CompressedSource.CompressionMode.DEFLATE);
        default:
          throw new IllegalArgumentException("Unknown compression type: " + getCompressionType());
      }
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);

      String filepatternDisplay = getFilepattern().isAccessible()
          ? getFilepattern().get() : getFilepattern().toString();
      builder
          .add(DisplayData.item("compressionType", getCompressionType().toString())
              .withLabel("Compression Type"))
          .addIfNotDefault(DisplayData.item("validation", getValidate())
              .withLabel("Validation Enabled"), true)
          .addIfNotNull(DisplayData.item("filePattern", filepatternDisplay)
              .withLabel("File Pattern"));
    }

    @Override
    protected Coder<byte[]> getDefaultOutputCoder() {
      return ByteArrayCoder.of();
    }
  }

  /////////////////////////////////////////////////////////////////////////////

  /** Implementation of {@link #write}. */
  @AutoValue
  public abstract static class Write extends PTransform<PCollection<byte[]>, PDone> {
    /** The directory to which files will be written. */
    @Nullable abstract ValueProvider<ResourceId> getOutputPrefix();

    /** The suffix of each file written, combined with prefix and shardTemplate. */
    @Nullable abstract String getFilenameSuffix();

    /** Requested number of shards. 0 for automatic. */
    abstract int getNumShards();

    /** The shard template of each file written, combined with prefix and suffix. */
    @Nullable abstract String getShardTemplate();

    /** Option to indicate the output sink's compression type. Default is NONE. */
    abstract CompressionType getCompressionType();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setOutputPrefix(ValueProvider<ResourceId> outputPrefix);

      abstract Builder setShardTemplate(String shardTemplate);

      abstract Builder setFilenameSuffix(String filenameSuffix);

      abstract Builder setNumShards(int numShards);

      abstract Builder setCompressionType(CompressionType compressionType);

      abstract Write build();
    }

    /**
     * Writes TFRecord file(s) with the given output prefix. The {@code prefix} will be used as a
     * to generate a {@link ResourceId} using any supported {@link FileSystem}.
     *
     * <p>In addition to their prefix, created files will have a shard identifier (see
     * {@link #withNumShards(int)}), and end in a common suffix, if given by
     * {@link #withSuffix(String)}.
     *
     * <p>For more information on filenames, see {@link DefaultFilenamePolicy}.
     */
    public Write to(String outputPrefix) {
      return to(FileBasedSink.convertToFileResourceIfPossible(outputPrefix));
    }

    /**
     * Writes TFRecord file(s) with a prefix given by the specified resource.
     *
     * <p>In addition to their prefix, created files will have a shard identifier (see
     * {@link #withNumShards(int)}), and end in a common suffix, if given by
     * {@link #withSuffix(String)}.
     *
     * <p>For more information on filenames, see {@link DefaultFilenamePolicy}.
     */
    @Experimental(Kind.FILESYSTEM)
    public Write to(ResourceId outputResource) {
      return toResource(StaticValueProvider.of(outputResource));
    }

    /**
     * Like {@link #to(ResourceId)}.
     */
    @Experimental(Kind.FILESYSTEM)
    public Write toResource(ValueProvider<ResourceId> outputResource) {
      return toBuilder().setOutputPrefix(outputResource).build();
    }

    /**
     * Writes to the file(s) with the given filename suffix.
     *
     * @see ShardNameTemplate
     */
    public Write withSuffix(String suffix) {
      return toBuilder().setFilenameSuffix(suffix).build();
    }

    /**
     * Writes to the provided number of shards.
     *
     * <p>Constraining the number of shards is likely to reduce
     * the performance of a pipeline. Setting this value is not recommended
     * unless you require a specific number of output files.
     *
     * @param numShards the number of shards to use, or 0 to let the system
     *                  decide.
     * @see ShardNameTemplate
     */
    public Write withNumShards(int numShards) {
      checkArgument(numShards >= 0, "Number of shards %s must be >= 0", numShards);
      return toBuilder().setNumShards(numShards).build();
    }

    /**
     * Uses the given shard name template.
     *
     * @see ShardNameTemplate
     */
    public Write withShardNameTemplate(String shardTemplate) {
      return toBuilder().setShardTemplate(shardTemplate).build();
    }

    /**
     * Forces a single file as output.
     *
     * <p>Constraining the number of shards is likely to reduce
     * the performance of a pipeline. Using this setting is not recommended
     * unless you truly require a single output file.
     *
     * <p>This is a shortcut for
     * {@code .withNumShards(1).withShardNameTemplate("")}
     */
    public Write withoutSharding() {
      return withNumShards(1).withShardNameTemplate("");
    }

    /**
     * Writes to output files using the specified compression type.
     *
     * <p>If no compression type is specified, the default is
     * {@link TFRecordIO.CompressionType#NONE}.
     * See {@link TFRecordIO.Read#withCompressionType} for more details.
     */
    public Write withCompressionType(CompressionType compressionType) {
      return toBuilder().setCompressionType(compressionType).build();
    }

    @Override
    public PDone expand(PCollection<byte[]> input) {
      checkState(getOutputPrefix() != null,
          "need to set the output prefix of a TFRecordIO.Write transform");
      WriteFiles<byte[], Void, byte[]> write =
          WriteFiles.to(
              new TFRecordSink(
                  getOutputPrefix(),
                  getShardTemplate(),
                  getFilenameSuffix(),
                  getCompressionType()));
      if (getNumShards() > 0) {
        write = write.withNumShards(getNumShards());
      }
      return input.apply("Write", write);
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);

      String outputPrefixString = null;
      if (getOutputPrefix().isAccessible()) {
        ResourceId dir = getOutputPrefix().get();
        outputPrefixString = dir.toString();
      } else {
        outputPrefixString = getOutputPrefix().toString();
      }
      builder
          .add(DisplayData.item("filePrefix", outputPrefixString)
              .withLabel("Output File Prefix"))
          .addIfNotNull(DisplayData.item("fileSuffix", getFilenameSuffix())
              .withLabel("Output File Suffix"))
          .addIfNotNull(DisplayData.item("shardNameTemplate", getShardTemplate())
                  .withLabel("Output Shard Name Template"))
          .addIfNotDefault(DisplayData.item("numShards", getNumShards())
              .withLabel("Maximum Output Shards"), 0)
          .add(DisplayData.item("compressionType", getCompressionType().toString())
              .withLabel("Compression Type"));
    }

    @Override
    protected Coder<Void> getDefaultOutputCoder() {
      return VoidCoder.of();
    }
  }

  /**
   * Possible TFRecord file compression types.
   */
  public enum CompressionType {
    /**
     * Automatically determine the compression type based on filename extension.
     */
    AUTO(""),
    /**
     * Uncompressed.
     */
    NONE(""),
    /**
     * GZipped.
     */
    GZIP(".gz"),
    /**
     * ZLIB compressed.
     */
    ZLIB(".zlib");

    private String filenameSuffix;

    CompressionType(String suffix) {
      this.filenameSuffix = suffix;
    }

    /**
     * Determine if a given filename matches a compression type based on its extension.
     * @param filename the filename to match
     * @return true iff the filename ends with the compression type's known extension.
     */
    public boolean matches(String filename) {
      return filename.toLowerCase().endsWith(filenameSuffix.toLowerCase());
    }
  }

  //////////////////////////////////////////////////////////////////////////////

  /** Disable construction of utility class. */
  private TFRecordIO() {}

  /**
   * A {@link FileBasedSource} which can decode records in TFRecord files.
   */
  @VisibleForTesting
  static class TFRecordSource extends FileBasedSource<byte[]> {
    @VisibleForTesting
    TFRecordSource(String fileSpec) {
      super(StaticValueProvider.of(fileSpec), 1L);
    }

    @VisibleForTesting
    TFRecordSource(ValueProvider<String> fileSpec) {
      super(fileSpec, Long.MAX_VALUE);
    }

    private TFRecordSource(Metadata metadata, long start, long end) {
      super(metadata, Long.MAX_VALUE, start, end);
    }

    @Override
    protected FileBasedSource<byte[]> createForSubrangeOfFile(
        Metadata metadata,
        long start,
        long end) {
      checkArgument(start == 0, "TFRecordSource is not splittable");
      return new TFRecordSource(metadata, start, end);
    }

    @Override
    protected FileBasedReader<byte[]> createSingleFileReader(PipelineOptions options) {
      return new TFRecordReader(this);
    }

    @Override
    public Coder<byte[]> getDefaultOutputCoder() {
      return DEFAULT_BYTE_ARRAY_CODER;
    }

    @Override
    protected boolean isSplittable() throws Exception {
      // TFRecord files are not splittable
      return false;
    }

    /**
     * A {@link org.apache.beam.sdk.io.FileBasedSource.FileBasedReader FileBasedReader}
     * which can decode records in TFRecord files.
     *
     * <p>See {@link TFRecordIO.TFRecordSource} for further details.
     */
    @VisibleForTesting
    static class TFRecordReader extends FileBasedReader<byte[]> {
      private long startOfRecord;
      private volatile long startOfNextRecord;
      private volatile boolean elementIsPresent;
      private byte[] currentValue;
      private ReadableByteChannel inChannel;
      private TFRecordCodec codec;

      private TFRecordReader(TFRecordSource source) {
        super(source);
      }

      @Override
      public boolean allowsDynamicSplitting() {
        /* TFRecords cannot be dynamically split. */
        return false;
      }

      @Override
      protected long getCurrentOffset() throws NoSuchElementException {
        if (!elementIsPresent) {
          throw new NoSuchElementException();
        }
        return startOfRecord;
      }

      @Override
      public byte[] getCurrent() throws NoSuchElementException {
        if (!elementIsPresent) {
          throw new NoSuchElementException();
        }
        return currentValue;
      }

      @Override
      protected void startReading(ReadableByteChannel channel) throws IOException {
        this.inChannel = channel;
        this.codec = new TFRecordCodec();
      }

      @Override
      protected boolean readNextRecord() throws IOException {
        startOfRecord = startOfNextRecord;
        currentValue = codec.read(inChannel);
        if (currentValue != null) {
          elementIsPresent = true;
          startOfNextRecord = startOfRecord + codec.recordLength(currentValue);
          return true;
        } else {
          elementIsPresent = false;
          return false;
        }
      }
    }
  }

  /** A {@link FileBasedSink} for TFRecord files. Produces TFRecord files. */
  @VisibleForTesting
  static class TFRecordSink extends FileBasedSink<byte[], Void, byte[]> {
    @VisibleForTesting
    TFRecordSink(
        ValueProvider<ResourceId> outputPrefix,
        @Nullable String shardTemplate,
        @Nullable String suffix,
        TFRecordIO.CompressionType compressionType) {
      super(
          outputPrefix,
          DynamicFileDestinations.<byte[]>constant(
              DefaultFilenamePolicy.fromStandardParameters(
                  outputPrefix, shardTemplate, suffix, false)),
          writableByteChannelFactory(compressionType));
    }

    private static class ExtractDirectory implements SerializableFunction<ResourceId, ResourceId> {
      @Override
      public ResourceId apply(ResourceId input) {
        return input.getCurrentDirectory();
      }
    }

    @Override
    public WriteOperation<Void, byte[]> createWriteOperation() {
      return new TFRecordWriteOperation(this);
    }

    private static WritableByteChannelFactory writableByteChannelFactory(
        TFRecordIO.CompressionType compressionType) {
      switch (compressionType) {
        case AUTO:
          throw new IllegalArgumentException("Unsupported compression type AUTO");
        case NONE:
          return CompressionType.UNCOMPRESSED;
        case GZIP:
          return CompressionType.GZIP;
        case ZLIB:
          return CompressionType.DEFLATE;
      }
      return CompressionType.UNCOMPRESSED;
    }

    /** A {@link WriteOperation WriteOperation} for TFRecord files. */
    private static class TFRecordWriteOperation extends WriteOperation<Void, byte[]> {
      private TFRecordWriteOperation(TFRecordSink sink) {
        super(sink);
      }

      @Override
      public Writer<Void, byte[]> createWriter() throws Exception {
        return new TFRecordWriter(this);
      }
    }

    /** A {@link Writer Writer} for TFRecord files. */
    private static class TFRecordWriter extends Writer<Void, byte[]> {
      private WritableByteChannel outChannel;
      private TFRecordCodec codec;

      private TFRecordWriter(WriteOperation<Void, byte[]> writeOperation) {
        super(writeOperation, MimeTypes.BINARY);
      }

      @Override
      protected void prepareWrite(WritableByteChannel channel) throws Exception {
        this.outChannel = channel;
        this.codec = new TFRecordCodec();
      }

      @Override
      public void write(byte[] value) throws Exception {
        codec.write(outChannel, value);
      }
    }
  }

  //////////////////////////////////////////////////////////////////////////////

  /**
   * Codec for TFRecords file format.
   * See https://www.tensorflow.org/api_guides/python/python_io#TFRecords_Format_Details
   */
  private static class TFRecordCodec {
    private static final int HEADER_LEN = (Long.SIZE + Integer.SIZE) / Byte.SIZE;
    private static final int FOOTER_LEN = Integer.SIZE / Byte.SIZE;
    private static HashFunction crc32c = Hashing.crc32c();

    private ByteBuffer header = ByteBuffer.allocate(HEADER_LEN).order(ByteOrder.LITTLE_ENDIAN);
    private ByteBuffer footer = ByteBuffer.allocate(FOOTER_LEN).order(ByteOrder.LITTLE_ENDIAN);

    private int mask(int crc) {
      return ((crc >>> 15) | (crc << 17)) + 0xa282ead8;
    }

    private int hashLong(long x) {
      return mask(crc32c.hashLong(x).asInt());
    }

    private int hashBytes(byte[] x) {
      return mask(crc32c.hashBytes(x).asInt());
    }

    public int recordLength(byte[] data) {
      return HEADER_LEN + data.length + FOOTER_LEN;
    }

    public byte[] read(ReadableByteChannel inChannel) throws IOException {
      header.clear();
      int headerBytes = inChannel.read(header);
      if (headerBytes <= 0) {
        return null;
      }
      checkState(
          headerBytes == HEADER_LEN,
          "Not a valid TFRecord. Fewer than 12 bytes.");
      header.rewind();
      long length = header.getLong();
      int maskedCrc32OfLength = header.getInt();
      checkState(
          hashLong(length) == maskedCrc32OfLength,
          "Mismatch of length mask");

      ByteBuffer data = ByteBuffer.allocate((int) length);
      checkState(inChannel.read(data) == length, "Invalid data");

      footer.clear();
      inChannel.read(footer);
      footer.rewind();
      int maskedCrc32OfData = footer.getInt();

      checkState(
          hashBytes(data.array()) == maskedCrc32OfData,
          "Mismatch of data mask");
      return data.array();
    }

    public void write(WritableByteChannel outChannel, byte[] data) throws IOException {
      int maskedCrc32OfLength = hashLong(data.length);
      int maskedCrc32OfData = hashBytes(data);

      header.clear();
      header.putLong(data.length).putInt(maskedCrc32OfLength);
      header.rewind();
      outChannel.write(header);

      outChannel.write(ByteBuffer.wrap(data));

      footer.clear();
      footer.putInt(maskedCrc32OfData);
      footer.rewind();
      outChannel.write(footer);
    }
  }

}
