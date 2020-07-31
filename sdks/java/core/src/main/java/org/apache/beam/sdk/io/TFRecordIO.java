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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.NoSuchElementException;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
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
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.hash.HashFunction;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.hash.Hashing;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * {@link PTransform}s for reading and writing TensorFlow TFRecord files.
 *
 * <p>For reading files, use {@link #read}.
 *
 * <p>For simple cases of writing files, use {@link #write}. For more complex cases (such as ability
 * to write windowed data or writing to multiple destinations) use {@link #sink} in combination with
 * {@link FileIO#write} or {@link FileIO#writeDynamic}.
 */
public class TFRecordIO {
  /** The default coder, which returns each record of the input file as a byte array. */
  public static final Coder<byte[]> DEFAULT_BYTE_ARRAY_CODER = ByteArrayCoder.of();

  /**
   * A {@link PTransform} that reads from a TFRecord file (or multiple TFRecord files matching a
   * pattern) and returns a {@link PCollection} containing the decoding of each of the records of
   * the TFRecord file(s) as a byte array.
   */
  public static Read read() {
    return new AutoValue_TFRecordIO_Read.Builder()
        .setValidate(true)
        .setCompression(Compression.AUTO)
        .build();
  }

  /**
   * Like {@link #read}, but reads each file in a {@link PCollection} of {@link
   * FileIO.ReadableFile}, returned by {@link FileIO#readMatches}.
   */
  public static ReadFiles readFiles() {
    return new AutoValue_TFRecordIO_ReadFiles.Builder().build();
  }

  /**
   * A {@link PTransform} that writes a {@link PCollection} to TFRecord file (or multiple TFRecord
   * files matching a sharding pattern), with each element of the input collection encoded into its
   * own record.
   */
  public static Write write() {
    return new AutoValue_TFRecordIO_Write.Builder()
        .setShardTemplate(null)
        .setFilenameSuffix(null)
        .setNumShards(0)
        .setCompression(Compression.UNCOMPRESSED)
        .setNoSpilling(false)
        .build();
  }

  /**
   * Returns a {@link FileIO.Sink} for use with {@link FileIO#write} and {@link
   * FileIO#writeDynamic}.
   */
  public static Sink sink() {
    return new Sink();
  }

  /** Implementation of {@link #read}. */
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<byte[]>> {

    abstract @Nullable ValueProvider<String> getFilepattern();

    abstract boolean getValidate();

    abstract Compression getCompression();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setFilepattern(ValueProvider<String> filepattern);

      abstract Builder setValidate(boolean validate);

      abstract Builder setCompression(Compression compression);

      abstract Read build();
    }

    /**
     * Returns a transform for reading TFRecord files that reads from the file(s) with the given
     * filename or filename pattern. This can be a local path (if running locally), or a Google
     * Cloud Storage filename or filename pattern of the form {@code "gs://<bucket>/<filepath>"} (if
     * running locally or using remote execution). Standard <a
     * href="http://docs.oracle.com/javase/tutorial/essential/io/find.html" >Java Filesystem glob
     * patterns</a> ("*", "?", "[..]") are supported.
     */
    public Read from(String filepattern) {
      return from(StaticValueProvider.of(filepattern));
    }

    /** Same as {@code from(filepattern)}, but accepting a {@link ValueProvider}. */
    public Read from(ValueProvider<String> filepattern) {
      return toBuilder().setFilepattern(filepattern).build();
    }

    /**
     * Returns a transform for reading TFRecord files that has GCS path validation on pipeline
     * creation disabled.
     *
     * <p>This can be useful in the case where the GCS input does not exist at the pipeline creation
     * time, but is expected to be available at execution time.
     */
    public Read withoutValidation() {
      return toBuilder().setValidate(false).build();
    }

    /** @deprecated Use {@link #withCompression}. */
    @Deprecated
    public Read withCompressionType(TFRecordIO.CompressionType compressionType) {
      return withCompression(compressionType.canonical);
    }

    /**
     * Returns a transform for reading TFRecord files that decompresses all input files using the
     * specified compression type.
     *
     * <p>If no compression type is specified, the default is {@link Compression#AUTO}. In this
     * mode, the compression type of the file is determined by its extension via {@link
     * Compression#detect(String)}.
     */
    public Read withCompression(Compression compression) {
      return toBuilder().setCompression(compression).build();
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

      return input.apply("Read", org.apache.beam.sdk.io.Read.from(getSource()));
    }

    // Helper to create a source specific to the requested compression type.
    protected FileBasedSource<byte[]> getSource() {
      return CompressedSource.from(new TFRecordSource(getFilepattern()))
          .withCompression(getCompression());
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder
          .add(
              DisplayData.item("compressionType", getCompression().toString())
                  .withLabel("Compression Type"))
          .addIfNotDefault(
              DisplayData.item("validation", getValidate()).withLabel("Validation Enabled"), true)
          .addIfNotNull(
              DisplayData.item("filePattern", getFilepattern()).withLabel("File Pattern"));
    }
  }

  /////////////////////////////////////////////////////////////////////////////

  /** Implementation of {@link #readFiles}. */
  @AutoValue
  public abstract static class ReadFiles
      extends PTransform<PCollection<FileIO.ReadableFile>, PCollection<byte[]>> {

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract TFRecordIO.ReadFiles build();
    }

    @Override
    public PCollection<byte[]> expand(PCollection<FileIO.ReadableFile> input) {
      return input.apply(
          "Read all via FileBasedSource",
          new ReadAllViaFileBasedSource<>(
              Long.MAX_VALUE, new CreateSourceFn(), DEFAULT_BYTE_ARRAY_CODER));
    }

    private static class CreateSourceFn
        implements SerializableFunction<String, FileBasedSource<byte[]>> {

      @Override
      public FileBasedSource<byte[]> apply(String input) {
        return new TFRecordSource(StaticValueProvider.of(input));
      }
    }
  }

  /////////////////////////////////////////////////////////////////////////////

  /** Implementation of {@link #write}. */
  @AutoValue
  public abstract static class Write extends PTransform<PCollection<byte[]>, PDone> {
    /** The directory to which files will be written. */
    abstract @Nullable ValueProvider<ResourceId> getOutputPrefix();

    /** The suffix of each file written, combined with prefix and shardTemplate. */
    abstract @Nullable String getFilenameSuffix();

    /** Requested number of shards. 0 for automatic. */
    abstract int getNumShards();

    /** The shard template of each file written, combined with prefix and suffix. */
    abstract @Nullable String getShardTemplate();

    /** Option to indicate the output sink's compression type. Default is NONE. */
    abstract Compression getCompression();

    /** Whether to skip the spilling of data caused by having maxNumWritersPerBundle. */
    abstract boolean getNoSpilling();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setOutputPrefix(ValueProvider<ResourceId> outputPrefix);

      abstract Builder setShardTemplate(@Nullable String shardTemplate);

      abstract Builder setFilenameSuffix(@Nullable String filenameSuffix);

      abstract Builder setNumShards(int numShards);

      abstract Builder setCompression(Compression compression);

      abstract Builder setNoSpilling(boolean noSpilling);

      abstract Write build();
    }

    /**
     * Writes TFRecord file(s) with the given output prefix. The {@code prefix} will be used as a to
     * generate a {@link ResourceId} using any supported {@link FileSystem}.
     *
     * <p>In addition to their prefix, created files will have a shard identifier (see {@link
     * #withNumShards(int)}), and end in a common suffix, if given by {@link #withSuffix(String)}.
     *
     * <p>For more information on filenames, see {@link DefaultFilenamePolicy}.
     */
    public Write to(String outputPrefix) {
      return to(FileBasedSink.convertToFileResourceIfPossible(outputPrefix));
    }

    /**
     * Writes TFRecord file(s) with a prefix given by the specified resource.
     *
     * <p>In addition to their prefix, created files will have a shard identifier (see {@link
     * #withNumShards(int)}), and end in a common suffix, if given by {@link #withSuffix(String)}.
     *
     * <p>For more information on filenames, see {@link DefaultFilenamePolicy}.
     */
    @Experimental(Kind.FILESYSTEM)
    public Write to(ResourceId outputResource) {
      return toResource(StaticValueProvider.of(outputResource));
    }

    /** Like {@link #to(ResourceId)}. */
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
     * <p>Constraining the number of shards is likely to reduce the performance of a pipeline.
     * Setting this value is not recommended unless you require a specific number of output files.
     *
     * @param numShards the number of shards to use, or 0 to let the system decide.
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
     * <p>Constraining the number of shards is likely to reduce the performance of a pipeline. Using
     * this setting is not recommended unless you truly require a single output file.
     *
     * <p>This is a shortcut for {@code .withNumShards(1).withShardNameTemplate("")}
     */
    public Write withoutSharding() {
      return withNumShards(1).withShardNameTemplate("");
    }

    /** @deprecated use {@link #withCompression}. */
    @Deprecated
    public Write withCompressionType(CompressionType compressionType) {
      return withCompression(compressionType.canonical);
    }

    /**
     * Writes to output files using the specified compression type.
     *
     * <p>If no compression type is specified, the default is {@link Compression#UNCOMPRESSED}. See
     * {@link TFRecordIO.Read#withCompression} for more details.
     */
    public Write withCompression(Compression compression) {
      return toBuilder().setCompression(compression).build();
    }

    /** See {@link WriteFiles#withNoSpilling()}. */
    public Write withNoSpilling() {
      return toBuilder().setNoSpilling(true).build();
    }

    @Override
    public PDone expand(PCollection<byte[]> input) {
      checkState(
          getOutputPrefix() != null,
          "need to set the output prefix of a TFRecordIO.Write transform");
      WriteFiles<byte[], Void, byte[]> write =
          WriteFiles.to(
              new TFRecordSink(
                  getOutputPrefix(), getShardTemplate(), getFilenameSuffix(), getCompression()));
      if (getNumShards() > 0) {
        write = write.withNumShards(getNumShards());
      }
      if (getNoSpilling()) {
        write = write.withNoSpilling();
      }
      input.apply("Write", write);
      return PDone.in(input.getPipeline());
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder
          .add(DisplayData.item("filePrefix", getOutputPrefix()).withLabel("Output File Prefix"))
          .addIfNotNull(
              DisplayData.item("fileSuffix", getFilenameSuffix()).withLabel("Output File Suffix"))
          .addIfNotNull(
              DisplayData.item("shardNameTemplate", getShardTemplate())
                  .withLabel("Output Shard Name Template"))
          .addIfNotDefault(
              DisplayData.item("numShards", getNumShards()).withLabel("Maximum Output Shards"), 0)
          .add(
              DisplayData.item("compressionType", getCompression().toString())
                  .withLabel("Compression Type"));
    }
  }

  /** A {@link FileIO.Sink} for use with {@link FileIO#write} and {@link FileIO#writeDynamic}. */
  public static class Sink implements FileIO.Sink<byte[]> {
    private transient @Nullable WritableByteChannel channel;
    private transient @Nullable TFRecordCodec codec;

    @Override
    public void open(WritableByteChannel channel) throws IOException {
      this.channel = channel;
      this.codec = new TFRecordCodec();
    }

    @Override
    public void write(byte[] element) throws IOException {
      codec.write(channel, element);
    }

    @Override
    public void flush() throws IOException {
      // Nothing to do here.
    }
  }

  /** @deprecated Use {@link Compression}. */
  @Deprecated
  public enum CompressionType {
    /** @see Compression#AUTO */
    AUTO(Compression.AUTO),

    /** @see Compression#UNCOMPRESSED */
    NONE(Compression.UNCOMPRESSED),

    /** @see Compression#GZIP */
    GZIP(Compression.GZIP),

    /** @see Compression#DEFLATE */
    ZLIB(Compression.DEFLATE);

    private final Compression canonical;

    CompressionType(Compression canonical) {
      this.canonical = canonical;
    }

    /** @see Compression#matches */
    public boolean matches(String filename) {
      return canonical.matches(filename);
    }
  }

  //////////////////////////////////////////////////////////////////////////////

  /** Disable construction of utility class. */
  private TFRecordIO() {}

  /** A {@link FileBasedSource} which can decode records in TFRecord files. */
  @VisibleForTesting
  static class TFRecordSource extends FileBasedSource<byte[]> {
    @VisibleForTesting
    TFRecordSource(ValueProvider<String> fileSpec) {
      super(fileSpec, Long.MAX_VALUE);
    }

    private TFRecordSource(Metadata metadata, long start, long end) {
      super(metadata, Long.MAX_VALUE, start, end);
    }

    @Override
    protected FileBasedSource<byte[]> createForSubrangeOfFile(
        Metadata metadata, long start, long end) {
      checkArgument(start == 0, "TFRecordSource is not splittable");
      return new TFRecordSource(metadata, start, end);
    }

    @Override
    protected FileBasedReader<byte[]> createSingleFileReader(PipelineOptions options) {
      return new TFRecordReader(this);
    }

    @Override
    public Coder<byte[]> getOutputCoder() {
      return DEFAULT_BYTE_ARRAY_CODER;
    }

    @Override
    protected boolean isSplittable() {
      // TFRecord files are not splittable
      return false;
    }

    /**
     * A {@link org.apache.beam.sdk.io.FileBasedSource.FileBasedReader FileBasedReader} which can
     * decode records in TFRecord files.
     *
     * <p>See {@link TFRecordIO.TFRecordSource} for further details.
     */
    @VisibleForTesting
    static class TFRecordReader extends FileBasedReader<byte[]> {
      private long startOfRecord;
      private volatile long startOfNextRecord;
      private volatile boolean elementIsPresent;
      private byte @Nullable [] currentValue;
      private @Nullable ReadableByteChannel inChannel;
      private @Nullable TFRecordCodec codec;

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
        Compression compression) {
      super(
          outputPrefix,
          DynamicFileDestinations.constant(
              DefaultFilenamePolicy.fromStandardParameters(
                  outputPrefix, shardTemplate, suffix, false)),
          compression);
    }

    @Override
    public WriteOperation<Void, byte[]> createWriteOperation() {
      return new TFRecordWriteOperation(this);
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
      private @Nullable WritableByteChannel outChannel;
      private @Nullable TFRecordCodec codec;

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
   * Codec for TFRecords file format. See
   * https://www.tensorflow.org/versions/r1.11/api_guides/python/python_io#TFRecords_Format_Details
   */
  @VisibleForTesting
  static class TFRecordCodec {
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

    public byte @Nullable [] read(ReadableByteChannel inChannel) throws IOException {
      header.clear();
      int headerBytes = read(inChannel, header);
      if (headerBytes == 0) {
        return null;
      }
      checkState(headerBytes == HEADER_LEN, "Not a valid TFRecord. Fewer than 12 bytes.");

      header.rewind();
      long length64 = header.getLong();
      long lengthHash = hashLong(length64);
      int maskedCrc32OfLength = header.getInt();
      if (lengthHash != maskedCrc32OfLength) {
        throw new IOException(
            String.format(
                "Mismatch of length mask when reading a record. Expected %d but received %d.",
                maskedCrc32OfLength, lengthHash));
      }
      int length = (int) length64;
      if (length != length64) {
        throw new IOException(String.format("length overflow %d", length64));
      }

      ByteBuffer data = ByteBuffer.allocate(length);
      readFully(inChannel, data);

      footer.clear();
      readFully(inChannel, footer);
      footer.rewind();

      int maskedCrc32OfData = footer.getInt();
      int dataHash = hashBytes(data.array());
      if (dataHash != maskedCrc32OfData) {
        throw new IOException(
            String.format(
                "Mismatch of data mask when reading a record. Expected %d but received %d.",
                maskedCrc32OfData, dataHash));
      }
      return data.array();
    }

    public void write(WritableByteChannel outChannel, byte[] data) throws IOException {
      int maskedCrc32OfLength = hashLong(data.length);
      int maskedCrc32OfData = hashBytes(data);

      header.clear();
      header.putLong(data.length).putInt(maskedCrc32OfLength);
      header.rewind();
      writeFully(outChannel, header);

      writeFully(outChannel, ByteBuffer.wrap(data));

      footer.clear();
      footer.putInt(maskedCrc32OfData);
      footer.rewind();
      writeFully(outChannel, footer);
    }

    @VisibleForTesting
    static void readFully(ReadableByteChannel in, ByteBuffer bb) throws IOException {
      int expected = bb.remaining();
      int actual = read(in, bb);
      if (expected != actual) {
        throw new IOException(String.format("expected %d, but got %d", expected, actual));
      }
    }

    private static int read(ReadableByteChannel in, ByteBuffer bb) throws IOException {
      int expected = bb.remaining();
      while (bb.hasRemaining() && in.read(bb) >= 0) {}
      return expected - bb.remaining();
    }

    @VisibleForTesting
    static void writeFully(WritableByteChannel channel, ByteBuffer buffer) throws IOException {
      while (buffer.hasRemaining()) {
        channel.write(buffer);
      }
    }
  }
}
