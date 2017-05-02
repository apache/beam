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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import com.google.common.annotations.VisibleForTesting;
import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InvalidObjectException;
import java.io.ObjectStreamException;
import java.io.PushbackInputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.util.Arrays;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;
import javax.annotation.concurrent.GuardedBy;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileConstants;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.PipelineRunner;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.snappy.SnappyCompressorInputStream;
import org.apache.commons.compress.compressors.xz.XZCompressorInputStream;
import org.apache.commons.compress.utils.CountingInputStream;

// CHECKSTYLE.OFF: JavadocStyle
/**
 * Do not use in pipelines directly: most users should use {@link AvroIO.Read}.
 *
 * <p>A {@link FileBasedSource} for reading Avro files.
 *
 * <p>To read a {@link PCollection} of objects from one or more Avro files, use
 * {@link AvroSource#from} to specify the path(s) of the files to read. The {@link AvroSource} that
 * is returned will read objects of type {@link GenericRecord} with the schema(s) that were written
 * at file creation. To further configure the {@link AvroSource} to read with a user-defined schema,
 * or to return records of a type other than {@link GenericRecord}, use
 * {@link AvroSource#withSchema(Schema)} (using an Avro {@link Schema}),
 * {@link AvroSource#withSchema(String)} (using a JSON schema), or
 * {@link AvroSource#withSchema(Class)} (to return objects of the Avro-generated class specified).
 *
 * <p>An {@link AvroSource} can be read from using the {@link Read} transform. For example:
 *
 * <pre>
 * {@code
 * AvroSource<MyType> source = AvroSource.from(file.toPath()).withSchema(MyType.class);
 * PCollection<MyType> records = Read.from(mySource);
 * }
 * </pre>
 *
 * <p>This class's implementation is based on the <a
 * href="https://avro.apache.org/docs/1.7.7/spec.html">Avro 1.7.7</a> specification and implements
 * parsing of some parts of Avro Object Container Files. The rationale for doing so is that the Avro
 * API does not provide efficient ways of computing the precise offsets of blocks within a file,
 * which is necessary to support dynamic work rebalancing. However, whenever it is possible to use
 * the Avro API in a way that supports maintaining precise offsets, this class uses the Avro API.
 *
 * <p>Avro Object Container files store records in blocks. Each block contains a collection of
 * records. Blocks may be encoded (e.g., with bzip2, deflate, snappy, etc.). Blocks are delineated
 * from one another by a 16-byte sync marker.
 *
 * <p>An {@link AvroSource} for a subrange of a single file contains records in the blocks such that
 * the start offset of the block is greater than or equal to the start offset of the source and less
 * than the end offset of the source.
 *
 * <p>To use XZ-encoded Avro files, please include an explicit dependency on {@code xz-1.5.jar},
 * which has been marked as optional in the Maven {@code sdk/pom.xml}.
 *
 * <pre>{@code
 * <dependency>
 *   <groupId>org.tukaani</groupId>
 *   <artifactId>xz</artifactId>
 *   <version>1.5</version>
 * </dependency>
 * }</pre>
 *
 * <h3>Permissions</h3>
 *
 * <p>Permission requirements depend on the {@link PipelineRunner} that is used to execute the
 * pipeline. Please refer to the documentation of corresponding {@link PipelineRunner}s for
 * more details.
 *
 * @param <T> The type of records to be read from the source.
 */
// CHECKSTYLE.ON: JavadocStyle
@Experimental(Experimental.Kind.SOURCE_SINK)
public class AvroSource<T> extends BlockBasedSource<T> {
  // Default minimum bundle size (chosen as two default-size Avro blocks to attempt to
  // ensure that every source has at least one block of records).
  // The default sync interval is 64k.
  static final long DEFAULT_MIN_BUNDLE_SIZE = 2 * DataFileConstants.DEFAULT_SYNC_INTERVAL;

  // The JSON schema used to encode records.
  private final String readSchemaString;

  // The JSON schema that was used to write the source Avro file (may differ from the schema we will
  // use to read from it).
  private final String fileSchemaString;

  // The type of the records contained in the file.
  private final Class<T> type;

  // The following metadata fields are not user-configurable. They are extracted from the object
  // container file header upon subsource creation.

  // The codec used to encode the blocks in the Avro file. String value drawn from those in
  // https://avro.apache.org/docs/1.7.7/api/java/org/apache/avro/file/CodecFactory.html
  private final String codec;

  // The object container file's 16-byte sync marker.
  private final byte[] syncMarker;

  // Default output coder, lazily initialized.
  private transient AvroCoder<T> coder = null;

  // Schema of the file, lazily initialized.
  private transient Schema fileSchema;

  // Schema used to encode records, lazily initialized.
  private transient Schema readSchema;

  /**
   * Creates an {@link AvroSource} that reads from the given file name or pattern ("glob"). The
   * returned source can be further configured by calling {@link #withSchema} to return a type other
   * than {@link GenericRecord}.
   */
  public static AvroSource<GenericRecord> from(String fileNameOrPattern) {
    return new AvroSource<>(
        fileNameOrPattern, DEFAULT_MIN_BUNDLE_SIZE, null, GenericRecord.class, null, null);
  }

  /**
   * Returns an {@link AvroSource} that's like this one but reads files containing records that
   * conform to the given schema.
   *
   * <p>Does not modify this object.
   */
  public AvroSource<GenericRecord> withSchema(String schema) {
    return new AvroSource<>(
        getFileOrPatternSpec(), getMinBundleSize(), schema, GenericRecord.class, codec, syncMarker);
  }

  /**
   * Returns an {@link AvroSource} that's like this one but reads files containing records that
   * conform to the given schema.
   *
   * <p>Does not modify this object.
   */
  public AvroSource<GenericRecord> withSchema(Schema schema) {
    return new AvroSource<>(getFileOrPatternSpec(), getMinBundleSize(), schema.toString(),
        GenericRecord.class, codec, syncMarker);
  }

  /**
   * Returns an {@link AvroSource} that's like this one but reads files containing records of the
   * type of the given class.
   *
   * <p>Does not modify this object.
   */
  public <X> AvroSource<X> withSchema(Class<X> clazz) {
    return new AvroSource<X>(getFileOrPatternSpec(), getMinBundleSize(),
        ReflectData.get().getSchema(clazz).toString(), clazz, codec, syncMarker);
  }

  /**
   * Returns an {@link AvroSource} that's like this one but uses the supplied minimum bundle size.
   * Refer to {@link OffsetBasedSource} for a description of {@code minBundleSize} and its use.
   *
   * <p>Does not modify this object.
   */
  public AvroSource<T> withMinBundleSize(long minBundleSize) {
    return new AvroSource<>(
        getFileOrPatternSpec(), minBundleSize, readSchemaString, type, codec, syncMarker);
  }

  private AvroSource(String fileNameOrPattern, long minBundleSize, String schema, Class<T> type,
      String codec, byte[] syncMarker) {
    super(fileNameOrPattern, minBundleSize);
    this.readSchemaString = internSchemaString(schema);
    this.codec = codec;
    this.syncMarker = syncMarker;
    this.type = type;
    this.fileSchemaString = null;
  }

  private AvroSource(Metadata metadata, long minBundleSize, long startOffset, long endOffset,
      String schema, Class<T> type, String codec, byte[] syncMarker, String fileSchema) {
    super(metadata, minBundleSize, startOffset, endOffset);
    this.readSchemaString = internSchemaString(schema);
    this.codec = codec;
    this.syncMarker = syncMarker;
    this.type = type;
    this.fileSchemaString = internSchemaString(fileSchema);
  }

  @Override
  public void validate() {
    // AvroSource objects do not need to be configured with more than a file pattern. Overridden to
    // make this explicit.
    super.validate();
  }

  @Deprecated // Added to let DataflowRunner migrate off of this; to be deleted.
  public BlockBasedSource<T> createForSubrangeOfFile(String fileName, long start, long end)
      throws IOException {
    return createForSubrangeOfFile(FileSystems.matchSingleFileSpec(fileName), start, end);
  }

  @Override
  public BlockBasedSource<T> createForSubrangeOfFile(Metadata fileMetadata, long start, long end) {
    byte[] syncMarker = this.syncMarker;
    String codec = this.codec;
    String readSchemaString = this.readSchemaString;
    String fileSchemaString = this.fileSchemaString;
    // codec and syncMarker are initially null when the source is created, as they differ
    // across input files and must be read from the file. Here, when we are creating a source
    // for a subrange of a file, we can initialize these values. When the resulting AvroSource
    // is further split, they do not need to be read again.
    if (codec == null || syncMarker == null || fileSchemaString == null) {
      AvroMetadata metadata;
      try {
        metadata = readMetadataFromFile(fileMetadata.resourceId());
      } catch (IOException e) {
        throw new RuntimeException("Error reading metadata from file " + fileMetadata, e);
      }
      codec = metadata.getCodec();
      syncMarker = metadata.getSyncMarker();
      fileSchemaString = metadata.getSchemaString();
      // If the source was created with a null schema, use the schema that we read from the file's
      // metadata.
      if (readSchemaString == null) {
        readSchemaString = metadata.getSchemaString();
      }
    }
    // Note that if the fileSchemaString is equivalent to the readSchemaString, "intern"ing
    // the string will occur within the constructor and return the same reference as the
    // readSchemaString. This allows for Java to have an efficient serialization since it
    // will only encode the schema once while just storing pointers to the encoded version
    // within this source.
    return new AvroSource<>(fileMetadata, getMinBundleSize(), start, end, readSchemaString, type,
        codec, syncMarker, fileSchemaString);
  }

  @Override
  protected BlockBasedReader<T> createSingleFileReader(PipelineOptions options) {
    return new AvroReader<>(this);
  }

  @Override
  public AvroCoder<T> getDefaultOutputCoder() {
    if (coder == null) {
      coder = AvroCoder.of(type, internOrParseSchemaString(readSchemaString));
    }
    return coder;
  }

  public String getSchema() {
    return readSchemaString;
  }

  @VisibleForTesting
  Schema getReadSchema() {
    if (readSchemaString == null) {
      return null;
    }

    // If the schema has not been parsed, parse it.
    if (readSchema == null) {
      readSchema = internOrParseSchemaString(readSchemaString);
    }
    return readSchema;
  }

  @VisibleForTesting
  Schema getFileSchema() {
    if (fileSchemaString == null) {
      return null;
    }

    // If the schema has not been parsed, parse it.
    if (fileSchema == null) {
      fileSchema = internOrParseSchemaString(fileSchemaString);
    }
    return fileSchema;
  }

  private byte[] getSyncMarker() {
    return syncMarker;
  }

  private String getCodec() {
    return codec;
  }

  /**
   * Avro file metadata.
   */
  @VisibleForTesting
  static class AvroMetadata {
    private byte[] syncMarker;
    private String codec;
    private String schemaString;

    AvroMetadata(byte[] syncMarker, String codec, String schemaString) {
      this.syncMarker = checkNotNull(syncMarker, "syncMarker");
      this.codec = checkNotNull(codec, "codec");
      this.schemaString = checkNotNull(schemaString, "schemaString");
    }

    /**
     * The JSON-encoded <a href="https://avro.apache.org/docs/1.7.7/spec.html#schemas">schema</a>
     * string for the file.
     */
    public String getSchemaString() {
      return schemaString;
    }

    /**
     * The <a href="https://avro.apache.org/docs/1.7.7/spec.html#Required+Codecs">codec</a> of the
     * file.
     */
    public String getCodec() {
      return codec;
    }

    /**
     * The 16-byte sync marker for the file.  See the documentation for
     * <a href="https://avro.apache.org/docs/1.7.7/spec.html#Object+Container+Files">Object
     * Container File</a> for more information.
     */
    public byte[] getSyncMarker() {
      return syncMarker;
    }
  }

  /**
   * Reads the {@link AvroMetadata} from the header of an Avro file.
   *
   * <p>This method parses the header of an Avro
   * <a href="https://avro.apache.org/docs/1.7.7/spec.html#Object+Container+Files">
   * Object Container File</a>.
   *
   * @throws IOException if the file is an invalid format.
   */
  @VisibleForTesting
  static AvroMetadata readMetadataFromFile(ResourceId fileResource) throws IOException {
    String codec = null;
    String schemaString = null;
    byte[] syncMarker;
    try (InputStream stream = Channels.newInputStream(FileSystems.open(fileResource))) {
      BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(stream, null);

      // The header of an object container file begins with a four-byte magic number, followed
      // by the file metadata (including the schema and codec), encoded as a map. Finally, the
      // header ends with the file's 16-byte sync marker.
      // See https://avro.apache.org/docs/1.7.7/spec.html#Object+Container+Files for details on
      // the encoding of container files.

      // Read the magic number.
      byte[] magic = new byte[DataFileConstants.MAGIC.length];
      decoder.readFixed(magic);
      if (!Arrays.equals(magic, DataFileConstants.MAGIC)) {
        throw new IOException("Missing Avro file signature: " + fileResource);
      }

      // Read the metadata to find the codec and schema.
      ByteBuffer valueBuffer = ByteBuffer.allocate(512);
      long numRecords = decoder.readMapStart();
      while (numRecords > 0) {
        for (long recordIndex = 0; recordIndex < numRecords; recordIndex++) {
          String key = decoder.readString();
          // readBytes() clears the buffer and returns a buffer where:
          // - position is the start of the bytes read
          // - limit is the end of the bytes read
          valueBuffer = decoder.readBytes(valueBuffer);
          byte[] bytes = new byte[valueBuffer.remaining()];
          valueBuffer.get(bytes);
          if (key.equals(DataFileConstants.CODEC)) {
            codec = new String(bytes, "UTF-8");
          } else if (key.equals(DataFileConstants.SCHEMA)) {
            schemaString = new String(bytes, "UTF-8");
          }
        }
        numRecords = decoder.mapNext();
      }
      if (codec == null) {
        codec = DataFileConstants.NULL_CODEC;
      }

      // Finally, read the sync marker.
      syncMarker = new byte[DataFileConstants.SYNC_SIZE];
      decoder.readFixed(syncMarker);
    }
    checkState(schemaString != null, "No schema present in Avro file metadata %s", fileResource);
    return new AvroMetadata(syncMarker, codec, schemaString);
  }

  private DatumReader<T> createDatumReader() {
    Schema readSchema = getReadSchema();
    Schema fileSchema = getFileSchema();
    checkNotNull(readSchema, "No read schema has been initialized for source %s", this);
    checkNotNull(fileSchema, "No file schema has been initialized for source %s", this);
    if (type == GenericRecord.class) {
      return new GenericDatumReader<>(fileSchema, readSchema);
    } else {
      return new ReflectDatumReader<>(fileSchema, readSchema);
    }
  }

  // A logical reference cache used to store schemas and schema strings to allow us to
  // "intern" values and reduce the number of copies of equivalent objects.
  private static final Map<String, Schema> schemaLogicalReferenceCache = new WeakHashMap<>();
  private static final Map<String, String> schemaStringLogicalReferenceCache = new WeakHashMap<>();

  // We avoid String.intern() because depending on the JVM, these may be added to the PermGenSpace
  // which we want to avoid otherwise we could run out of PermGenSpace.
  private static synchronized String internSchemaString(String schema) {
    String internSchema = schemaStringLogicalReferenceCache.get(schema);
    if (internSchema != null) {
      return internSchema;
    }
    schemaStringLogicalReferenceCache.put(schema, schema);
    return schema;
  }

  private static synchronized Schema internOrParseSchemaString(String schemaString) {
    Schema schema = schemaLogicalReferenceCache.get(schemaString);
    if (schema != null) {
      return schema;
    }
    Schema.Parser parser = new Schema.Parser();
    schema = parser.parse(schemaString);
    schemaLogicalReferenceCache.put(schemaString, schema);
    return schema;
  }

  // Reading the object from Java serialization typically does not go through the constructor,
  // we use readResolve to replace the constructed instance with one which uses the constructor
  // allowing us to intern any schemas.
  @SuppressWarnings("unused")
  private Object readResolve() throws ObjectStreamException {
    switch (getMode()) {
      case SINGLE_FILE_OR_SUBRANGE:
        return new AvroSource<>(
            getSingleFileMetadata(),
            getMinBundleSize(),
            getStartOffset(),
            getEndOffset(),
            readSchemaString,
            type,
            codec,
            syncMarker,
            fileSchemaString);
      case FILEPATTERN:
        return new AvroSource<>(
            getFileOrPatternSpec(),
            getMinBundleSize(),
            readSchemaString,
            type,
            codec,
            syncMarker);
        default:
          throw new InvalidObjectException(
              String.format("Unknown mode %s for AvroSource %s", getMode(), this));
    }
  }

  /**
   * A {@link BlockBasedSource.Block} of Avro records.
   *
   * @param <T> The type of records stored in the block.
   */
  @Experimental(Experimental.Kind.SOURCE_SINK)
  static class AvroBlock<T> extends Block<T> {
    // The number of records in the block.
    private final long numRecords;

    // The current record in the block.
    private T currentRecord;

    // The index of the current record in the block.
    private long currentRecordIndex = 0;

    // A DatumReader to read records from the block.
    private final DatumReader<T> reader;

    // A BinaryDecoder used by the reader to decode records.
    private final BinaryDecoder decoder;

    /**
     * Decodes a byte array as an InputStream. The byte array may be compressed using some
     * codec. Reads from the returned stream will result in decompressed bytes.
     *
     * <p>This supports the same codecs as Avro's {@link CodecFactory}, namely those defined in
     * {@link DataFileConstants}.
     *
     * <ul>
     * <li>"snappy" : Google's Snappy compression
     * <li>"deflate" : deflate compression
     * <li>"bzip2" : Bzip2 compression
     * <li>"xz" : xz compression
     * <li>"null" (the string, not the value): Uncompressed data
     * </ul>
     */
    private static InputStream decodeAsInputStream(byte[] data, String codec) throws IOException {
      ByteArrayInputStream byteStream = new ByteArrayInputStream(data);
      switch (codec) {
        case DataFileConstants.SNAPPY_CODEC:
          return new SnappyCompressorInputStream(byteStream, 1 << 16 /* Avro uses 64KB blocks */);
        case DataFileConstants.DEFLATE_CODEC:
          // nowrap == true: Do not expect ZLIB header or checksum, as Avro does not write them.
          Inflater inflater = new Inflater(true);
          return new InflaterInputStream(byteStream, inflater);
        case DataFileConstants.XZ_CODEC:
          return new XZCompressorInputStream(byteStream);
        case DataFileConstants.BZIP2_CODEC:
          return new BZip2CompressorInputStream(byteStream);
        case DataFileConstants.NULL_CODEC:
          return byteStream;
        default:
          throw new IllegalArgumentException("Unsupported codec: " + codec);
      }
    }

    AvroBlock(byte[] data, long numRecords, AvroSource<T> source) throws IOException {
      this.numRecords = numRecords;
      this.reader = source.createDatumReader();
      this.decoder =
          DecoderFactory.get().binaryDecoder(decodeAsInputStream(data, source.getCodec()), null);
    }

    @Override
    public T getCurrentRecord() {
      return currentRecord;
    }

    @Override
    public boolean readNextRecord() throws IOException {
      if (currentRecordIndex >= numRecords) {
        return false;
      }
      currentRecord = reader.read(null, decoder);
      currentRecordIndex++;
      return true;
    }

    @Override
    public double getFractionOfBlockConsumed() {
      return ((double) currentRecordIndex) / numRecords;
    }
  }

  /**
   * A {@link BlockBasedSource.BlockBasedReader} for reading blocks from Avro files.
   *
   * <p>An Avro Object Container File consists of a header followed by a 16-bit sync marker
   * and then a sequence of blocks, where each block begins with two encoded longs representing
   * the total number of records in the block and the block's size in bytes, followed by the
   * block's (optionally-encoded) records. Each block is terminated by a 16-bit sync marker.
   *
   * @param <T> The type of records contained in the block.
   */
  @Experimental(Experimental.Kind.SOURCE_SINK)
  public static class AvroReader<T> extends BlockBasedReader<T> {
    // The current block.
    private AvroBlock<T> currentBlock;

    // A lock used to synchronize block offsets for getRemainingParallelism
    private final Object progressLock = new Object();

    // Offset of the current block.
    @GuardedBy("progressLock")
    private long currentBlockOffset = 0;

    // Size of the current block.
    @GuardedBy("progressLock")
    private long currentBlockSizeBytes = 0;

    // Stream used to read from the underlying file.
    // A pushback stream is used to restore bytes buffered during seeking.
    private PushbackInputStream stream;
    // Counts the number of bytes read. Used only to tell how many bytes are taken up in
    // a block's variable-length header.
    private CountingInputStream countStream;

    // Caches the Avro DirectBinaryDecoder used to decode binary-encoded values from the buffer.
    private BinaryDecoder decoder;

    /**
     * Reads Avro records of type {@code T} from the specified source.
     */
    public AvroReader(AvroSource<T> source) {
      super(source);
    }

    @Override
    public synchronized AvroSource<T> getCurrentSource() {
      return (AvroSource<T>) super.getCurrentSource();
    }

    // Precondition: the stream is positioned after the sync marker in the current (about to be
    // previous) block. currentBlockSize equals the size of the current block, or zero if this
    // reader was just started.
    //
    // Postcondition: same as above, but for the new current (formerly next) block.
    @Override
    public boolean readNextBlock() throws IOException {
      long startOfNextBlock;
      synchronized (progressLock) {
        startOfNextBlock = currentBlockOffset + currentBlockSizeBytes;
      }

      // Before reading the variable-sized block header, record the current number of bytes read.
      long preHeaderCount = countStream.getBytesRead();
      decoder = DecoderFactory.get().directBinaryDecoder(countStream, decoder);
      long numRecords;
      try {
        numRecords = decoder.readLong();
      } catch (EOFException e) {
        // Expected for the last block, at which the start position is the EOF. The way to detect
        // stream ending is to try reading from it.
        return false;
      }
      long blockSize = decoder.readLong();

      // Mark header size as the change in the number of bytes read.
      long headerSize = countStream.getBytesRead() - preHeaderCount;

      // Create the current block by reading blockSize bytes. Block sizes permitted by the Avro
      // specification are [32, 2^30], so this narrowing is ok.
      byte[] data = new byte[(int) blockSize];
      int read = stream.read(data);
      checkState(blockSize == read, "Only %s/%s bytes in the block were read", read, blockSize);
      currentBlock = new AvroBlock<>(data, numRecords, getCurrentSource());

      // Read the end of this block, which MUST be a sync marker for correctness.
      byte[] syncMarker = getCurrentSource().getSyncMarker();
      byte[] readSyncMarker = new byte[syncMarker.length];
      long syncMarkerOffset = startOfNextBlock + headerSize + blockSize;
      long bytesRead = stream.read(readSyncMarker);
      checkState(
          bytesRead == syncMarker.length,
          "When trying to read a sync marker at position %s, only able to read %s/%s bytes",
          syncMarkerOffset,
          bytesRead,
          syncMarker.length);
      if (!Arrays.equals(syncMarker, readSyncMarker)) {
        throw new IllegalStateException(
            String.format(
                "Expected the bytes [%d,%d) in file %s to be a sync marker, but found %s",
                syncMarkerOffset,
                syncMarkerOffset + syncMarker.length,
                getCurrentSource().getFileOrPatternSpec(),
                Arrays.toString(readSyncMarker)
            ));
      }

      // Atomically update both the position and offset of the new block.
      synchronized (progressLock) {
        currentBlockOffset = startOfNextBlock;
        // Total block size includes the header, block content, and trailing sync marker.
        currentBlockSizeBytes = headerSize + blockSize + syncMarker.length;
      }

      return true;
    }

    @Override
    public AvroBlock<T> getCurrentBlock() {
      return currentBlock;
    }

    @Override
    public long getCurrentBlockOffset() {
      synchronized (progressLock) {
        return currentBlockOffset;
      }
    }

    @Override
    public long getCurrentBlockSize() {
      synchronized (progressLock) {
        return currentBlockSizeBytes;
      }
    }

    @Override
    public long getSplitPointsRemaining() {
      if (isDone()) {
        return 0;
      }
      synchronized (progressLock) {
        if (currentBlockOffset + currentBlockSizeBytes >= getCurrentSource().getEndOffset()) {
          // This block is known to be the last block in the range.
          return 1;
        }
      }
      return super.getSplitPointsRemaining();
    }

    /**
     * Creates a {@link PushbackInputStream} that has a large enough pushback buffer to be able
     * to push back the syncBuffer.
     */
    private PushbackInputStream createStream(ReadableByteChannel channel) {
      return new PushbackInputStream(
          Channels.newInputStream(channel),
          getCurrentSource().getSyncMarker().length);
    }

    // Postcondition: the stream is positioned at the beginning of the first block after the start
    // of the current source, and currentBlockOffset is that position. Additionally,
    // currentBlockSizeBytes will be set to 0 indicating that the previous block was empty.
    @Override
    protected void startReading(ReadableByteChannel channel) throws IOException {
      long startOffset = getCurrentSource().getStartOffset();
      byte[] syncMarker = getCurrentSource().getSyncMarker();
      long syncMarkerLength = syncMarker.length;

      if (startOffset != 0) {
        // Rewind order to find the sync marker ending the previous block.
        long position = Math.max(0, startOffset - syncMarkerLength);
        ((SeekableByteChannel) channel).position(position);
        startOffset = position;
      }

      // Satisfy the post condition.
      stream = createStream(channel);
      countStream = new CountingInputStream(stream);
      synchronized (progressLock) {
        currentBlockOffset = startOffset + advancePastNextSyncMarker(stream, syncMarker);
        currentBlockSizeBytes = 0;
      }
    }

    /**
     * Advances to the first byte after the next occurrence of the sync marker in the
     * stream when reading from the current offset. Returns the number of bytes consumed
     * from the stream. Note that this method requires a PushbackInputStream with a buffer
     * at least as big as the marker it is seeking for.
     */
    static long advancePastNextSyncMarker(PushbackInputStream stream, byte[] syncMarker)
        throws IOException {
      Seeker seeker = new Seeker(syncMarker);
      byte[] syncBuffer = new byte[syncMarker.length];
      long totalBytesConsumed = 0;
      // Seek until either a sync marker is found or we reach the end of the file.
      int mark = -1; // Position of the last byte in the sync marker.
      int read; // Number of bytes read.
      do {
        read = stream.read(syncBuffer);
        if (read >= 0) {
          mark = seeker.find(syncBuffer, read);
          // Update the currentOffset with the number of bytes read.
          totalBytesConsumed += read;
        }
      } while (mark < 0 && read > 0);

      // If the sync marker was found, unread block data and update the current offsets.
      if (mark >= 0) {
        // The current offset after this call should be just past the sync marker, so we should
        // unread the remaining buffer contents and update the currentOffset accordingly.
        stream.unread(syncBuffer, mark + 1, read - (mark + 1));
        totalBytesConsumed = totalBytesConsumed - (read - (mark + 1));
      }
      return totalBytesConsumed;
    }

    /**
     * A {@link Seeker} looks for a given marker within a byte buffer. Uses naive string matching
     * with a sliding window, as sync markers are small and random.
     */
    static class Seeker {
      // The marker to search for.
      private byte[] marker;

      // Buffer used for the sliding window.
      private byte[] searchBuffer;

      // Number of bytes available to be matched in the buffer.
      private int available = 0;

      /**
       * Create a {@link Seeker} that looks for the given marker.
       */
      public Seeker(byte[] marker) {
        this.marker = marker;
        this.searchBuffer = new byte[marker.length];
      }

      /**
       * Find the marker in the byte buffer. Returns the index of the end of the marker in the
       * buffer. If the marker is not found, returns -1.
       *
       * <p>State is maintained between calls. If the marker was partially matched, a subsequent
       * call to find will resume matching the marker.
       *
       * @param buffer
       * @return the index of the end of the marker within the buffer, or -1 if the buffer was not
       * found.
       */
      public int find(byte[] buffer, int length) {
        for (int i = 0; i < length; i++) {
          System.arraycopy(searchBuffer, 1, searchBuffer, 0, searchBuffer.length - 1);
          searchBuffer[searchBuffer.length - 1] = buffer[i];
          available = Math.min(available + 1, searchBuffer.length);
          if (ByteBuffer.wrap(searchBuffer, searchBuffer.length - available, available)
                  .equals(ByteBuffer.wrap(marker))) {
            available = 0;
            return i;
          }
        }
        return -1;
      }
    }
  }
}
