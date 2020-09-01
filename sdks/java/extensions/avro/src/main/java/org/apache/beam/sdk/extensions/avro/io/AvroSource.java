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
package org.apache.beam.sdk.extensions.avro.io;

import static org.apache.beam.sdk.io.FileBasedSource.Mode.SINGLE_FILE_OR_SUBRANGE;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.ObjectStreamException;
import java.io.PushbackInputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
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
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.BlockBasedSource;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.snappy.SnappyCompressorInputStream;
import org.apache.commons.compress.compressors.xz.XZCompressorInputStream;
import org.apache.commons.compress.utils.CountingInputStream;
import org.apache.commons.compress.utils.IOUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

// CHECKSTYLE.OFF: JavadocStyle
/**
 * Do not use in pipelines directly: most users should use {@link AvroIO.Read}.
 *
 * <p>A {@link FileBasedSource} for reading Avro files.
 *
 * <p>To read a {@link PCollection} of objects from one or more Avro files, use {@link
 * AvroSource#from} to specify the path(s) of the files to read. The {@link AvroSource} that is
 * returned will read objects of type {@link GenericRecord} with the schema(s) that were written at
 * file creation. To further configure the {@link AvroSource} to read with a user-defined schema, or
 * to return records of a type other than {@link GenericRecord}, use {@link
 * AvroSource#withSchema(Schema)} (using an Avro {@link Schema}), {@link
 * AvroSource#withSchema(String)} (using a JSON schema), or {@link AvroSource#withSchema(Class)} (to
 * return objects of the Avro-generated class specified).
 *
 * <p>An {@link AvroSource} can be read from using the {@link Read} transform. For example:
 *
 * <pre>{@code
 * AvroSource<MyType> source = AvroSource.from(file.toPath()).withSchema(MyType.class);
 * PCollection<MyType> records = Read.from(mySource);
 * }</pre>
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
 * <p>To use XZ-encoded Avro files, please include an explicit dependency on {@code xz-1.8.jar},
 * which has been marked as optional in the Maven {@code sdk/pom.xml}.
 *
 * <pre>{@code
 * <dependency>
 *   <groupId>org.tukaani</groupId>
 *   <artifactId>xz</artifactId>
 *   <version>1.8</version>
 * </dependency>
 * }</pre>
 *
 * <h3>Permissions</h3>
 *
 * <p>Permission requirements depend on the {@link PipelineRunner} that is used to execute the
 * pipeline. Please refer to the documentation of corresponding {@link PipelineRunner}s for more
 * details.
 *
 * @param <T> The type of records to be read from the source.
 */
// CHECKSTYLE.ON: JavadocStyle
@Experimental(Kind.SOURCE_SINK)
public class AvroSource<T> extends BlockBasedSource<T> {
  // Default minimum bundle size (chosen as two default-size Avro blocks to attempt to
  // ensure that every source has at least one block of records).
  // The default sync interval is 64k.
  private static final long DEFAULT_MIN_BUNDLE_SIZE = 2L * DataFileConstants.DEFAULT_SYNC_INTERVAL;

  @FunctionalInterface
  public interface DatumReaderFactory<T> extends Serializable {
    DatumReader<T> apply(Schema writer, Schema reader);
  }

  private static final DatumReaderFactory<?> GENERIC_DATUM_READER_FACTORY = GenericDatumReader::new;

  private static final DatumReaderFactory<?> REFLECT_DATUM_READER_FACTORY = ReflectDatumReader::new;

  // Use cases of AvroSource are:
  // 1) AvroSource<GenericRecord> Reading GenericRecord records with a specified schema.
  // 2) AvroSource<Foo> Reading records of a generated Avro class Foo.
  // 3) AvroSource<T> Reading GenericRecord records with an unspecified schema
  //    and converting them to type T.
  //                     |    Case 1     |    Case 2   |     Case 3    |
  // type                | GenericRecord |     Foo     | GenericRecord |
  // readerSchemaString  |    non-null   |   non-null  |     null      |
  // parseFn             |      null     |     null    |   non-null    |
  // outputCoder         |      null     |     null    |   non-null    |
  // readerFactory       |     either    |    either   |    either     |
  private static class Mode<T> implements Serializable {
    private final Class<?> type;

    // The JSON schema used to decode records.
    private @Nullable String readerSchemaString;

    private final @Nullable SerializableFunction<GenericRecord, T> parseFn;

    private final @Nullable Coder<T> outputCoder;

    private final @Nullable DatumReaderFactory<?> readerFactory;

    private Mode(
        Class<?> type,
        @Nullable String readerSchemaString,
        @Nullable SerializableFunction<GenericRecord, T> parseFn,
        @Nullable Coder<T> outputCoder,
        @Nullable DatumReaderFactory<?> readerFactory) {
      this.type = type;
      this.readerSchemaString = internSchemaString(readerSchemaString);
      this.parseFn = parseFn;
      this.outputCoder = outputCoder;
      this.readerFactory = readerFactory;
    }

    private void readObject(ObjectInputStream is) throws IOException, ClassNotFoundException {
      is.defaultReadObject();
      readerSchemaString = internSchemaString(readerSchemaString);
    }

    private Coder<T> getOutputCoder() {
      if (parseFn == null) {
        return AvroCoder.of((Class<T>) type, internOrParseSchemaString(readerSchemaString));
      } else {
        return outputCoder;
      }
    }

    private void validate() {
      if (parseFn == null) {
        checkArgument(
            readerSchemaString != null,
            "schema must be specified using withSchema() when not using a parse fn");
      }
    }

    private Mode<T> withReaderFactory(DatumReaderFactory<?> factory) {
      return new Mode<>(type, readerSchemaString, parseFn, outputCoder, factory);
    }

    private DatumReader<?> createReader(Schema writerSchema, Schema readerSchema) {
      DatumReaderFactory<?> factory = this.readerFactory;
      if (factory == null) {
        factory =
            (type == GenericRecord.class)
                ? GENERIC_DATUM_READER_FACTORY
                : REFLECT_DATUM_READER_FACTORY;
      }
      return factory.apply(writerSchema, readerSchema);
    }
  }

  private static Mode<GenericRecord> readGenericRecordsWithSchema(
      String schema, @Nullable DatumReaderFactory<?> factory) {
    return new Mode<>(GenericRecord.class, schema, null, null, factory);
  }

  private static <T> Mode<T> readGeneratedClasses(
      Class<T> clazz, @Nullable DatumReaderFactory<?> factory) {
    return new Mode<>(clazz, ReflectData.get().getSchema(clazz).toString(), null, null, factory);
  }

  private static <T> Mode<T> parseGenericRecords(
      SerializableFunction<GenericRecord, T> parseFn,
      Coder<T> outputCoder,
      @Nullable DatumReaderFactory<?> factory) {
    return new Mode<>(GenericRecord.class, null, parseFn, outputCoder, factory);
  }

  private final Mode<T> mode;

  /**
   * Reads from the given file name or pattern ("glob"). The returned source needs to be further
   * configured by calling {@link #withSchema} to return a type other than {@link GenericRecord}.
   */
  public static AvroSource<GenericRecord> from(ValueProvider<String> fileNameOrPattern) {
    return new AvroSource<>(
        fileNameOrPattern,
        EmptyMatchTreatment.DISALLOW,
        DEFAULT_MIN_BUNDLE_SIZE,
        readGenericRecordsWithSchema(null /* will need to be specified in withSchema */, null));
  }

  public static AvroSource<GenericRecord> from(Metadata metadata) {
    return new AvroSource<>(
        metadata,
        DEFAULT_MIN_BUNDLE_SIZE,
        0,
        metadata.sizeBytes(),
        readGenericRecordsWithSchema(null /* will need to be specified in withSchema */, null));
  }

  /** Like {@link #from(ValueProvider)}. */
  public static AvroSource<GenericRecord> from(String fileNameOrPattern) {
    return from(ValueProvider.StaticValueProvider.of(fileNameOrPattern));
  }

  public AvroSource<T> withEmptyMatchTreatment(EmptyMatchTreatment emptyMatchTreatment) {
    return new AvroSource<>(
        getFileOrPatternSpecProvider(), emptyMatchTreatment, getMinBundleSize(), mode);
  }

  /** Reads files containing records that conform to the given schema. */
  public AvroSource<GenericRecord> withSchema(String schema) {
    checkArgument(schema != null, "schema can not be null");
    return new AvroSource<>(
        getFileOrPatternSpecProvider(),
        getEmptyMatchTreatment(),
        getMinBundleSize(),
        readGenericRecordsWithSchema(schema, mode.readerFactory));
  }

  /** Like {@link #withSchema(String)}. */
  public AvroSource<GenericRecord> withSchema(Schema schema) {
    checkArgument(schema != null, "schema can not be null");
    return withSchema(schema.toString());
  }

  /** Reads files containing records of the given class. */
  public <X> AvroSource<X> withSchema(Class<X> clazz) {
    checkArgument(clazz != null, "clazz can not be null");
    if (getMode() == SINGLE_FILE_OR_SUBRANGE) {
      return new AvroSource<>(
          getSingleFileMetadata(),
          getMinBundleSize(),
          getStartOffset(),
          getEndOffset(),
          readGeneratedClasses(clazz, mode.readerFactory));
    }
    return new AvroSource<>(
        getFileOrPatternSpecProvider(),
        getEmptyMatchTreatment(),
        getMinBundleSize(),
        readGeneratedClasses(clazz, mode.readerFactory));
  }

  /**
   * Reads {@link GenericRecord} of unspecified schema and maps them to instances of a custom type
   * using the given {@code parseFn} and encoded using the given coder.
   */
  public <X> AvroSource<X> withParseFn(
      SerializableFunction<GenericRecord, X> parseFn, Coder<X> coder) {
    checkArgument(parseFn != null, "parseFn can not be null");
    checkArgument(coder != null, "coder can not be null");
    if (getMode() == SINGLE_FILE_OR_SUBRANGE) {
      return new AvroSource<>(
          getSingleFileMetadata(),
          getMinBundleSize(),
          getStartOffset(),
          getEndOffset(),
          parseGenericRecords(parseFn, coder, mode.readerFactory));
    }
    return new AvroSource<>(
        getFileOrPatternSpecProvider(),
        getEmptyMatchTreatment(),
        getMinBundleSize(),
        parseGenericRecords(parseFn, coder, mode.readerFactory));
  }

  /**
   * Sets the minimum bundle size. Refer to {@link OffsetBasedSource} for a description of {@code
   * minBundleSize} and its use.
   */
  public AvroSource<T> withMinBundleSize(long minBundleSize) {
    if (getMode() == SINGLE_FILE_OR_SUBRANGE) {
      return new AvroSource<>(
          getSingleFileMetadata(), minBundleSize, getStartOffset(), getEndOffset(), mode);
    }
    return new AvroSource<>(
        getFileOrPatternSpecProvider(), getEmptyMatchTreatment(), minBundleSize, mode);
  }

  public AvroSource<T> withDatumReaderFactory(DatumReaderFactory<?> factory) {
    Mode<T> newMode = mode.withReaderFactory(factory);
    if (getMode() == SINGLE_FILE_OR_SUBRANGE) {
      return new AvroSource<>(
          getSingleFileMetadata(), getMinBundleSize(), getStartOffset(), getEndOffset(), newMode);
    }
    return new AvroSource<>(
        getFileOrPatternSpecProvider(), getEmptyMatchTreatment(), getMinBundleSize(), newMode);
  }

  /** Constructor for FILEPATTERN mode. */
  private AvroSource(
      ValueProvider<String> fileNameOrPattern,
      EmptyMatchTreatment emptyMatchTreatment,
      long minBundleSize,
      Mode<T> mode) {
    super(fileNameOrPattern, emptyMatchTreatment, minBundleSize);
    this.mode = mode;
  }

  /** Constructor for SINGLE_FILE_OR_SUBRANGE mode. */
  private AvroSource(
      Metadata metadata, long minBundleSize, long startOffset, long endOffset, Mode<T> mode) {
    super(metadata, minBundleSize, startOffset, endOffset);
    this.mode = mode;
  }

  @Override
  public void validate() {
    super.validate();
    mode.validate();
  }

  /**
   * Used by the Dataflow worker. Do not introduce new usages. Do not delete without confirming that
   * Dataflow ValidatesRunner tests pass.
   *
   * @deprecated Used by Dataflow worker
   */
  @Deprecated
  public BlockBasedSource<T> createForSubrangeOfFile(String fileName, long start, long end)
      throws IOException {
    return createForSubrangeOfFile(FileSystems.matchSingleFileSpec(fileName), start, end);
  }

  @Override
  public BlockBasedSource<T> createForSubrangeOfFile(Metadata fileMetadata, long start, long end) {
    return new AvroSource<>(fileMetadata, getMinBundleSize(), start, end, mode);
  }

  @Override
  protected BlockBasedReader<T> createSingleFileReader(PipelineOptions options) {
    return new AvroReader<>(this);
  }

  @Override
  public Coder<T> getOutputCoder() {
    return mode.getOutputCoder();
  }

  @VisibleForTesting
  @Nullable
  String getReaderSchemaString() {
    return mode.readerSchemaString;
  }

  /** Avro file metadata. */
  @VisibleForTesting
  static class AvroMetadata {
    private final byte[] syncMarker;
    private final String codec;
    private final String schemaString;

    AvroMetadata(byte[] syncMarker, String codec, String schemaString) {
      this.syncMarker = checkNotNull(syncMarker, "syncMarker");
      this.codec = checkNotNull(codec, "codec");
      this.schemaString = internSchemaString(checkNotNull(schemaString, "schemaString"));
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
     * The 16-byte sync marker for the file. See the documentation for <a
     * href="https://avro.apache.org/docs/1.7.7/spec.html#Object+Container+Files">Object Container
     * File</a> for more information.
     */
    public byte[] getSyncMarker() {
      return syncMarker;
    }
  }

  /**
   * Reads the {@link AvroMetadata} from the header of an Avro file.
   *
   * <p>This method parses the header of an Avro <a
   * href="https://avro.apache.org/docs/1.7.7/spec.html#Object+Container+Files">Object Container
   * File</a>.
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
            codec = new String(bytes, StandardCharsets.UTF_8);
          } else if (key.equals(DataFileConstants.SCHEMA)) {
            schemaString = new String(bytes, StandardCharsets.UTF_8);
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
            getSingleFileMetadata(), getMinBundleSize(), getStartOffset(), getEndOffset(), mode);
      case FILEPATTERN:
        return new AvroSource<>(
            getFileOrPatternSpecProvider(), getEmptyMatchTreatment(), getMinBundleSize(), mode);
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
  @Experimental(Kind.SOURCE_SINK)
  static class AvroBlock<T> extends Block<T> {
    private final Mode<T> mode;

    // The number of records in the block.
    private final long numRecords;

    // The current record in the block. Initialized in readNextRecord.
    private @Nullable T currentRecord;

    // The index of the current record in the block.
    private long currentRecordIndex = 0;

    // A DatumReader to read records from the block.
    private final DatumReader<?> reader;

    // A BinaryDecoder used by the reader to decode records.
    private final BinaryDecoder decoder;

    /**
     * Decodes a byte array as an InputStream. The byte array may be compressed using some codec.
     * Reads from the returned stream will result in decompressed bytes.
     *
     * <p>This supports the same codecs as Avro's {@link CodecFactory}, namely those defined in
     * {@link DataFileConstants}.
     *
     * <ul>
     *   <li>"snappy" : Google's Snappy compression
     *   <li>"deflate" : deflate compression
     *   <li>"bzip2" : Bzip2 compression
     *   <li>"xz" : xz compression
     *   <li>"null" (the string, not the value): Uncompressed data
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

    AvroBlock(byte[] data, long numRecords, Mode<T> mode, String writerSchemaString, String codec)
        throws IOException {
      this.mode = mode;
      this.numRecords = numRecords;
      checkNotNull(writerSchemaString, "writerSchemaString");
      Schema writerSchema = internOrParseSchemaString(writerSchemaString);
      Schema readerSchema =
          internOrParseSchemaString(
              MoreObjects.firstNonNull(mode.readerSchemaString, writerSchemaString));

      this.reader = mode.createReader(writerSchema, readerSchema);

      if (codec.equals(DataFileConstants.NULL_CODEC)) {
        // Avro can read from a byte[] using a more efficient implementation.  If the input is not
        // compressed, pass the data in directly.
        this.decoder = DecoderFactory.get().binaryDecoder(data, null);
      } else {
        this.decoder = DecoderFactory.get().binaryDecoder(decodeAsInputStream(data, codec), null);
      }
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
      Object record = reader.read(null, decoder);
      currentRecord =
          (mode.parseFn == null) ? ((T) record) : mode.parseFn.apply((GenericRecord) record);
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
   * <p>An Avro Object Container File consists of a header followed by a 16-bit sync marker and then
   * a sequence of blocks, where each block begins with two encoded longs representing the total
   * number of records in the block and the block's size in bytes, followed by the block's
   * (optionally-encoded) records. Each block is terminated by a 16-bit sync marker.
   *
   * @param <T> The type of records contained in the block.
   */
  @Experimental(Kind.SOURCE_SINK)
  public static class AvroReader<T> extends BlockBasedReader<T> {
    // Initialized in startReading.
    private @Nullable AvroMetadata metadata;

    // The current block.
    // Initialized in readNextRecord.
    private @Nullable AvroBlock<T> currentBlock;

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
    // Initialized in startReading.
    private @Nullable PushbackInputStream stream;

    // Counts the number of bytes read. Used only to tell how many bytes are taken up in
    // a block's variable-length header.
    // Initialized in startReading.
    private @Nullable CountingInputStream countStream;

    // Caches the Avro DirectBinaryDecoder used to decode binary-encoded values from the buffer.
    // Initialized in readNextBlock.
    private @Nullable BinaryDecoder decoder;

    /** Reads Avro records of type {@code T} from the specified source. */
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
      // specification are [32, 2^30], so the cast is safe.
      byte[] data = new byte[(int) blockSize];
      int bytesRead = IOUtils.readFully(stream, data);
      checkState(
          blockSize == bytesRead,
          "Only able to read %s/%s bytes in the block before EOF reached.",
          bytesRead,
          blockSize);
      currentBlock =
          new AvroBlock<>(
              data,
              numRecords,
              getCurrentSource().mode,
              metadata.getSchemaString(),
              metadata.getCodec());

      // Read the end of this block, which MUST be a sync marker for correctness.
      byte[] syncMarker = metadata.getSyncMarker();
      byte[] readSyncMarker = new byte[syncMarker.length];
      long syncMarkerOffset = startOfNextBlock + headerSize + blockSize;
      bytesRead = IOUtils.readFully(stream, readSyncMarker);
      checkState(
          bytesRead == syncMarker.length,
          "Only able to read %s/%s bytes of Avro sync marker at position %s before EOF reached.",
          bytesRead,
          syncMarker.length,
          syncMarkerOffset);
      if (!Arrays.equals(syncMarker, readSyncMarker)) {
        throw new IllegalStateException(
            String.format(
                "Expected the bytes [%d,%d) in file %s to be a sync marker, but found %s",
                syncMarkerOffset,
                syncMarkerOffset + syncMarker.length,
                getCurrentSource().getFileOrPatternSpec(),
                Arrays.toString(readSyncMarker)));
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
     * Creates a {@link PushbackInputStream} that has a large enough pushback buffer to be able to
     * push back the syncBuffer.
     */
    private PushbackInputStream createStream(ReadableByteChannel channel) {
      return new PushbackInputStream(
          Channels.newInputStream(channel), metadata.getSyncMarker().length);
    }

    // Postcondition: the stream is positioned at the beginning of the first block after the start
    // of the current source, and currentBlockOffset is that position. Additionally,
    // currentBlockSizeBytes will be set to 0 indicating that the previous block was empty.
    @Override
    protected void startReading(ReadableByteChannel channel) throws IOException {
      try {
        metadata = readMetadataFromFile(getCurrentSource().getSingleFileMetadata().resourceId());
      } catch (IOException e) {
        throw new RuntimeException(
            "Error reading metadata from file " + getCurrentSource().getSingleFileMetadata(), e);
      }

      long startOffset = getCurrentSource().getStartOffset();
      byte[] syncMarker = metadata.getSyncMarker();
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
     * Advances to the first byte after the next occurrence of the sync marker in the stream when
     * reading from the current offset. Returns the number of bytes consumed from the stream. Note
     * that this method requires a PushbackInputStream with a buffer at least as big as the marker
     * it is seeking for.
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

      /** Create a {@link Seeker} that looks for the given marker. */
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
       *     found.
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
