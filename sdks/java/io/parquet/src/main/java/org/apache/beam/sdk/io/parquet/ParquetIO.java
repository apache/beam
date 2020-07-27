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
package org.apache.beam.sdk.io.parquet;

import static java.lang.String.format;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.parquet.Preconditions.checkNotNull;
import static org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE;
import static org.apache.parquet.hadoop.ParquetInputFormat.STRICT_TYPE_CHECKING;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.compat.FilterCompat.Filter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.io.*;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * IO to read and write Parquet files.
 *
 * <h3>Reading Parquet files</h3>
 *
 * <p>{@link ParquetIO} source returns a {@link PCollection} for Parquet files. The elements in the
 * {@link PCollection} are Avro {@link GenericRecord}.
 *
 * <p>To configure the {@link Read}, you have to provide the file patterns (from) of the Parquet
 * files and the schema.
 *
 * <p>For example:
 *
 * <pre>{@code
 * PCollection<GenericRecord> records = pipeline.apply(ParquetIO.read(SCHEMA).from("/foo/bar"));
 * ...
 * }</pre>
 *
 * <p>As {@link Read} is based on {@link FileIO}, it supports any filesystem (hdfs, ...).
 *
 * <p>When using schemas created via reflection, it may be useful to generate {@link GenericRecord}
 * instances rather than instances of the class associated with the schema. {@link Read} and {@link
 * ReadFiles} provide {@link ParquetIO.Read#withAvroDataModel(GenericData)} allowing implementations
 * to set the data model associated with the {@link AvroParquetReader}
 *
 * <p>For more advanced use cases, like reading each file in a {@link PCollection} of {@link
 * FileIO.ReadableFile}, use the {@link ReadFiles} transform.
 *
 * <p>For example:
 *
 * <pre>{@code
 * PCollection<FileIO.ReadableFile> files = pipeline
 *   .apply(FileIO.match().filepattern(options.getInputFilepattern())
 *   .apply(FileIO.readMatches());
 *
 * PCollection<GenericRecord> output = files.apply(ParquetIO.readFiles(SCHEMA));
 * }</pre>
 *
 * <h3>Writing Parquet files</h3>
 *
 * <p>{@link ParquetIO.Sink} allows you to write a {@link PCollection} of {@link GenericRecord} into
 * a Parquet file. It can be used with the general-purpose {@link FileIO} transforms with
 * FileIO.write/writeDynamic specifically.
 *
 * <p>By default, {@link ParquetIO.Sink} produces output files that are compressed using the {@link
 * org.apache.parquet.format.CompressionCodec#SNAPPY}. This default can be changed or overridden
 * using {@link ParquetIO.Sink#withCompressionCodec(CompressionCodecName)}.
 *
 * <p>For example:
 *
 * <pre>{@code
 * pipeline
 *   .apply(...) // PCollection<GenericRecord>
 *   .apply(FileIO
 *     .<GenericRecord>write()
 *     .via(ParquetIO.sink(SCHEMA)
 *       .withCompressionCodec(CompressionCodecName.SNAPPY))
 *     .to("destination/path")
 *     .withSuffix(".parquet"));
 * }</pre>
 *
 * <p>This IO API is considered experimental and may break or receive backwards-incompatible changes
 * in future versions of the Apache Beam SDK.
 *
 * @see <a href="https://beam.apache.org/documentation/io/built-in/parquet/">Beam ParquetIO
 *     documentation</a>
 */
@Experimental(Kind.SOURCE_SINK)
public class ParquetIO {

  /**
   * Reads {@link GenericRecord} from a Parquet file (or multiple Parquet files matching the
   * pattern).
   */
  public static Read read(Schema schema) {
    return new AutoValue_ParquetIO_Read.Builder().setSchema(schema).setSplit(false).build();
  }

  /**
   * Like {@link #read(Schema)}, but reads each file in a {@link PCollection} of {@link
   * org.apache.beam.sdk.io.FileIO.ReadableFile}, which allows more flexible usage.
   */
  public static ReadFiles readFiles(Schema schema) {
    return new AutoValue_ParquetIO_ReadFiles.Builder().setSchema(schema).setSplit(false).build();
  }

  /** Implementation of {@link #read(Schema)}. */
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<GenericRecord>> {

    @Nullable
    abstract ValueProvider<String> getFilepattern();

    @Nullable
    abstract Schema getSchema();

    @Nullable
    abstract GenericData getAvroDataModel();

    abstract boolean getSplit();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {

      abstract Builder setSplit(boolean split);

      abstract Builder setFilepattern(ValueProvider<String> filepattern);

      abstract Builder setSchema(Schema schema);

      abstract Builder setAvroDataModel(GenericData model);

      abstract Read build();
    }

    /** Reads from the given filename or filepattern. */
    public Read from(ValueProvider<String> filepattern) {
      return toBuilder().setFilepattern(filepattern).build();
    }

    /** Like {@link #from(ValueProvider)}. */
    public Read from(String filepattern) {
      return from(ValueProvider.StaticValueProvider.of(filepattern));
    }

    public Read withSplit() {
      return toBuilder().setSplit(true).build();
    }

    /**
     * Define the Avro data model; see {@link AvroParquetReader.Builder#withDataModel(GenericData)}.
     */
    public Read withAvroDataModel(GenericData model) {
      return toBuilder().setAvroDataModel(model).build();
    }

    @Override
    public PCollection<GenericRecord> expand(PBegin input) {
      checkNotNull(getFilepattern(), "Filepattern cannot be null.");
      PCollection<FileIO.ReadableFile> inputFiles =
          input
              .apply(
                  "Create filepattern", Create.ofProvider(getFilepattern(), StringUtf8Coder.of()))
              .apply(FileIO.matchAll())
              .apply(FileIO.readMatches());
      if (!getSplit()) {
        return inputFiles.apply(
            readFiles(getSchema()).withSplit().withAvroDataModel(getAvroDataModel()));
      } else {
        return inputFiles.apply(readFiles(getSchema()).withAvroDataModel(getAvroDataModel()));
      }
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.add(
          DisplayData.item("filePattern", getFilepattern()).withLabel("Input File Pattern"));
    }
  }

  /** Implementation of {@link #readFiles(Schema)}. */
  @AutoValue
  public abstract static class ReadFiles
      extends PTransform<PCollection<FileIO.ReadableFile>, PCollection<GenericRecord>> {

    @Nullable
    abstract Schema getSchema();

    @Nullable
    abstract GenericData getAvroDataModel();

    abstract boolean getSplit();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setSchema(Schema schema);

      abstract Builder setAvroDataModel(GenericData model);

      abstract Builder setSplit(boolean split);

      abstract ReadFiles build();
    }

    /**
     * Define the Avro data model; see {@link AvroParquetReader.Builder#withDataModel(GenericData)}.
     */
    public ReadFiles withAvroDataModel(GenericData model) {
      return toBuilder().setAvroDataModel(model).build();
    }

    public ReadFiles withSplit() {
      return toBuilder().setSplit(true).build();
    }

    @Override
    public PCollection<GenericRecord> expand(PCollection<FileIO.ReadableFile> input) {
      checkNotNull(getSchema(), "Schema can not be null");
      if (!getSplit()) {
        return input
            .apply(ParDo.of(new SplitReadFn(getAvroDataModel())))
            .setCoder(AvroCoder.of(getSchema()));
      } else {
        return input
            .apply(ParDo.of(new ReadFn(getAvroDataModel())))
            .setCoder(AvroCoder.of(getSchema()));
      }
    }

    @DoFn.BoundedPerElement
    static class SplitReadFn extends DoFn<FileIO.ReadableFile, GenericRecord> {
      private Class<? extends GenericData> modelClass;
      private static final Logger LOG = LoggerFactory.getLogger(SplitReadFn.class);
      ReadSupport<GenericRecord> readSupport;

      SplitReadFn(GenericData model) {
        this.modelClass = model != null ? model.getClass() : null;
      }

      private static <K, V> Map<K, Set<V>> toSetMultiMap(Map<K, V> map) {
        Map<K, Set<V>> setMultiMap = new HashMap<K, Set<V>>();
        for (Map.Entry<K, V> entry : map.entrySet()) {
          Set<V> set = new HashSet<V>();
          set.add(entry.getValue());
          setMultiMap.put(entry.getKey(), Collections.unmodifiableSet(set));
        }
        return Collections.unmodifiableMap(setMultiMap);
      }

      private InputFile getInputFile(FileIO.ReadableFile file) throws IOException {
        if (!file.getMetadata().isReadSeekEfficient()) {
          throw new RuntimeException(
              String.format("File has to be seekable: %s", file.getMetadata().resourceId()));
        }
        return new BeamParquetInputFile(file.openSeekable());
      }

      @ProcessElement
      public void processElement(
          @Element FileIO.ReadableFile file,
          RestrictionTracker<OffsetRange, Long> tracker,
          OutputReceiver<GenericRecord> outputReceiver)
          throws Exception {
        ReadSupport<GenericRecord> readSupport;
        InputFile inputFile = getInputFile(file);
        Configuration conf = setConf();
        GenericData model = null;
        if (modelClass != null) {
          model = (GenericData) modelClass.getMethod("get").invoke(null);
        }
        readSupport = new AvroReadSupport<GenericRecord>(model);
        ParquetReadOptions options = HadoopReadOptions.builder(conf).build();
        ParquetFileReader reader = ParquetFileReader.open(inputFile, options);
        Filter filter = checkNotNull(options.getRecordFilter(), "filter");
        conf = ((HadoopReadOptions) options).getConf();
        for (String property : options.getPropertyNames()) {
          conf.set(property, options.getProperty(property));
        }
        FileMetaData parquetFileMetadata = reader.getFooter().getFileMetaData();
        MessageType fileSchema = parquetFileMetadata.getSchema();
        Map<String, String> fileMetadata = parquetFileMetadata.getKeyValueMetaData();

        ReadSupport.ReadContext readContext =
            readSupport.init(new InitContext(conf, toSetMultiMap(fileMetadata), fileSchema));
        ColumnIOFactory columnIOFactory = new ColumnIOFactory(parquetFileMetadata.getCreatedBy());
        MessageType requestedSchema = readContext.getRequestedSchema();
        RecordMaterializer<GenericRecord> recordConverter =
            readSupport.prepareForRead(conf, fileMetadata, fileSchema, readContext);
        boolean strictTypeChecking = options.isEnabled(STRICT_TYPE_CHECKING, true);
        boolean filterRecords = options.useRecordFilter();
        reader.setRequestedSchema(requestedSchema);
        MessageColumnIO columnIO =
            columnIOFactory.getColumnIO(requestedSchema, fileSchema, strictTypeChecking);
        long currentBlock = tracker.currentRestriction().getFrom();
        for (int i = 0; i < currentBlock; i++) {
          reader.skipNextRowGroup();
        }
        while (tracker.tryClaim(currentBlock)) {

          LOG.info("reading block" + currentBlock);
          PageReadStore pages = reader.readNextRowGroup();
          currentBlock += 1;
          RecordReader<GenericRecord> recordReader =
                  columnIO.getRecordReader(
                          pages, recordConverter, filterRecords ? filter : FilterCompat.NOOP);
          long currentRow = 0;
          long totalRows = pages.getRowCount();
          while (currentRow < totalRows) {
            try {
              GenericRecord record;
              currentRow += 1;
              try {
                record = recordReader.read();
              } catch (RecordMaterializer.RecordMaterializationException e) {
                LOG.debug("skipping a corrupt record");
                continue;
              }
              if (recordReader.shouldSkipCurrentRecord()) {
                // this record is being filtered via the filter2 package
                LOG.debug("skipping record");
                continue;
              }
              if (record == null) {
                // only happens with FilteredRecordReader at end of block
                LOG.debug("filtered record reader reached end of block");
                break;
              }
              outputReceiver.output(record);
            } catch (RuntimeException e) {
              throw new ParquetDecodingException(format("Can not read value at %d in block %d in file %s", currentRow, currentBlock, file.toString()), e);
            }
          }
          LOG.info("finish read " + currentRow + " rows");
        }



      }

      private Configuration setConf() throws Exception {
        Configuration conf = new Configuration();
        GenericData model = null;
        if (modelClass != null) {
          model = (GenericData) modelClass.getMethod("get").invoke(null);
        }
        if (model != null
            && (model.getClass() == GenericData.class || model.getClass() == SpecificData.class)) {
          conf.setBoolean(AvroReadSupport.AVRO_COMPATIBILITY, true);
        } else {
          conf.setBoolean(AvroReadSupport.AVRO_COMPATIBILITY, false);
        }
        return conf;
      }

      @GetInitialRestriction
      public OffsetRange getInitialRestriction(@Element FileIO.ReadableFile file) throws Exception {
        InputFile inputFile = getInputFile(file);
        Configuration conf = setConf();
        ParquetReadOptions options = HadoopReadOptions.builder(conf).build();
        ParquetFileReader reader = ParquetFileReader.open(inputFile, options);
        return new OffsetRange(0, reader.getRowGroups().size());
      }

      @SplitRestriction
      public void split(@Restriction OffsetRange restriction, OutputReceiver<OffsetRange> out) {
        for (OffsetRange range : restriction.split(1, 0)) {
          out.output(range);
        }
      }

      @NewTracker
      public OffsetRangeTracker newTracker(@Restriction OffsetRange restriction) {
        return new OffsetRangeTracker(restriction);
      }

      @GetRestrictionCoder
      public OffsetRange.Coder getRestrictionCoder() {
        return new OffsetRange.Coder();
      }

      @GetSize
      public double getSize(@Element FileIO.ReadableFile file, @Restriction OffsetRange restriction)
          throws Exception {
        InputFile inputFile = getInputFile(file);
        Configuration conf = setConf();
        ParquetReadOptions options = HadoopReadOptions.builder(conf).build();
        ParquetFileReader reader = ParquetFileReader.open(inputFile, options);
        if (restriction == null) {
          return 0;
        } else {
          long start = restriction.getFrom();
          long end = restriction.getTo();
          List<BlockMetaData> blocks = reader.getRowGroups();
          double size = 0;
          for (long i = start; i < end; i++) {
            size += blocks.get((int) i).getRowCount();
          }
          return size;
        }
      }
    }

    static class ReadFn extends DoFn<FileIO.ReadableFile, GenericRecord> {

      private Class<? extends GenericData> modelClass;

      ReadFn(GenericData model) {
        this.modelClass = model != null ? model.getClass() : null;
      }

      @ProcessElement
      public void processElement(ProcessContext processContext) throws Exception {
        FileIO.ReadableFile file = processContext.element();

        if (!file.getMetadata().isReadSeekEfficient()) {
          ResourceId filename = file.getMetadata().resourceId();
          throw new RuntimeException(String.format("File has to be seekable: %s", filename));
        }

        SeekableByteChannel seekableByteChannel = file.openSeekable();

        AvroParquetReader.Builder builder =
            AvroParquetReader.<GenericRecord>builder(new BeamParquetInputFile(seekableByteChannel));
        if (modelClass != null) {
          // all GenericData implementations have a static get method
          builder = builder.withDataModel((GenericData) modelClass.getMethod("get").invoke(null));
        }

        try (ParquetReader<GenericRecord> reader = builder.build()) {
          GenericRecord read;
          while ((read = reader.read()) != null) {
            processContext.output(read);
          }
        }
      }
    }

    private static class BeamParquetInputFile implements InputFile {

      private SeekableByteChannel seekableByteChannel;

      BeamParquetInputFile(SeekableByteChannel seekableByteChannel) {
        this.seekableByteChannel = seekableByteChannel;
      }

      @Override
      public long getLength() throws IOException {
        return seekableByteChannel.size();
      }

      @Override
      public SeekableInputStream newStream() {
        return new DelegatingSeekableInputStream(Channels.newInputStream(seekableByteChannel)) {

          @Override
          public long getPos() throws IOException {
            return seekableByteChannel.position();
          }

          @Override
          public void seek(long newPos) throws IOException {
            seekableByteChannel.position(newPos);
          }
        };
      }
    }
  }

  /** Creates a {@link Sink} that, for use with {@link FileIO#write}. */
  public static Sink sink(Schema schema) {
    return new AutoValue_ParquetIO_Sink.Builder()
        .setJsonSchema(schema.toString())
        .setCompressionCodec(CompressionCodecName.SNAPPY)
        .build();
  }

  /** Implementation of {@link #sink}. */
  @AutoValue
  public abstract static class Sink implements FileIO.Sink<GenericRecord> {

    @Nullable
    abstract String getJsonSchema();

    abstract CompressionCodecName getCompressionCodec();

    @Nullable
    abstract SerializableConfiguration getConfiguration();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setJsonSchema(String jsonSchema);

      abstract Builder setCompressionCodec(CompressionCodecName compressionCodec);

      abstract Builder setConfiguration(SerializableConfiguration configuration);

      abstract Sink build();
    }

    /** Specifies compression codec. By default, CompressionCodecName.SNAPPY. */
    public Sink withCompressionCodec(CompressionCodecName compressionCodecName) {
      return toBuilder().setCompressionCodec(compressionCodecName).build();
    }

    /** Specifies configuration to be passed into the sink's writer. */
    public Sink withConfiguration(Map<String, String> configuration) {
      Configuration hadoopConfiguration = new Configuration();
      for (Map.Entry<String, String> entry : configuration.entrySet()) {
        hadoopConfiguration.set(entry.getKey(), entry.getValue());
      }
      return toBuilder()
          .setConfiguration(new SerializableConfiguration(hadoopConfiguration))
          .build();
    }

    @Nullable private transient ParquetWriter<GenericRecord> writer;

    @Override
    public void open(WritableByteChannel channel) throws IOException {
      checkNotNull(getJsonSchema(), "Schema cannot be null");

      Schema schema = new Schema.Parser().parse(getJsonSchema());

      BeamParquetOutputFile beamParquetOutputFile =
          new BeamParquetOutputFile(Channels.newOutputStream(channel));

      AvroParquetWriter.Builder<GenericRecord> builder =
          AvroParquetWriter.<GenericRecord>builder(beamParquetOutputFile)
              .withSchema(schema)
              .withCompressionCodec(getCompressionCodec())
              .withWriteMode(OVERWRITE);

      if (getConfiguration() != null) {
        builder = builder.withConf(getConfiguration().get());
      }

      this.writer = builder.build();
    }

    @Override
    public void write(GenericRecord element) throws IOException {
      checkNotNull(writer, "Writer cannot be null");
      writer.write(element);
    }

    @Override
    public void flush() throws IOException {
      // the only way to completely flush the output is to call writer.close() here
      writer.close();
    }

    private static class BeamParquetOutputFile implements OutputFile {

      private OutputStream outputStream;

      BeamParquetOutputFile(OutputStream outputStream) {
        this.outputStream = outputStream;
      }

      @Override
      public PositionOutputStream create(long blockSizeHint) {
        return new BeamOutputStream(outputStream);
      }

      @Override
      public PositionOutputStream createOrOverwrite(long blockSizeHint) {
        return new BeamOutputStream(outputStream);
      }

      @Override
      public boolean supportsBlockSize() {
        return false;
      }

      @Override
      public long defaultBlockSize() {
        return 0;
      }
    }

    private static class BeamOutputStream extends PositionOutputStream {
      private long position = 0;
      private OutputStream outputStream;

      private BeamOutputStream(OutputStream outputStream) {
        this.outputStream = outputStream;
      }

      @Override
      public long getPos() throws IOException {
        return position;
      }

      @Override
      public void write(int b) throws IOException {
        position++;
        outputStream.write(b);
      }

      @Override
      public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
      }

      @Override
      public void write(byte[] b, int off, int len) throws IOException {
        outputStream.write(b, off, len);
        position += len;
      }

      @Override
      public void flush() throws IOException {
        outputStream.flush();
      }

      @Override
      public void close() throws IOException {
        outputStream.close();
      }
    }
  }

  /** Disallow construction of utility class. */
  private ParquetIO() {}
}
