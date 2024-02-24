package org.apache.beam.io.iceberg;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import javax.annotation.Nullable;
import org.apache.beam.io.iceberg.util.RowHelper;
import org.apache.beam.sdk.io.DefaultFilenamePolicy;
import org.apache.beam.sdk.io.DynamicFileDestinations;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.SerializableBiConsumer;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.orc.GenericOrcWriter;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.log4j.Logger;

@SuppressWarnings("all")
public class IcebergSink extends FileBasedSink<Row,Void,Row> {

  private static final Logger LOG = Logger.getLogger(IcebergSink.class);

  Iceberg.Catalog catalog;
  String tableId;

  Iceberg.WriteFormat format;

  SerializableBiConsumer<String,KV<DataFile,ResourceId>> metadataFn;

  private static ValueProvider<ResourceId> constantResourceId(String value) {
    final ResourceId resource = FileBasedSink.convertToFileResourceIfPossible(value);
    return new ValueProvider<ResourceId>() {
      @Override
      public ResourceId get() {
        return resource;
      }

      @Override
      public boolean isAccessible() {
        return false;
      }
    };
  }

  private static String tableLocation(Iceberg.Catalog catalog,String tableId) {
    return catalog.catalog().loadTable(TableIdentifier.parse(tableId)).location();
  }


  public IcebergSink(
      Iceberg.Catalog catalog,
      String tableId,
      Iceberg.WriteFormat format,
      SerializableBiConsumer<String, KV<DataFile,ResourceId>> metadataFn) {

    super(
        constantResourceId(tableLocation(catalog,tableId)),
        DynamicFileDestinations.constant(DefaultFilenamePolicy.fromStandardParameters(
            constantResourceId(tableLocation(catalog,tableId)),
            DefaultFilenamePolicy.DEFAULT_WINDOWED_SHARD_TEMPLATE,
            "",false)
        ));
    this.catalog = catalog;
    this.tableId = tableId;
    this.format = format;
    this.metadataFn = metadataFn;
  }

  public Table getTable() {
    return catalog.catalog().loadTable(TableIdentifier.parse(tableId));
  }

  public Iceberg.WriteFormat getFormat() {
    return format;
  }

  private static class IcebergWriteOperation extends WriteOperation<Void, Row> {


    public IcebergWriteOperation(IcebergSink sink,
        Table table,Iceberg.WriteFormat format) {
      super(sink);
    }

    public Table getTable() {
      return ((IcebergSink)getSink()).getTable();
    }

    public Iceberg.WriteFormat getFormat() {
      return ((IcebergSink)getSink()).getFormat();
    }

    @Override
    public Writer<Void, Row> createWriter()
        throws  Exception {
      return new IcebergWriter(this);
    }
  }

  @Override
  public WriteOperation<Void, Row> createWriteOperation() {
    return new IcebergWriteOperation(this,
        catalog.catalog().loadTable(TableIdentifier.parse(tableId)),
        format
    );
  }

  @SuppressWarnings("all")
  private static class IcebergWriter extends Writer<Void,Row> {

    transient @Nullable DataWriter<Record> appender;
    transient @Nullable GenericRecord baseRecord;

    public IcebergWriter(IcebergWriteOperation writeOperation) {
      super(writeOperation, MimeTypes.BINARY);
    }

    @Override
    protected void prepareWrite(WritableByteChannel channel)
        throws Exception {
      Table t = ((IcebergWriteOperation)getWriteOperation()).getTable();
      baseRecord = GenericRecord.create(t.schema());
      switch(((IcebergWriteOperation)getWriteOperation()).getFormat()) {
        case AVRO:
          appender = Avro.writeData(new IcebergOutputFile(getOutputFile(),channel))
              .schema(t.schema())
              .withSpec(PartitionSpec.unpartitioned())
              .overwrite().build();
          break;
        case PARQUET:
          appender = Parquet.writeData(new IcebergOutputFile(getOutputFile(),channel))
              .createWriterFunc(GenericParquetWriter::buildWriter)
              .schema(t.schema())
              .withSpec(PartitionSpec.unpartitioned())
              .overwrite().build();
          break;
        case ORC:
          appender = ORC.writeData(new IcebergOutputFile(getOutputFile(),channel))
              .createWriterFunc(GenericOrcWriter::buildWriter)
              .schema(t.schema())
              .withSpec(PartitionSpec.unpartitioned())
              .overwrite().build();
          break;
      }
    }

    @Override
    public void write(Row value) throws Exception {
      appender.write(RowHelper.copy(baseRecord,value));
    }

    @Override
    protected void finishWrite() throws Exception {
      LOG.info("Finishing: "+getOutputFile().toString());
      if(appender == null) {
        throw new RuntimeException("Appender not initialized?!");
      }
      appender.close();
      super.finishWrite();

      //TODO: Move this to a function so it can (for example) be sent to another pcollection.
      ((IcebergWriteOperation)getWriteOperation()).getTable().newFastAppend()
          .appendFile(appender.toDataFile())
          .commit();

    }
  }



  private static class IcebergDummyInputfile implements InputFile {

    IcebergOutputFile source;
    public IcebergDummyInputfile(IcebergOutputFile source) {
      this.source = source;
    }

    @Override
    public long getLength() {
      return 0;
    }

    @Override
    public SeekableInputStream newStream() {
      return null;
    }

    @Override
    public String location() {
      return source.location();
    }

    @Override
    public boolean exists() {
      return true;
    }
  }

  private static class IcebergOutputFile implements OutputFile {

    WritableByteChannel channel;
    ResourceId location;

    private IcebergOutputFile(ResourceId location,WritableByteChannel channel) {
      this.location = location;
      this.channel = channel;
    }

    @Override
    public PositionOutputStream create() {
      return new PositionOutputStream() {

        long pos = 0;

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
          pos += len;
          channel.write(ByteBuffer.wrap(b, 0, len));
        }

        @Override
        public long getPos() throws IOException {
          return pos;
        }

        @Override
        public void write(int b) throws IOException {
          byte byt = (byte) (b & 0xff);
          write(new byte[]{byt}, 0, 1);
        }
      };
    }

    @Override
    public PositionOutputStream createOrOverwrite() {
      return create();
    }

    @Override
    public String location() {
      return location.toString();
    }

    @Override
    public InputFile toInputFile() {
      return new IcebergDummyInputfile(this);
    }
  }


}
