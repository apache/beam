package org.apache.beam.io.iceberg;

import java.io.IOException;
import java.util.UUID;
import org.apache.beam.io.iceberg.util.RowHelper;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.orc.GenericOrcWriter;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergCatalogWriter extends PTransform<PCollection<Row>,IcebergWriteResult> {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergCatalogWriter.class);
  private static final TupleTag<Row> successfulWrites = new TupleTag<>();
  private static final TupleTag<Row> failedWrites = new TupleTag<>();
  private static final TupleTag<KV<KV<String,String>,DataFile>> catalogUpdates = new TupleTag<>();


  final Iceberg.Catalog catalog;
  final String tableId;

  final FileFormat format;

  final PartitionSpec partitionSpec;

  public IcebergCatalogWriter(
      Iceberg.Catalog catalog,
      String tableId,
      FileFormat format,
      PartitionSpec partitionSpec) {
    this.catalog = catalog;
    this.tableId = tableId;
    this.format = format;
    this.partitionSpec = partitionSpec;
  }



  @Override
  public IcebergWriteResult expand(PCollection<Row> input) {
    if(input.isBounded() == IsBounded.UNBOUNDED) {
      throw new UnsupportedOperationException("Unbounded Appends Not Yet Implemented");
    }
    //Put everything into the global window
    input = input.apply("RewindowIntoGlobal",
        Window.<>into(new GlobalWindows()).triggering(
        DefaultTrigger.of()).discardingFiredPanes());
    //Write things into files
    input.apply(new WriteUnshardedFiles(catalog,tableId,partitionSpec,format));

    //Update the manifest


    return new IcebergWriteResult(input.getPipeline());
  }

  private static class WriteUnshardedFiles extends PTransform<PCollection<Row>, PCollectionTuple> {

    Iceberg.Catalog catalog;

    String tableId;

    PartitionSpec partitionSpec;

    FileFormat format;

    public WriteUnshardedFiles(
        Iceberg.Catalog catalog,
        String tableId,
        PartitionSpec partitionSpec,
        FileFormat format) {
      this.catalog = catalog;
      this.tableId = tableId;
      this.partitionSpec = partitionSpec;
      this.format = format;
    }

    @Override
    public PCollectionTuple expand(PCollection<Row> input) {
      return input.apply(ParDo.of(new WriteUnshardedFilesFn(catalog,tableId,partitionSpec,format))
          .withOutputTags());
    }
  }

  private static class WriteUnshardedFilesFn extends DoFn<Row, Row> {

    private Iceberg.Catalog catalog;
    private String tableId;

    private PartitionSpec partitionSpec;

    private FileFormat format;

    transient DataWriter<Record> writer;
    transient OutputFile outputFile;

    transient GenericRecord baseRecord;

    transient BoundedWindow window = null;
    transient Instant timestamp = null;

    public WriteUnshardedFilesFn(
        Iceberg.Catalog catalog,
        String tableId,
        PartitionSpec partitionSpec,
        FileFormat format) {
      this.catalog = catalog;
      this.tableId = tableId;
      this.partitionSpec = partitionSpec;
      this.format = format;
    }

    @ProcessElement
    public void processElement(ProcessContext context, BoundedWindow window) {
      writer.write(RowHelper.copy(baseRecord,context.element()));
      if(timestamp.compareTo(context.timestamp()) < 0) {
        this.window = window;
        this.timestamp = context.timestamp();
      }
      context.output(context.element());
    }

    @StartBundle
    public void startBundle(StartBundleContext c) {
      if(writer != null) {
        throw new UnsupportedOperationException("Writer function has been reused without proper shutdown");
      }
      Table table = catalog.catalog().loadTable(TableIdentifier.parse(tableId));
      outputFile = table.io().newOutputFile(table.location()+"/"+ UUID.randomUUID().toString());
      baseRecord = GenericRecord.create(table.schema());
      try {
        switch (format) {
          case AVRO:
            writer = Avro.writeData(outputFile)
                .schema(table.schema())
                .withSpec(partitionSpec)
                .overwrite().build();
            break;
          case PARQUET:
            writer = Parquet.writeData(outputFile)
                .createWriterFunc(GenericParquetWriter::buildWriter)
                .schema(table.schema())
                .withSpec(partitionSpec)
                .overwrite().build();
            break;
          case ORC:
            writer = ORC.writeData(outputFile)
                .createWriterFunc(GenericOrcWriter::buildWriter)
                .schema(table.schema())
                .withSpec(partitionSpec)
                .overwrite().build();
            break;

        }
      } catch(IOException e) {
        throw new RuntimeException(e.getMessage());
      }
    }

    @FinishBundle
    public void finishBundle(FinishBundleContext context) {
      try {
        writer.close();
        DataFile dataFile = writer.toDataFile();
        context.output(catalogUpdates,
            KV.of(KV.of(tableId,outputFile.location()),dataFile),
            timestamp,window);
      } catch(IOException e) {
      } finally {
        writer = null;
        outputFile = null;
      }
    }
  }


}
