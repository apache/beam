package org.apache.beam.io.iceberg;

import java.io.IOException;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.DataReader;
import org.apache.iceberg.data.orc.GenericOrcReader;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.encryption.InputFilesDecryptor;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergFileScanFn extends DoFn<IcebergScanTask, Row> {
    private static Logger LOG = LoggerFactory.getLogger(IcebergFileScanFn.class);

    private Schema project;
    private org.apache.beam.sdk.schemas.Schema rowSchema;


    public IcebergFileScanFn(Schema project,org.apache.beam.sdk.schemas.Schema rowSchema) {
        this.project = project;
        this.rowSchema = rowSchema;
    }

    public IcebergFileScanFn(Schema schema) {
        this(schema,SchemaHelper.convert(schema));
    }

    public IcebergFileScanFn(org.apache.beam.sdk.schemas.Schema schema) {
        this(SchemaHelper.convert(schema),schema);
    }



    private Row convert(Record record) {
        Row.Builder b = Row.withSchema(rowSchema);
        for(int i=0;i< rowSchema.getFieldCount();i++) {
            //TODO: A lot obviously
            b.addValue(record.getField(rowSchema.getField(i).getName()));
        }
        return b.build();
    }


    @ProcessElement
    public void process(ProcessContext context) {
        IcebergScanTask icebergScanTask = context.element();
        CombinedScanTask task = icebergScanTask.task();
        if(task == null) {
            LOG.info("Skipping scan task.");
            return; // Not a valid scan task
        }

        InputFilesDecryptor decryptor = new InputFilesDecryptor(task,icebergScanTask.io(),icebergScanTask.encryption());
        for(FileScanTask fileTask : task.files()) {
            if(fileTask.isDataTask()) {
                LOG.error("{} is a DataTask. Skipping.",fileTask.toString());
                continue;
            }
            //Maybe a split opportunity here, though the split size is controlled in the planning process...
            DataFile file = fileTask.file();
            InputFile input = decryptor.getInputFile(fileTask);

            CloseableIterable<Record> baseIter = null;
            switch(file.format()) {
                case ORC:
                    LOG.info("Preparing ORC input");
                    baseIter = ORC.read(input).project(project)
                        .createReaderFunc(fileSchema -> GenericOrcReader.buildReader(project,fileSchema))
                        .build();
                    break;
                case PARQUET:
                    LOG.info("Preparing Parquet input.");
                    baseIter = Parquet.read(input).project(project)
                        .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(project,fileSchema))
                        .build();
                    break;
                case AVRO:
                    LOG.info("Preparing Avro input.");
                    baseIter = Avro.read(input).project(project)
                        .createReaderFunc(DataReader::create).build();
                    break;
                default:
                    throw new UnsupportedOperationException("Cannot read format: "+file.format());
            }
            try(CloseableIterable<Record> iter = baseIter) {
                int counter = 0;
                for(Record t : iter) {
                    context.output(convert(t));
                    counter++;
                }
                LOG.info("Produced {} records.",counter);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        }


    }

}
