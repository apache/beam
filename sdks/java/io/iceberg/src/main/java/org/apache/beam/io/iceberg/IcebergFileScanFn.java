package org.apache.beam.io.iceberg;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.encryption.InputFilesDecryptor;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class IcebergFileScanFn<T> extends DoFn<IcebergScanTask, T> {
    static Logger LOG = LoggerFactory.getLogger(IcebergFileScanFn.class);

    private Schema project;

    IcebergFileScanFn(Schema schema) {
        this.project = schema;
    }

    @ProcessElement
    public void process(ProcessContext context) {
        IcebergScanTask icebergScanTask = context.element();
        CombinedScanTask task = icebergScanTask.task();
        if(task == null) {
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

            CloseableIterable<T> baseIter = null;
            switch(file.format()) {
                case ORC:
                    baseIter = ORC.read(input).project(project).build();
                    break;
                case PARQUET:
                    baseIter = Parquet.read(input).project(project).build();
                    break;
                case AVRO:
                    baseIter = Avro.read(input).project(project).build();
                    break;
                default:
                    throw new UnsupportedOperationException("Cannot read format: "+file.format());
            }
            try(CloseableIterable<T> iter = baseIter) {
                for(T t : iter) {
                    context.output(t);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        }


    }

}
