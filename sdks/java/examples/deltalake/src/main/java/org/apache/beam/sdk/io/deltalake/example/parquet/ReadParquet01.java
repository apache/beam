package org.apache.beam.sdk.io.deltalake.example.parquet;

import org.apache.beam.sdk.io.deltalake.example.util.LogValueFn;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ReadParquet01
{
    static final String TABLE_PATH = "data/delta-lake-stream-03/*.parquet";

    private static final Logger logger = LoggerFactory.getLogger(ReadParquet00.class);

    public static void main(String[] args)
    {
        PipelineOptions options = PipelineOptionsFactory
            .fromArgs(
                "--runner=FlinkRunner",
                "--flinkMaster=[auto]",
//            "--flinkMaster=localhost:8081",
                "--maxParallelism=360",
                "--parallelism=3"
            )
            .withValidation().as(PipelineOptions.class);

        runReadPipeline(options);
    }


    private static void runReadPipeline(PipelineOptions options)
    {
        Pipeline pipeline = Pipeline.create(options);

        try {
            PCollection<FileIO.ReadableFile> files = pipeline

                .apply("Find files",
                    FileIO.match().filepattern(TABLE_PATH)
                )

                .apply("Read matched files",
                    FileIO.readMatches()
                )

                .apply("Log Files",
                    ParDo.of(new LogValueFn<>(file -> String.format("==> file: %s", file.toString())))
                )
            ;

            PCollection<String> records = files
                .apply("Read parquet files",
                    ParquetIO.parseFilesGenericRecords(
                        new SerializableFunction<GenericRecord, String>() {
                            public String apply(GenericRecord record) {
                                // System.out.println(record.toString());
                                // If needed, access the schema of the record using record.getSchema()
                                System.out.println(">>>>>>: " + record.getSchema().toString(true));
                                return record.toString();
                            }
                        })
                )
                .apply("Log Values",
                    ParDo.of(new LogValueFn<>(s -> String.format("->  %s", s)))
                )
            ;

            pipeline.run();

        } catch (Exception e) {
            logger.error("Failed to initialize pipeline");
            throw new RuntimeException(e);
        }
    }

}