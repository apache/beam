package org.apache.beam.sdk.io.deltalake.example.delta;

import org.apache.beam.sdk.io.deltalake.example.data.TestEvent;
import org.apache.beam.sdk.io.deltalake.example.util.LogValueFn;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.DeltaFileIO;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.joda.time.Duration;

public class ReadDelta01
{
    static final String TABLE_PATH =
        "data/delta-lake-stream-03"
    ;

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

        /* -------  if reading from aws
        Configuration hadoopConfiguration = new Configuration();
        hadoopConfiguration.set("fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.profile.ProfileCredentialsProvider");
       // -----------------------*/

        PCollection<FileIO.ReadableFile> files = pipeline

            .apply("Find files",
                DeltaFileIO.snapshot()
                    .filepattern(TABLE_PATH)
                    // .withVersion(1L)
                    // .withHadoopConfiguration(hadoopConfiguration)
                    .withEmptyMatchTreatment(EmptyMatchTreatment.ALLOW)
                    .continuously(
                        Duration.standardSeconds(15),
                        Watch.Growth.afterTimeSinceNewOutput(Duration.standardDays(7))
                    )
            )

            .apply("Read matched files",
                FileIO.readMatches()
            )

            .apply("Log Files",
                ParDo.of(new LogValueFn<>(file -> String.format("==> file: %s", file.toString())))
            )
        ;

        PCollection<TestEvent> records = files
            .apply("Read parquet files",
                ParquetIO.parseFilesGenericRecords(new TestEvent.TestEventFromParquetFn())
            )

            .apply("Log Values",
                ParDo.of(new LogValueFn<>(s -> String.format("->  %s", s)))
            )
        ;

        pipeline.run();
    }

}