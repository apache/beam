package org.apache.beam.sdk.io.deltalake.example.parquet;

import org.apache.beam.sdk.io.deltalake.example.util.LogValueFn;
import org.apache.beam.sdk.io.deltalake.example.util.RuntimeUtil;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import java.io.IOException;
import java.util.List;

public class ReadParquet00 {

  static final String TABLE_PATH =
      // "/tmp/delta-lake-01/*.parquet"
      "s3://idl-spp-deltalake-test-mb/delta-lake-stream-01/*.parquet"
      ;

  public static void main(String[] args) throws IOException
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


  private static void runReadPipeline(PipelineOptions options) throws IOException
  {
    Pipeline pipeline = Pipeline.create(options);

    System.out.println("------ started --------");

    List<MatchResult.Metadata> list = RuntimeUtil.getFiles("s3://idl-spp-deltalake-test-mb/delta-lake-stream-01/*.parquet", true);

    PCollection<FileIO.ReadableFile> files = pipeline

        .apply("Find files", FileIO.match().filepattern(TABLE_PATH))

        .apply("Read matched files", FileIO.readMatches())

        .apply("Log Files",
            ParDo.of(new LogValueFn<>(file -> String.format("==> file: %s", file.toString())))
        )
        ;

//    PCollection<String> records = files
//      .apply(
//          ParquetIO.parseFilesGenericRecords(
//              new SerializableFunction<GenericRecord, String>() {
//                public String apply(GenericRecord record) {
//                  // If needed, access the schema of the record using record.getSchema()
//                  System.out.println(record.toString());
//                  // System.out.println(record.getSchema().toString(true));
//                  return record.toString();
//                }
//              })
//      )
//    ;

    // .apply("Read parquet files", ParquetIO.readFiles(SCHEMA))

    // .apply("Map records to strings", MapElements.into(strings()).via(new GetRecordsFn()))

    // pipeline.run();
  }

}
