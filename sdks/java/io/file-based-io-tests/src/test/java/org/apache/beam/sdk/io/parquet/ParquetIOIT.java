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

import static org.apache.beam.sdk.io.common.FileBasedIOITHelper.appendTimestampSuffix;
import static org.apache.beam.sdk.io.common.FileBasedIOITHelper.getExpectedHashForLineCount;
import static org.apache.beam.sdk.io.common.FileBasedIOITHelper.readFileBasedIOITPipelineOptions;
import static org.apache.beam.sdk.values.TypeDescriptors.strings;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.common.FileBasedIOITHelper;
import org.apache.beam.sdk.io.common.FileBasedIOTestPipelineOptions;
import org.apache.beam.sdk.io.common.HashingFn;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Integration tests for {@link org.apache.beam.sdk.io.parquet.ParquetIO}.
 *
 * <p>Run this test using the command below. Pass in connection information via PipelineOptions:
 *
 * <pre>
 *  ./gradlew integrationTest -p sdks/java/io/file-based-io-tests
 *  -DintegrationTestPipelineOptions='[
 *  "--numberOfRecords=100000",
 *  "--filenamePrefix=output_file_path",
 *  ]'
 *  --tests org.apache.beam.sdk.io.parquet.ParquetIOIT
 *  -DintegrationTestRunner=direct
 * </pre>
 *
 * <p>Please see 'build_rules.gradle' file for instructions regarding running this test using Beam
 * performance testing framework.
 */
@RunWith(JUnit4.class)
public class ParquetIOIT {

  private static final Schema SCHEMA =
      new Schema.Parser()
          .parse(
              "{\n"
                  + " \"namespace\": \"ioitavro\",\n"
                  + " \"type\": \"record\",\n"
                  + " \"name\": \"TestAvroLine\",\n"
                  + " \"fields\": [\n"
                  + "     {\"name\": \"row\", \"type\": \"string\"}\n"
                  + " ]\n"
                  + "}");

  private static String filenamePrefix;
  private static Integer numberOfRecords;

  @Rule public TestPipeline pipeline = TestPipeline.create();

  @BeforeClass
  public static void setup() {
    FileBasedIOTestPipelineOptions options = readFileBasedIOITPipelineOptions();

    numberOfRecords = options.getNumberOfRecords();
    filenamePrefix = appendTimestampSuffix(options.getFilenamePrefix());
  }

  @Test
  public void writeThenReadAll() {
    PCollection<String> testFiles =
        pipeline
            .apply("Generate sequence", GenerateSequence.from(0).to(numberOfRecords))
            .apply(
                "Produce text lines",
                ParDo.of(new FileBasedIOITHelper.DeterministicallyConstructTestTextLineFn()))
            .apply("Produce Avro records", ParDo.of(new DeterministicallyConstructAvroRecordsFn()))
            .setCoder(AvroCoder.of(SCHEMA))
            .apply(
                "Write Parquet files",
                FileIO.<GenericRecord>write().via(ParquetIO.sink(SCHEMA)).to(filenamePrefix))
            .getPerDestinationOutputFilenames()
            .apply("Get file names", Values.create());

    PCollection<String> consolidatedHashcode =
        testFiles
            .apply("Find files", FileIO.matchAll())
            .apply("Read matched files", FileIO.readMatches())
            .apply("Read parquet files", ParquetIO.readFiles(SCHEMA))
            .apply(
                "Map records to strings",
                MapElements.into(strings())
                    .via(
                        (SerializableFunction<GenericRecord, String>)
                            record -> String.valueOf(record.get("row"))))
            .apply("Calculate hashcode", Combine.globally(new HashingFn()));

    String expectedHash = getExpectedHashForLineCount(numberOfRecords);
    PAssert.thatSingleton(consolidatedHashcode).isEqualTo(expectedHash);

    testFiles.apply(
        "Delete test files",
        ParDo.of(new FileBasedIOITHelper.DeleteFileFn())
            .withSideInputs(consolidatedHashcode.apply(View.asSingleton())));

    pipeline.run().waitUntilFinish();
  }

  private static class DeterministicallyConstructAvroRecordsFn extends DoFn<String, GenericRecord> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(new GenericRecordBuilder(SCHEMA).set("row", c.element()).build());
    }
  }
}
