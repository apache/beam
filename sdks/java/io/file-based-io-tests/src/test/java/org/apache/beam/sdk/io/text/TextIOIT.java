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

package org.apache.beam.sdk.io.text;

import static org.apache.beam.sdk.io.Compression.AUTO;
import static org.apache.beam.sdk.io.common.FileBasedIOITHelper.appendTimestampSuffix;
import static org.apache.beam.sdk.io.common.FileBasedIOITHelper.getExpectedHashForLineCount;
import static org.apache.beam.sdk.io.common.FileBasedIOITHelper.readTestPipelineOptions;

import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.common.FileBasedIOITHelper;
import org.apache.beam.sdk.io.common.FileBasedIOITHelper.DeleteFileFn;
import org.apache.beam.sdk.io.common.HashingFn;
import org.apache.beam.sdk.io.common.IOTestPipelineOptions;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Integration tests for {@link org.apache.beam.sdk.io.TextIO}.
 *
 * <p>Run this test using the command below. Pass in connection information via PipelineOptions:
 * <pre>
 *  mvn -e -Pio-it verify -pl sdks/java/io/file-based-io-tests
 *  -Dit.test=org.apache.beam.sdk.io.text.TextIOIT
 *  -DintegrationTestPipelineOptions='[
 *  "--numberOfRecords=100000",
 *  "--filenamePrefix=output_file_path",
 *  "--compressionType=GZIP"
 *  ]'
 * </pre>
 * </p>
 * <p>Please see 'sdks/java/io/file-based-io-tests/pom.xml' for instructions regarding
 * running this test using Beam performance testing framework.</p>
 */
@RunWith(JUnit4.class)
public class TextIOIT {

  private static String filenamePrefix;
  private static Integer numberOfTextLines;
  private static Compression compressionType;

  @Rule
  public TestPipeline pipeline = TestPipeline.create();

  @BeforeClass
  public static void setup() {
    IOTestPipelineOptions options = readTestPipelineOptions();

    numberOfTextLines = options.getNumberOfRecords();
    filenamePrefix = appendTimestampSuffix(options.getFilenamePrefix());
    compressionType = Compression.valueOf(options.getCompressionType());
  }

  @Test
  public void writeThenReadAll() {
    TextIO.TypedWrite<String, Object> write = TextIO
        .write()
        .to(filenamePrefix)
        .withOutputFilenames()
        .withCompression(compressionType);

    PCollection<String> testFilenames =
        pipeline
            .apply("Generate sequence", GenerateSequence.from(0).to(numberOfTextLines))
            .apply(
                "Produce text lines",
                ParDo.of(new FileBasedIOITHelper.DeterministicallyConstructTestTextLineFn()))
            .apply("Write content to files", write)
            .getPerDestinationOutputFilenames()
            .apply(Values.create())
            .apply(Reshuffle.viaRandomKey());

    PCollection<String> consolidatedHashcode = testFilenames
        .apply("Read all files", TextIO.readAll().withCompression(AUTO))
        .apply("Calculate hashcode", Combine.globally(new HashingFn()));

    String expectedHash = getExpectedHashForLineCount(numberOfTextLines);
    PAssert.thatSingleton(consolidatedHashcode).isEqualTo(expectedHash);

    testFilenames.apply(
        "Delete test files",
        ParDo.of(new DeleteFileFn())
            .withSideInputs(consolidatedHashcode.apply(View.asSingleton())));

    pipeline.run().waitUntilFinish();
  }
}
