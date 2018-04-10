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

package org.apache.beam.sdk.io.tfrecord;

import static org.apache.beam.sdk.io.Compression.AUTO;
import static org.apache.beam.sdk.io.common.FileBasedIOITHelper.appendTimestampSuffix;
import static org.apache.beam.sdk.io.common.FileBasedIOITHelper.getExpectedHashForLineCount;
import static org.apache.beam.sdk.io.common.FileBasedIOITHelper.readTestPipelineOptions;

import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.TFRecordIO;
import org.apache.beam.sdk.io.common.FileBasedIOITHelper;
import org.apache.beam.sdk.io.common.FileBasedIOITHelper.DeleteFileFn;
import org.apache.beam.sdk.io.common.HashingFn;
import org.apache.beam.sdk.io.common.IOTestPipelineOptions;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Integration tests for {@link org.apache.beam.sdk.io.TFRecordIO}.
 *
 * <p>Run this test using the command below. Pass in connection information via PipelineOptions:
 * <pre>
 *  mvn -e -Pio-it verify -pl sdks/java/io/file-based-io-tests
 *  -Dit.test=org.apache.beam.sdk.io.tfrecord.TFRecordIOIT
 *  -DintegrationTestPipelineOptions='[
 *  "--numberOfRecords=100000",
 *  "--filenamePrefix=output_file_path",
 *  "--compressionType=GZIP"
 *  ]'
 * </pre>
 * </p>
 * <p>Please {@see 'sdks/java/io/file-based-io-tests/pom.xml'} for instructions regarding
 * running this test using Beam performance testing framework.</p>
 */
@RunWith(JUnit4.class)
public class TFRecordIOIT {

  private static String filenamePrefix;
  private static Integer numberOfTextLines;
  private static Compression compressionType;

  @Rule
  public TestPipeline writePipeline = TestPipeline.create();

  @Rule
  public TestPipeline readPipeline = TestPipeline.create();

  @BeforeClass
  public static void setup() {
    IOTestPipelineOptions options = readTestPipelineOptions();

    numberOfTextLines = options.getNumberOfRecords();
    filenamePrefix = appendTimestampSuffix(options.getFilenamePrefix());
    compressionType = Compression.valueOf(options.getCompressionType());
  }

  private static String createFilenamePattern() {
    return filenamePrefix + "*";
  }

  // TODO: There are two pipelines due to: https://issues.apache.org/jira/browse/BEAM-3267
  @Test
  public void writeThenReadAll() {
    TFRecordIO.Write writeTransform = TFRecordIO
        .write()
        .to(filenamePrefix)
        .withCompression(compressionType)
        .withSuffix(".tfrecord");

    writePipeline
        .apply("Generate sequence", GenerateSequence.from(0).to(numberOfTextLines))
        .apply("Produce text lines",
            ParDo.of(new FileBasedIOITHelper.DeterministicallyConstructTestTextLineFn()))
        .apply("Transform strings to bytes", MapElements.via(new StringToByteArray()))
        .apply("Write content to files", writeTransform);

    writePipeline.run().waitUntilFinish();

    String filenamePattern = createFilenamePattern();
    PCollection<String> consolidatedHashcode =
        readPipeline
            .apply(TFRecordIO.read().from(filenamePattern).withCompression(AUTO))
            .apply("Transform bytes to strings", MapElements.via(new ByteArrayToString()))
            .apply("Calculate hashcode", Combine.globally(new HashingFn()))
            .apply(Reshuffle.viaRandomKey());

    String expectedHash = getExpectedHashForLineCount(numberOfTextLines);
    PAssert.thatSingleton(consolidatedHashcode).isEqualTo(expectedHash);

    readPipeline
        .apply(Create.of(filenamePattern))
        .apply(
            "Delete test files",
            ParDo.of(new DeleteFileFn())
                .withSideInputs(consolidatedHashcode.apply(View.asSingleton())));
    readPipeline.run().waitUntilFinish();
  }

  static class StringToByteArray extends SimpleFunction<String, byte[]> {
    @Override
    public byte[] apply(String input) {
      return input.getBytes();
    }
  }

  static class ByteArrayToString extends SimpleFunction<byte[], String> {
    @Override
    public String apply(byte[] input) {
      return new String(input);
    }
  }
}
