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
package org.apache.beam.sdk.io.xml;

import static org.apache.beam.sdk.io.common.IOTestHelper.appendTimestampSuffix;

import com.google.common.collect.ImmutableMap;
import java.nio.charset.Charset;
import java.util.Map;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.common.DeleteFileFn;
import org.apache.beam.sdk.io.common.HashingFn;
import org.apache.beam.sdk.io.common.IOTestHelper;
import org.apache.beam.sdk.io.common.IOTestPipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Integration tests for {@link org.apache.beam.sdk.io.xml.XmlIO}.
 *
 * <p>Run those tests using the command below. Pass in connection information via PipelineOptions:
 * <pre>
 *  mvn -e -Pio-it verify -pl sdks/java/io/xml
 *  -Dit.test=org.apache.beam.sdk.io.xml.XmlIOIT
 *  -DintegrationTestPipelineOptions='[
 *  "--numberOfRecords=100000",
 *  "--filenamePrefix=output_file_path",
 *  "--charset=UTF-8",
 *  ]'
 * </pre>
 * </p>
 */
@RunWith(JUnit4.class)
public class XmlIOIT {

  private static final Map<Integer, String> EXPECTED_HASHES = ImmutableMap.of(
    1000, "7f51adaf701441ee83459a3f705c1b86",
    100_000, "af7775de90d0b0c8bbc36273fbca26fe",
    100_000_000, "bfee52b33aa1552b9c1bfa8bcc41ae80"
  );

  private static Integer numberOfRecords;

  private static String filenamePrefix;

  private static Charset charset;

  @Rule
  public TestPipeline pipeline = TestPipeline.create();

  @Rule
  public TestPipeline readPipeline = TestPipeline.create();

  @BeforeClass
  public static void setup() {
    PipelineOptionsFactory.register(IOTestPipelineOptions.class);
    IOTestPipelineOptions options = TestPipeline
      .testingPipelineOptions()
      .as(IOTestPipelineOptions.class);

    filenamePrefix = appendTimestampSuffix(options.getFilenamePrefix());
    numberOfRecords = options.getNumberOfRecords();
    charset = Charset.forName(options.getCharset());
  }

  @Test
  public void writeThenReadViaSinkAndReadFiles() {
    PCollection<String> testFileNames = pipeline
      .apply("Generate sequence", GenerateSequence.from(0).to(numberOfRecords))
      .apply("Create Birds", MapElements.via(new LongToBird()))
      .apply("Write birds to xml files", FileIO.<Bird>write()
          .via(XmlIO.sink(Bird.class)
            .withRootElement("birds")
            .withCharset(charset))
          .to(filenamePrefix)
          .withPrefix("birds")
          .withSuffix(".xml"))
      .getPerDestinationOutputFilenames()
      .apply("Prevent fusion", Reshuffle.viaRandomKey())
      .apply("Get file names", Values.create());

    PCollection<Bird> birds = testFileNames
      .apply("Find files", FileIO.matchAll())
      .apply("Read matched files", FileIO.readMatches())
      .apply("Read xml files", XmlIO.<Bird>readFiles()
        .withRecordClass(Bird.class).withRootElement("birds")
        .withRecordElement("bird")
        .withCharset(charset));

    PCollection<String> consolidatedHashcode = birds
      .apply("Map birds to strings", MapElements.via(new BirdToString()))
      .apply("Calculate hashcode", Combine.globally(new HashingFn()));

    String expectedHash = IOTestHelper.getHashForRecordCount(numberOfRecords, EXPECTED_HASHES);
    PAssert.thatSingleton(consolidatedHashcode).isEqualTo(expectedHash);

    testFileNames.apply("Delete test files", ParDo.of(new DeleteFileFn())
        .withSideInputs(consolidatedHashcode.apply(View.asSingleton())));

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void writeThenReadViaWriteAndRead() {
    String filenamePattern = filenamePrefix + "*";

    pipeline
      .apply("Generate sequence", GenerateSequence.from(0).to(numberOfRecords))
      .apply("Create Birds", MapElements.via(new LongToBird()))
      .apply("Write birds to xml files", XmlIO.<Bird>write()
          .to(filenamePrefix)
          .withRecordClass(Bird.class)
          .withRootElement("birds")
          .withCharset(charset));

    pipeline.run().waitUntilFinish();

    PCollection<String> consolidatedHashcode = readPipeline
      .apply(XmlIO.<Bird>read()
        .from(filenamePattern)
        .withRecordClass(Bird.class)
        .withRootElement("birds")
        .withRecordElement("bird")
        .withCharset(charset))
      .apply("Map birds to strings", MapElements.via(new BirdToString()))
      .apply("Calculate hashcode", Combine.globally(new HashingFn()));

    String expectedHash = IOTestHelper.getHashForRecordCount(numberOfRecords, EXPECTED_HASHES);
    PAssert.thatSingleton(consolidatedHashcode).isEqualTo(expectedHash);

    readPipeline
      .apply("Get file names to delete", Create.of(filenamePattern))
      .apply("Delete test files", ParDo.of(new DeleteFileFn())
          .withSideInputs(consolidatedHashcode.apply(View.asSingleton())));

    readPipeline.run().waitUntilFinish();
  }

  private static class LongToBird extends SimpleFunction<Long, Bird> {
    @Override
    public Bird apply(Long input) {
      return new Bird("Testing", "Bird number " + input);
    }
  }

  private static class BirdToString extends SimpleFunction<Bird, String>{
    @Override
    public String apply(Bird input) {
      return input.toString();
    }
  }
}
