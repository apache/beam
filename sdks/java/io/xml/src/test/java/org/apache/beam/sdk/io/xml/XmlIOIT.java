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

import com.google.common.collect.ImmutableMap;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.Map;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.common.DeleteFileFn;
import org.apache.beam.sdk.io.common.HashingFn;
import org.apache.beam.sdk.io.common.IOTestPipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
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

  private static Integer numberOfRecords;

  private static String filenamePrefix;

  private static Charset charset;

  @Rule
  public TestPipeline pipeline = TestPipeline.create();

  @BeforeClass
  public static void setup() {
    PipelineOptionsFactory.register(IOTestPipelineOptions.class);
    IOTestPipelineOptions options = TestPipeline
      .testingPipelineOptions()
      .as(IOTestPipelineOptions.class);

    filenamePrefix = appendTimestampToPrefix(options.getFilenamePrefix());
    numberOfRecords = options.getNumberOfRecords();
    charset = Charset.forName(options.getCharset());
  }

  private static String appendTimestampToPrefix(String filenamePrefix) {
    return String.format("%s_%s", filenamePrefix, new Date().getTime());
  }

  @Test
  public void writeThenReadViaSinkAndReadFiles() {
    PCollection<String> testFileNames = pipeline
      .apply("Generate sequence", GenerateSequence.from(0).to(numberOfRecords))
      .apply("Create Birds", MapElements.via(new LongToBird()))
      .apply("Write birds to xml files",
        FileIO.<Bird>write()
          .via(XmlIO.sink(Bird.class).withRootElement("birds").withCharset(charset))
          .to(filenamePrefix).withPrefix("birds").withSuffix(".xml"))
      .getPerDestinationOutputFilenames().apply("Get filenames", Values.create());

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

    String expectedHash = getExpectedHashForRecordCount(numberOfRecords);
    PAssert.thatSingleton(consolidatedHashcode).isEqualTo(expectedHash);

    testFileNames.apply("Delete test files", ParDo.of(new DeleteFileFn())
        .withSideInputs(consolidatedHashcode.apply(View.asSingleton())));

    pipeline.run().waitUntilFinish();
  }

  private static String getExpectedHashForRecordCount(int numberOfRecords) {
    Map<Integer, String> expectedHashes = ImmutableMap.of(
      100_000, "af7775de90d0b0c8bbc36273fbca26fe"
    );

    String hash = expectedHashes.get(numberOfRecords);
    if (hash == null) {
      throw new UnsupportedOperationException(
        String.format("No hash for that line count: %s", numberOfRecords)
      );
    }
    return hash;
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
