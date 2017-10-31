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

import java.io.File;
import java.io.FileFilter;
import java.text.ParseException;
import java.util.Date;

import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.common.HashingFn;
import org.apache.beam.sdk.io.common.IOTestPipelineOptions;
import org.apache.beam.sdk.io.common.TestTextLine;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.PCollection;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;


/**
 * A test of {@link org.apache.beam.sdk.io.TextIO}.
 *
 * <p>Run this test using the command below. Pass in connection information via PipelineOptions:
 * <pre>
 *  mvn -e -Pio-it verify -pl sdks/java/io/text -DintegrationTestPipelineOptions='[
 *  "--linesOfTextCount=100000" ]'
 * </pre>
 *
 * <p>For now, this test can be run on DirectRunner only.
 * */
@RunWith(JUnit4.class)
public class TextIOIT {

  private static String filenamePrefix;
  private static Long linesOfTextCount;

  @Rule
  public TestPipeline pipeline = TestPipeline.create();

  @BeforeClass
  public static void setup() throws ParseException {
    PipelineOptionsFactory.register(IOTestPipelineOptions.class);
    IOTestPipelineOptions options = TestPipeline.testingPipelineOptions()
        .as(IOTestPipelineOptions.class);

    if (options.getLinesOfTextCount() != null) {
      linesOfTextCount = options.getLinesOfTextCount();
    }

    filenamePrefix = generateFileBasename();
  }

  private static String generateFileBasename() {
    return String.format("TEXTIOIT_%s", new Date().getTime());
  }

  @After public void tearDown() throws Exception {
    deleteFiles();
  }

  private void deleteFiles() {
    File currentDirectory = new File(".");
    File[] filesToDelete = currentDirectory
        .listFiles(new FileFilter() {
          @Override
          public boolean accept(File pathname) {
            return (pathname.getName().startsWith(filenamePrefix));
          }
        });

    if (filesToDelete != null) {
      for (File f : filesToDelete) {
        f.delete();
      }
    }
  }

  @Test
  public void testWriteThenRead() {
    PCollection<String> consolidatedHashcode = pipeline
        .apply("Generate sequence", GenerateSequence.from(0).to(linesOfTextCount))
        .apply("Produce text lines",
            ParDo.of(new TestTextLine.DeterministicallyConstructTestTextLineFn()))
        .apply("Write content to files", TextIO.write().to(filenamePrefix).withOutputFilenames())
        .getPerDestinationOutputFilenames().apply(Values.<String>create())
        .apply("Read all files", TextIO.readAll())
        .apply("Calculate hashcode", Combine.globally(new HashingFn()).withoutDefaults());

    assertHashcodeIsOk(consolidatedHashcode);

    pipeline.run().waitUntilFinish();
  }

  /**
   * Assert that the obtained PCollection's consolidated hashcode equals a precalculated one.
   */
  private void assertHashcodeIsOk(PCollection<String> consolidatedContentHashcode) {
    String expectedHash = TestTextLine.getExpectedHashForLineCount(linesOfTextCount);
    PAssert.that(consolidatedContentHashcode).containsInAnyOrder(expectedHash);
  }
}
