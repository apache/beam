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

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import java.io.IOException;
import java.text.ParseException;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.common.HashingFn;
import org.apache.beam.sdk.io.common.IOTestPipelineOptions;
import org.apache.beam.sdk.io.common.TestTextLine;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.ResourceId;
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
 * An integration test for {@link org.apache.beam.sdk.io.TextIO}.
 *
 * <p>Run this test using the command below. Pass in connection information via PipelineOptions:
 * <pre>
 *  mvn -e -Pio-it verify -pl sdks/java/io/text -DintegrationTestPipelineOptions='[
 *  "--numberOfRecords=100000" ]'
 * </pre>
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

    linesOfTextCount = options.getNumberOfRecords();
    filenamePrefix = generateFileBasename();
  }

  @After
  public void tearDown() throws Exception {
    MatchResult match = Iterables.getOnlyElement(
        FileSystems.match(Collections.singletonList(String.format("%s*", filenamePrefix))));
    FileSystems.delete(toResourceIds(match));
  }

  private Collection<ResourceId> toResourceIds(MatchResult match) throws IOException {
    return FluentIterable.from(match.metadata())
        .transform(new Function<MatchResult.Metadata, ResourceId>() {
          @Override
          public ResourceId apply(MatchResult.Metadata metadata) {
            return metadata.resourceId();
          }
        }).toList();
  }

  private static String generateFileBasename() {
    return String.format("TEXTIOIT_%s", new Date().getTime());
  }

  @Test
  public void writeThenReadAll() {
    PCollection<String> consolidatedHashcode = pipeline
        .apply("Generate sequence", GenerateSequence.from(0).to(linesOfTextCount))
        .apply("Produce text lines",
            ParDo.of(new TestTextLine.DeterministicallyConstructTestTextLineFn()))
        .apply("Write content to files", TextIO.write().to(filenamePrefix).withOutputFilenames())
        .getPerDestinationOutputFilenames().apply(Values.<String>create())
        .apply("Read all files", TextIO.readAll())
        .apply("Calculate hashcode", Combine.globally(new HashingFn()));

    String expectedHash = TestTextLine.getExpectedHashForLineCount(linesOfTextCount);
    PAssert.thatSingleton(consolidatedHashcode).isEqualTo(expectedHash);

    pipeline.run().waitUntilFinish();
  }
}
