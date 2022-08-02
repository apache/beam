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
package org.apache.beam.examples.cookbook;

import static org.apache.beam.sdk.testing.FileChecksumMatcher.fileContentsHaveChecksum;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Date;
import java.util.regex.Pattern;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResolveOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.util.NumberedShardedFile;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** An end-to-end test for {@link org.apache.beam.examples.cookbook.JoinExamples}. */
@RunWith(JUnit4.class)
public class JoinExamplesIT {
  private static final String EXPECTED_OUTPUT_CHECKSUM = "22394366a77255b6941ec747794df4f51f73c07d";
  private static final Pattern DEFAULT_SHARD_TEMPLATE =
      Pattern.compile("(?x) \\S* (?<shardnum> \\d+) -of- (?<numshards> \\d+)");

  /** Options for the JoinExamples Integration Test. */
  public interface JoinExamplesOptions extends TestPipelineOptions, JoinExamples.Options {}

  @BeforeClass
  public static void setUp() {
    PipelineOptionsFactory.register(TestPipelineOptions.class);
  }

  @Test
  public void testE2EJoinExamples() throws Exception {
    JoinExamplesIT.JoinExamplesOptions options =
        TestPipeline.testingPipelineOptions().as(JoinExamplesIT.JoinExamplesOptions.class);
    options.setOutput(
        FileSystems.matchNewResource(options.getTempRoot(), true)
            .resolve(
                String.format("JoinExamples-%tF-%<tH-%<tM-%<tS-%<tL", new Date()),
                ResolveOptions.StandardResolveOptions.RESOLVE_DIRECTORY)
            .resolve("output", ResolveOptions.StandardResolveOptions.RESOLVE_DIRECTORY)
            .resolve("results", ResolveOptions.StandardResolveOptions.RESOLVE_FILE)
            .toString());
    JoinExamples.runJoinExamples(options);

    assertThat(
        new NumberedShardedFile(options.getOutput() + "*-of-*", DEFAULT_SHARD_TEMPLATE),
        fileContentsHaveChecksum(EXPECTED_OUTPUT_CHECKSUM));
  }
}
