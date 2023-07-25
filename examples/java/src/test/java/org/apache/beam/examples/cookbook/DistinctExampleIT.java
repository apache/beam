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

/**
 * An end-to-end test for {@link org.apache.beam.examples.cookbook.DistinctExample}.
 *
 * <p>This tests uses as input text of King Lear, by William Shakespeare as plaintext files, and
 * will remove any duplicate lines from this file. (The output does not preserve any input order).
 *
 * <p>Running instructions:
 *
 * <pre>
 *  ./gradlew integrationTest -p examples/java/ -DintegrationTestPipelineOptions='[
 *  "--tempLocation=gs://apache-beam-testing-developers/"]'
 *  --tests org.apache.beam.examples.cookbook.DistinctExampleIT
 *  -DintegrationTestRunner=direct
 * </pre>
 *
 * <p>Check {@link org.apache.beam.examples.cookbook.DistinctExample} form more configuration
 * options via PipelineOptions.
 */
@RunWith(JUnit4.class)
public class DistinctExampleIT {

  private static final String EXPECTED_OUTPUT_CHECKSUM = "474c8925d94dce3b8147e6ec88c551c9066effd0";
  private static final Pattern DEFAULT_SHARD_TEMPLATE =
      Pattern.compile("(?x) \\S* (?<shardnum> \\d+) -of- (?<numshards> \\d+)");

  /** Options for the DistinctExample Integration Test. */
  public interface DistinctExampleOptions extends TestPipelineOptions, DistinctExample.Options {}

  @BeforeClass
  public static void setUp() {
    PipelineOptionsFactory.register(TestPipelineOptions.class);
  }

  @Test
  public void testE2EDistinctExample() throws Exception {
    DistinctExampleOptions options =
        TestPipeline.testingPipelineOptions().as(DistinctExampleOptions.class);
    options.setOutput(
        FileSystems.matchNewResource(options.getTempRoot(), true)
            .resolve(
                String.format("DistinctExample-%tF-%<tH-%<tM-%<tS-%<tL", new Date()),
                ResolveOptions.StandardResolveOptions.RESOLVE_DIRECTORY)
            .resolve("output", ResolveOptions.StandardResolveOptions.RESOLVE_DIRECTORY)
            .resolve("results", ResolveOptions.StandardResolveOptions.RESOLVE_FILE)
            .toString());
    DistinctExample.runDistinctExample(options);

    assertThat(
        new NumberedShardedFile(options.getOutput() + "*-of-*", DEFAULT_SHARD_TEMPLATE),
        fileContentsHaveChecksum(EXPECTED_OUTPUT_CHECKSUM));
  }
}
