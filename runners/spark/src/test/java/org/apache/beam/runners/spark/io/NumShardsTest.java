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
package org.apache.beam.runners.spark.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.apache.beam.runners.spark.examples.WordCount;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.Files;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Number of shards test. */
public class NumShardsTest {

  private static final String[] WORDS_ARRAY = {
    "hi there", "hi", "hi sue bob",
    "hi sue", "", "bob hi"
  };
  private static final List<String> WORDS = Arrays.asList(WORDS_ARRAY);

  private File outputDir;

  @Rule public final TemporaryFolder tmpDir = new TemporaryFolder();

  @Rule public final TestPipeline p = TestPipeline.create();

  @Before
  public void setUp() throws IOException {
    outputDir = tmpDir.newFolder("out");
    outputDir.delete();
  }

  @Test
  public void testText() throws Exception {
    PCollection<String> inputWords = p.apply(Create.of(WORDS).withCoder(StringUtf8Coder.of()));
    PCollection<String> output =
        inputWords
            .apply(new WordCount.CountWords())
            .apply(MapElements.via(new WordCount.FormatAsTextFn()));
    output.apply(
        TextIO.write().to(outputDir.getAbsolutePath()).withNumShards(3).withSuffix(".txt"));
    p.run().waitUntilFinish();

    int count = 0;
    Set<String> expected = Sets.newHashSet("hi: 5", "there: 1", "sue: 2", "bob: 2");
    for (File f :
        tmpDir.getRoot().listFiles(pathname -> pathname.getName().matches("out-.*\\.txt"))) {
      count++;
      for (String line : Files.readLines(f, StandardCharsets.UTF_8)) {
        assertTrue(line + " not found", expected.remove(line));
      }
    }
    assertEquals(3, count);
    assertTrue(expected.toString(), expected.isEmpty());
  }
}
