/*
 * Copyright (c) 2015, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package com.cloudera.dataflow.spark;

import com.google.cloud.dataflow.examples.WordCount;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.common.base.Charsets;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class NumShardsTest {

  private static final String[] WORDS_ARRAY = {
      "hi there", "hi", "hi sue bob",
      "hi sue", "", "bob hi"};
  private static final List<String> WORDS = Arrays.asList(WORDS_ARRAY);

  private File outputDir;

  @Rule
  public final TemporaryFolder tmpDir = new TemporaryFolder();

  @Before
  public void setUp() throws IOException {
    outputDir = tmpDir.newFolder("out");
    outputDir.delete();
  }

  @Test
  public void testText() throws Exception {
    SparkPipelineOptions options = SparkPipelineOptionsFactory.create();
    options.setRunner(SparkPipelineRunner.class);
    Pipeline p = Pipeline.create(options);
    PCollection<String> inputWords = p.apply(Create.of(WORDS)).setCoder(StringUtf8Coder.of());
    PCollection<String> output = inputWords.apply(new WordCount.CountWords())
        .apply(ParDo.of(new WordCount.FormatAsTextFn()));
    output.apply(TextIO.Write.to(outputDir.getAbsolutePath()).withNumShards(3).withSuffix(".txt"));
    EvaluationResult res = SparkPipelineRunner.create().run(p);
    res.close();

    int count = 0;
    Set<String> expected = Sets.newHashSet("hi: 5", "there: 1", "sue: 2", "bob: 2");
    for (File f : tmpDir.getRoot().listFiles(new FileFilter() {
      @Override public boolean accept(File pathname) {
        return pathname.getName().matches("out-.*\\.txt");
      }
    })) {
      count++;
      for (String line : Files.readLines(f, Charsets.UTF_8)) {
        assertTrue(line + " not found", expected.remove(line));
      }
    }
    assertEquals(3, count);
    assertTrue(expected.isEmpty());
  }

}
