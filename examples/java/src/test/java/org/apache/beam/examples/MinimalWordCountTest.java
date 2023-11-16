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
package org.apache.beam.examples;

import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.extensions.gcp.util.GcsUtil;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

/**
 * To keep {@link MinimalWordCount} simple, it is not factored or testable. This test file should be
 * maintained with a copy of its code for a basic smoke test.
 */
@RunWith(JUnit4.class)
public class MinimalWordCountTest implements Serializable {

  @Rule public TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);

  /** A basic smoke test that ensures there is no crash at pipeline construction time. */
  @Test
  public void testMinimalWordCount() throws Exception {
    p.getOptions().as(GcsOptions.class).setGcsUtil(buildMockGcsUtil());

    p.apply(TextIO.read().from("gs://apache-beam-samples/shakespeare/*"))
        .apply(
            FlatMapElements.into(TypeDescriptors.strings())
                .via((String word) -> Arrays.asList(word.split("[^a-zA-Z']+"))))
        .apply(Filter.by((String word) -> !word.isEmpty()))
        .apply(Count.perElement())
        .apply(
            MapElements.into(TypeDescriptors.strings())
                .via(
                    (KV<String, Long> wordCount) ->
                        wordCount.getKey() + ": " + wordCount.getValue()))
        .apply(TextIO.write().to("gs://your-output-bucket/and-output-prefix"));
  }

  private GcsUtil buildMockGcsUtil() throws IOException {
    GcsUtil mockGcsUtil = Mockito.mock(GcsUtil.class);

    // Any request to open gets a new bogus channel
    Mockito.when(mockGcsUtil.open(Mockito.any(GcsPath.class)))
        .then(
            invocation ->
                FileChannel.open(
                    Files.createTempFile("channel-", ".tmp"),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.DELETE_ON_CLOSE));

    // Any request for expansion returns a list containing the original GcsPath
    // This is required to pass validation that occurs in TextIO during apply()
    Mockito.when(mockGcsUtil.expand(Mockito.any(GcsPath.class)))
        .then(invocation -> ImmutableList.of((GcsPath) invocation.getArguments()[0]));

    return mockGcsUtil;
  }
}
