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
package ${package};

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.GcsOptions;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.util.GcsUtil;
import org.apache.beam.sdk.util.gcsfs.GcsPath;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * To keep {@link MinimalWordCountJava8} simple, it is not factored or testable. This test
 * file should be maintained with a copy of its code for a basic smoke test.
 */
@RunWith(JUnit4.class)
public class MinimalWordCountJava8Test implements Serializable {

  @Rule
  public TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);

  /**
   * A basic smoke test that ensures there is no crash at pipeline construction time.
   */
  @Test
  public void testMinimalWordCountJava8() throws Exception {
    p.getOptions().as(GcsOptions.class).setGcsUtil(buildMockGcsUtil());

    p.apply(TextIO.Read.from("gs://apache-beam-samples/shakespeare/*"))
     .apply(FlatMapElements.via((String word) -> Arrays.asList(word.split("[^a-zA-Z']+")))
         .withOutputType(TypeDescriptors.strings()))
     .apply(Filter.by((String word) -> !word.isEmpty()))
     .apply(Count.<String>perElement())
     .apply(MapElements
         .via((KV<String, Long> wordCount) -> wordCount.getKey() + ": " + wordCount.getValue())
         .withOutputType(TypeDescriptors.strings()))
     .apply(TextIO.Write.to("gs://your-output-bucket/and-output-prefix"));
  }

  private GcsUtil buildMockGcsUtil() throws IOException {
    GcsUtil mockGcsUtil = Mockito.mock(GcsUtil.class);

    // Any request to open gets a new bogus channel
    Mockito
        .when(mockGcsUtil.open(Mockito.any(GcsPath.class)))
        .then(new Answer<SeekableByteChannel>() {
          @Override
          public SeekableByteChannel answer(InvocationOnMock invocation) throws Throwable {
            return FileChannel.open(
                Files.createTempFile("channel-", ".tmp"),
                StandardOpenOption.CREATE, StandardOpenOption.DELETE_ON_CLOSE);
          }
        });

    // Any request for expansion returns a list containing the original GcsPath
    // This is required to pass validation that occurs in TextIO during apply()
    Mockito
        .when(mockGcsUtil.expand(Mockito.any(GcsPath.class)))
        .then(new Answer<List<GcsPath>>() {
          @Override
          public List<GcsPath> answer(InvocationOnMock invocation) throws Throwable {
            return ImmutableList.of((GcsPath) invocation.getArguments()[0]);
          }
        });

    return mockGcsUtil;
  }
}
