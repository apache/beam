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
package org.apache.beam.sdk;

import static org.apache.beam.sdk.testing.FileChecksumMatcher.fileContentsHaveChecksum;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.UUID;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.util.FilePatternMatchingShardedFile;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Integration test for the support of {@link
 * org.apache.beam.sdk.transforms.DoFn.RequiresStableInput} annotation.
 */
@RunWith(JUnit4.class)
public class RequiresStableInputIT {

  private static final String VALUE = "value";
  // SHA-1 hash of string "value"
  private static final String VALUE_CHECKSUM = "f32b67c7e26342af42efabc674d441dca0a281c5";

  /** Assigns a random key to a value. */
  public static class PairWithRandomKeyFn extends SimpleFunction<String, KV<String, String>> {
    @Override
    public KV<String, String> apply(String value) {
      String key = UUID.randomUUID().toString();
      return KV.of(key, value);
    }
  }

  /** Simulates side effect by writing input to a file. */
  public static class MakeSideEffectAndThenFailFn extends DoFn<KV<String, String>, String> {
    private final String outputPrefix;
    private final SerializableFunction<Void, Void> firstTimeCallback;

    public MakeSideEffectAndThenFailFn(
        String outputPrefix, SerializableFunction<Void, Void> firstTimeCallback) {
      this.outputPrefix = outputPrefix;
      this.firstTimeCallback = firstTimeCallback;
    }

    @RequiresStableInput
    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      MatchResult matchResult = FileSystems.match(outputPrefix + "*");
      boolean firstTime = matchResult.metadata().isEmpty();

      KV<String, String> kv = c.element();
      writeTextToFileSideEffect(kv.getValue(), outputPrefix + kv.getKey());
      if (firstTime) {
        firstTimeCallback.apply(null);
      }
    }

    public static void writeTextToFileSideEffect(String text, String filename) throws IOException {
      ResourceId rid = FileSystems.matchNewResource(filename, false);
      try (WritableByteChannel chan = FileSystems.create(rid, "text/plain")) {
        chan.write(ByteBuffer.wrap(text.getBytes(StandardCharsets.UTF_8)));
      }
    }
  }

  @BeforeClass
  public static void setup() {
    PipelineOptionsFactory.register(TestPipelineOptions.class);
  }

  /**
   * Test for the support of {@link org.apache.beam.sdk.transforms.DoFn.RequiresStableInput} in both
   * {@link ParDo.SingleOutput} and {@link ParDo.MultiOutput}.
   *
   * <p>In each test, a singleton string value is paired with a random key. In the following
   * transform, the value is written to a file, whose path is specified by the random key, and then
   * the transform fails. When the pipeline retries, the latter transform should receive the same
   * input from the former transform, because its {@link DoFn} is annotated with {@link
   * org.apache.beam.sdk.transforms.DoFn.RequiresStableInput}, and it will not fail due to presence
   * of the file. Therefore, only one file for each transform is expected.
   */
  @Test
  public void testParDoRequiresStableInput() {
    TestPipelineOptions options =
        TestPipeline.testingPipelineOptions().as(TestPipelineOptions.class);

    ResourceId outputDir =
        FileSystems.matchNewResource(options.getTempRoot(), true)
            .resolve(
                String.format("requires-stable-input-%tF-%<tH-%<tM-%<tS-%<tL", new Date()),
                StandardResolveOptions.RESOLVE_DIRECTORY);
    String singleOutputPrefix =
        outputDir
            .resolve("pardo-single-output", StandardResolveOptions.RESOLVE_DIRECTORY)
            .resolve("key-", StandardResolveOptions.RESOLVE_FILE)
            .toString();
    String multiOutputPrefix =
        outputDir
            .resolve("pardo-multi-output", StandardResolveOptions.RESOLVE_DIRECTORY)
            .resolve("key-", StandardResolveOptions.RESOLVE_FILE)
            .toString();

    Pipeline p = Pipeline.create(options);

    SerializableFunction<Void, Void> firstTime =
        (SerializableFunction<Void, Void>)
            value -> {
              throw new RuntimeException(
                  "Deliberate failure: should happen only once for each application of the DoFn"
                      + " within the transform graph.");
            };

    PCollection<String> singleton = p.apply("CreatePCollectionOfOneValue", Create.of(VALUE));
    singleton
        .apply("Single-PairWithRandomKey", MapElements.via(new PairWithRandomKeyFn()))
        .apply(
            "Single-MakeSideEffectAndThenFail",
            ParDo.of(new MakeSideEffectAndThenFailFn(singleOutputPrefix, firstTime)));
    singleton
        .apply("Multi-PairWithRandomKey", MapElements.via(new PairWithRandomKeyFn()))
        .apply(
            "Multi-MakeSideEffectAndThenFail",
            ParDo.of(new MakeSideEffectAndThenFailFn(multiOutputPrefix, firstTime))
                .withOutputTags(new TupleTag<>(), TupleTagList.empty()));

    p.run().waitUntilFinish();

    assertThat(
        new FilePatternMatchingShardedFile(singleOutputPrefix + "*"),
        fileContentsHaveChecksum(VALUE_CHECKSUM));
    assertThat(
        new FilePatternMatchingShardedFile(multiOutputPrefix + "*"),
        fileContentsHaveChecksum(VALUE_CHECKSUM));
  }
}
