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
package org.apache.beam.sdk.io.gcp.storage;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;

import com.google.common.collect.Iterables;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Date;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.Sink;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.TFRecordIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.FileChecksumMatcher;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.GcsUtil;
import org.apache.beam.sdk.util.gcsfs.GcsPath;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

// Run a specific test using:
//   ./gradlew :beam-sdks-java-io-google-cloud-platform:integrationTest --tests
// GcsKmsKeyIT.testFileIOWithKmsKey --info

/** Integration test for GCS CMEK support. */
@RunWith(JUnit4.class)
public class GcsKmsKeyIT {

  private static final String INPUT_FILE = "gs://dataflow-samples/shakespeare/kinglear.txt";
  private static final String EXPECTED_CHECKSUM = "b9778bfac7fa8b934e42a322ef4bd4706b538fd0";
  private static final String KMS_KEY =
      "projects/apache-beam-testing/locations/global/keyRings/beam-it/cryptoKeys/test";
  // The key used has key rotation disabled such that the current version is always 1.
  private static final String KMS_KEY_WITH_VERSION =
      "projects/apache-beam-testing/locations/global/keyRings/beam-it/cryptoKeys/test/cryptoKeyVersions/1";

  @BeforeClass
  public static void setup() {
    PipelineOptionsFactory.register(TestPipelineOptions.class);
  }

  /**
   * Tests TextIO writing to GCS with kmsKey. This includes writing temporary files with kmsKey and
   * performing a rename operation with destKmsKey. Verifies that resulting output uses specified
   * key and is readable. Does not verify temporary file encryption.
   */
  @Test
  public void testTextIOWithKmsKey() {
    TestPipelineOptions options =
        TestPipeline.testingPipelineOptions().as(TestPipelineOptions.class);

    ResourceId filenamePrefix =
        FileSystems.matchNewResource(options.getTempRoot(), true)
            .resolve(
                String.format("GcsKmsKeyIT-%tF-%<tH-%<tM-%<tS-%<tL.output", new Date()),
                StandardResolveOptions.RESOLVE_FILE);

    Pipeline p = Pipeline.create(options);
    p.apply("ReadLines", TextIO.read().from(INPUT_FILE))
        .apply("WriteLines", TextIO.write().to(filenamePrefix).withKmsKey(KMS_KEY));

    PipelineResult result = p.run();
    State state = result.waitUntilFinish();
    assertThat(state, equalTo(State.DONE));

    String filePattern = filenamePrefix + "*-of-*";
    assertThat(result, new FileChecksumMatcher(EXPECTED_CHECKSUM, filePattern));

    try {
      MatchResult matchResult =
          Iterables.getOnlyElement(FileSystems.match(Collections.singletonList(filePattern)));
      GcsOptions gcsOptions = options.as(GcsOptions.class);
      GcsUtil gcsUtil = gcsOptions.getGcsUtil();
      for (Metadata metadata : matchResult.metadata()) {
        String kmsKey = gcsUtil.kmsKey(GcsPath.fromUri(metadata.resourceId().toString()));
        assertThat(metadata.resourceId().toString(), kmsKey, equalTo(KMS_KEY_WITH_VERSION));
      }
    } catch (IOException e) {
      throw new AssertionError(e);
    }
  }

  private static class TextSink implements Sink<String> {
    private PrintWriter writer;

    @Override
    public void open(WritableByteChannel channel) throws IOException {
      writer = new PrintWriter(Channels.newWriter(channel, StandardCharsets.UTF_8.name()));
    }

    @Override
    public void write(String element) throws IOException {
      writer.println(element);
    }

    @Override
    public void flush() throws IOException {
      writer.flush();
    }
  }

  /** Like testTextIOWithKmsKey, but using FileIO. */
  @Test
  public void testFileIOWithKmsKey() {
    TestPipelineOptions options =
        TestPipeline.testingPipelineOptions().as(TestPipelineOptions.class);

    ResourceId outputDirectory =
        FileSystems.matchNewResource(options.getTempRoot(), true)
            .resolve(
                String.format("GcsKmsKeyIT-%tF-%<tH-%<tM-%<tS-%<tL", new Date()),
                StandardResolveOptions.RESOLVE_FILE);
    String filenamePrefix = "output";

    Pipeline p = Pipeline.create(options);
    p.apply("ReadLines", TextIO.read().from(INPUT_FILE))
        .apply(
            "WriteLines",
            FileIO.<String>write()
                .via(new TextSink())
                .to(outputDirectory.toString())
                .withPrefix(filenamePrefix)
                .withKmsKey(KMS_KEY));

    PipelineResult result = p.run();
    State state = result.waitUntilFinish();
    assertThat(state, equalTo(State.DONE));

    String filePattern = outputDirectory + "/" + filenamePrefix + "*-of-*";
    assertThat(result, new FileChecksumMatcher(EXPECTED_CHECKSUM, filePattern));

    try {
      MatchResult matchResult =
          Iterables.getOnlyElement(FileSystems.match(Collections.singletonList(filePattern)));
      GcsOptions gcsOptions = options.as(GcsOptions.class);
      GcsUtil gcsUtil = gcsOptions.getGcsUtil();
      for (Metadata metadata : matchResult.metadata()) {
        String kmsKey = gcsUtil.kmsKey(GcsPath.fromUri(metadata.resourceId().toString()));
        assertThat(metadata.resourceId().toString(), kmsKey, equalTo(KMS_KEY_WITH_VERSION));
      }
    } catch (IOException e) {
      throw new AssertionError(e);
    }
  }

  /**
   * Like testTextIOWithKmsKey, but using AvroIO.
   *
   * <p>Doesn't verify output contents.
   */
  @Test
  public void testAvroIOWithKmsKey() {
    TestPipelineOptions options =
        TestPipeline.testingPipelineOptions().as(TestPipelineOptions.class);

    ResourceId filenamePrefix =
        FileSystems.matchNewResource(options.getTempRoot(), true)
            .resolve(
                String.format("GcsKmsKeyIT-%tF-%<tH-%<tM-%<tS-%<tL.output", new Date()),
                StandardResolveOptions.RESOLVE_FILE);

    Pipeline p = Pipeline.create(options);
    p.apply("ReadLines", TextIO.read().from(INPUT_FILE))
        .apply("WriteLines", AvroIO.write(String.class).to(filenamePrefix).withKmsKey(KMS_KEY));

    PipelineResult result = p.run();
    State state = result.waitUntilFinish();
    assertThat(state, equalTo(State.DONE));

    String filePattern = filenamePrefix + "*-of-*";
    try {
      MatchResult matchResult =
          Iterables.getOnlyElement(FileSystems.match(Collections.singletonList(filePattern)));
      GcsOptions gcsOptions = options.as(GcsOptions.class);
      GcsUtil gcsUtil = gcsOptions.getGcsUtil();
      for (Metadata metadata : matchResult.metadata()) {
        GcsPath gcsPath = GcsPath.fromUri(metadata.resourceId().toString());
        assertThat(gcsUtil.fileSize(gcsPath), greaterThan(0L));
        String kmsKey = gcsUtil.kmsKey(gcsPath);
        assertThat(metadata.resourceId().toString(), kmsKey, equalTo(KMS_KEY_WITH_VERSION));
      }
    } catch (IOException e) {
      throw new AssertionError(e);
    }
  }

  private static class StringToBytesFn extends DoFn<String, byte[]> {
    @ProcessElement
    public void process(@Element String e, OutputReceiver<byte[]> output) {
      output.output(e.getBytes(StandardCharsets.UTF_8));
    }
  }

  /**
   * Like testTextIOWithKmsKey, but using TFRecordIO.
   *
   * <p>Doesn't verify output contents.
   */
  @Test
  public void testTFRecordIOWithKmsKey() {
    TestPipelineOptions options =
        TestPipeline.testingPipelineOptions().as(TestPipelineOptions.class);

    ResourceId filenamePrefix =
        FileSystems.matchNewResource(options.getTempRoot(), true)
            .resolve(
                String.format("GcsKmsKeyIT-%tF-%<tH-%<tM-%<tS-%<tL.output", new Date()),
                StandardResolveOptions.RESOLVE_FILE);

    Pipeline p = Pipeline.create(options);
    p.apply("ReadLines", TextIO.read().from(INPUT_FILE))
        .apply("LinesToBytes", ParDo.of(new StringToBytesFn()))
        .apply("WriteBytes", TFRecordIO.write().to(filenamePrefix).withKmsKey(KMS_KEY));

    PipelineResult result = p.run();
    State state = result.waitUntilFinish();
    assertThat(state, equalTo(State.DONE));

    String filePattern = filenamePrefix + "*-of-*";
    try {
      MatchResult matchResult =
          Iterables.getOnlyElement(FileSystems.match(Collections.singletonList(filePattern)));
      GcsOptions gcsOptions = options.as(GcsOptions.class);
      GcsUtil gcsUtil = gcsOptions.getGcsUtil();
      for (Metadata metadata : matchResult.metadata()) {
        GcsPath gcsPath = GcsPath.fromUri(metadata.resourceId().toString());
        assertThat(gcsUtil.fileSize(gcsPath), greaterThan(0L));
        String kmsKey = gcsUtil.kmsKey(gcsPath);
        assertThat(metadata.resourceId().toString(), kmsKey, equalTo(KMS_KEY_WITH_VERSION));
      }
    } catch (IOException e) {
      throw new AssertionError(e);
    }
  }
}
