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
package org.apache.beam.sdk.extensions.gcp.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Collections;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.extensions.gcp.util.GcsUtil.CreateOptions;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.testing.UsesKms;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Integration tests for {@link GcsUtil}. These tests are designed to run against production Google
 * Cloud Storage.
 *
 * <p>This is a runnerless integration test, even though the Beam IT framework assumes one. Thus,
 * this test should only be run against single runner (such as DirectRunner).
 */
@RunWith(JUnit4.class)
@Category(UsesKms.class)
public class GcsUtilIT {
  /** Tests a rewrite operation that requires multiple API calls (using a continuation token). */
  @Test
  public void testRewriteMultiPart() throws IOException {
    TestPipelineOptions options =
        TestPipeline.testingPipelineOptions().as(TestPipelineOptions.class);
    // Using a KMS key is necessary to trigger multi-part rewrites (bucket is created
    // with a bucket default key).
    assertNotNull(options.getTempRoot());
    options.setTempLocation(
        FileSystems.matchNewDirectory(options.getTempRoot(), "testRewriteMultiPart").toString());

    GcsOptions gcsOptions = options.as(GcsOptions.class);
    GcsUtil gcsUtil = gcsOptions.getGcsUtil();
    String srcFilename = "gs://dataflow-samples/wikipedia_edits/wiki_data-000000000000.json";
    String dstFilename =
        gcsOptions.getGcpTempLocation()
            + String.format(
                "/GcsUtilIT-%tF-%<tH-%<tM-%<tS-%<tL.testRewriteMultiPart.copy", new Date());
    gcsUtil.maxBytesRewrittenPerCall = 50L * 1024 * 1024;
    gcsUtil.numRewriteTokensUsed = new AtomicInteger();

    gcsUtil.copy(Lists.newArrayList(srcFilename), Lists.newArrayList(dstFilename));

    assertThat(gcsUtil.numRewriteTokensUsed.get(), equalTo(3));
    assertThat(
        gcsUtil.getObject(GcsPath.fromUri(srcFilename)).getMd5Hash(),
        equalTo(gcsUtil.getObject(GcsPath.fromUri(dstFilename)).getMd5Hash()));

    gcsUtil.remove(Lists.newArrayList(dstFilename));
  }

  // TODO: once the gRPC feature is in public GA, we will have to refactor this test.
  // As gRPC will be automatically enabled in each bucket by then, we will no longer need to check
  // the failed case. The interface of GcsGrpcOptions can also be removed.
  @Test
  public void testWriteAndReadGcsWithGrpc() throws IOException {
    final String outputPattern =
        "%s/GcsUtilIT-%tF-%<tH-%<tM-%<tS-%<tL.testWriteAndReadGcsWithGrpc.txt";
    final String testContent = "This is a test string.";

    PipelineOptionsFactory.register(GcsGrpcOptions.class);

    TestPipelineOptions options =
        TestPipeline.testingPipelineOptions().as(TestPipelineOptions.class);

    // set the experimental flag to enable grpc
    ExperimentalOptions experimental = options.as(ExperimentalOptions.class);
    experimental.setExperiments(Collections.singletonList("use_grpc_for_gcs"));

    GcsOptions gcsOptions = options.as(GcsOptions.class);
    GcsUtil gcsUtil = gcsOptions.getGcsUtil();
    assertNotNull(gcsUtil);

    // Write a test file in a bucket without gRPC enabled.
    // This assumes that GCS gRPC feature is not enabled in every bucket by default.
    assertNotNull(options.getTempRoot());
    String tempLocationWithoutGrpc = options.getTempRoot() + "/temp";
    String wrongFilename = String.format(outputPattern, tempLocationWithoutGrpc, new Date());
    assertThrows(IOException.class, () -> writeGcsTextFile(gcsUtil, wrongFilename, testContent));

    // Write a test file in a bucket with gRPC enabled.
    GcsGrpcOptions grpcOptions = options.as(GcsGrpcOptions.class);
    assertNotNull(grpcOptions.getGrpcTempRoot());
    String tempLocationWithGrpc = grpcOptions.getGrpcTempRoot() + "/temp";
    String filename = String.format(outputPattern, tempLocationWithGrpc, new Date());
    writeGcsTextFile(gcsUtil, filename, testContent);

    // Read the test file back and verify
    assertEquals(testContent, readGcsTextFile(gcsUtil, filename));

    gcsUtil.remove(Collections.singletonList(filename));
  }

  public interface GcsGrpcOptions extends PipelineOptions {
    /** Get tempRoot in a gRPC-enabled bucket. */
    @Description("TempRoot in a gRPC-enabled bucket")
    String getGrpcTempRoot();

    /** Set the tempRoot in a gRPC-enabled bucket. */
    void setGrpcTempRoot(String grpcTempRoot);
  }

  void writeGcsTextFile(GcsUtil gcsUtil, String filename, String content) throws IOException {
    GcsPath gcsPath = GcsPath.fromUri(filename);
    try (WritableByteChannel channel =
        gcsUtil.create(
            gcsPath, CreateOptions.builder().setContentType("text/plain;charset=utf-8").build())) {
      channel.write(ByteString.copyFromUtf8(content).asReadOnlyByteBuffer());
    }
  }

  String readGcsTextFile(GcsUtil gcsUtil, String filename) throws IOException {
    GcsPath gcsPath = GcsPath.fromUri(filename);
    try (ByteStringOutputStream output = new ByteStringOutputStream()) {
      try (ReadableByteChannel channel = gcsUtil.open(gcsPath)) {
        ByteBuffer bb = ByteBuffer.allocate(16);
        while (channel.read(bb) != -1) {
          output.write(bb.array(), 0, bb.capacity() - bb.remaining());
          bb.clear();
        }
      }
      return output.toByteString().toStringUtf8();
    }
  }
}
