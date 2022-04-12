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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.google.common.io.ByteStreams;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class GcsMatchIT {
  /** Integration test for TextIO.MatchAll watching for file updates in gcs filesystem */
  @Test
  public void testGcsMatchContinuously() throws InterruptedException {
    TestPipelineOptions options =
        TestPipeline.testingPipelineOptions().as(TestPipelineOptions.class);
    assertNotNull(options.getTempRoot());
    options.setTempLocation(options.getTempRoot() + "/testGcsMatchContinuouslyTest");
    GcsOptions gcsOptions = options.as(GcsOptions.class);
    String dstFolderName =
        gcsOptions.getGcpTempLocation()
            + String.format(
                "/GcsMatchIT-%tF-%<tH-%<tM-%<tS-%<tL.testGcsMatchContinuously.copy/", new Date());
    final GcsPath watchPath = GcsPath.fromUri(dstFolderName);

    Pipeline p = Pipeline.create(options);

    PCollection<Metadata> matchAllUpdatedMetadata =
        p.apply("create for matchAll updated files", Create.of(watchPath.resolve("*").toString()))
            .apply(
                "matchAll updated",
                FileIO.matchAll()
                    .continuously(
                        Duration.millis(250),
                        Watch.Growth.afterTimeSinceNewOutput(Duration.standardSeconds(3)),
                        true));

    // Copy the files to the "watch" directory;
    Thread writer =
        new Thread(
            () -> {
              try {
                Thread.sleep(1000);
                writeBytesToFile(watchPath.resolve("first").toString(), 42);
                Thread.sleep(1000);
                writeBytesToFile(watchPath.resolve("first").toString(), 99);
                writeBytesToFile(watchPath.resolve("second").toString(), 42);
                Thread.sleep(1000);
                writeBytesToFile(watchPath.resolve("first").toString(), 37);
                writeBytesToFile(watchPath.resolve("second").toString(), 42);
                writeBytesToFile(watchPath.resolve("third").toString(), 99);

              } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
              }
            });

    List<String> expectedMatchAllUpdated =
        Arrays.asList("first", "first", "first", "second", "second", "third");
    PCollection<String> matchAllUpdatedCount =
        matchAllUpdatedMetadata.apply(
            "pick up matchAll file names",
            MapElements.into(TypeDescriptors.strings())
                .via((metadata) -> metadata.resourceId().getFilename()));
    PAssert.that(matchAllUpdatedCount).containsInAnyOrder(expectedMatchAllUpdated);

    writer.start();
    PipelineResult result = p.run();
    State state = result.waitUntilFinish();
    assertEquals(State.DONE, state);

    writer.join();
  }

  private static void writeBytesToFile(String gcsPath, int length) throws IOException {
    ResourceId newFileResourceId = FileSystems.matchNewResource(gcsPath, false);
    try (ByteArrayInputStream in = new ByteArrayInputStream(new byte[length]);
        ReadableByteChannel readerChannel = Channels.newChannel(in);
        WritableByteChannel writerChannel = FileSystems.create(newFileResourceId, MimeTypes.TEXT)) {
      ByteStreams.copy(readerChannel, writerChannel);
    }
  }
}
