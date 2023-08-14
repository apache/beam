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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Date;
import java.util.Objects;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.FluentIterable;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.ByteStreams;
import org.joda.time.Duration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class GcsMatchIT {
  /** A thread that write to Gcs continuously. */
  private static class WriteToPathContinuously extends Thread {
    public WriteToPathContinuously(Path writePath, long interval) {
      this.writePath = writePath;
      this.interval = interval;
    }

    @Override
    public void run() {
      int fileSize = 1;
      // write a file at the beginning
      writeBytesToFile(writePath.resolve("first").toString(), fileSize);

      while (!Thread.interrupted() && fileSize < maxFileSize) {
        try {
          Thread.sleep(interval);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        // write another file continuously
        writeBytesToFile(writePath.resolve("second").toString(), fileSize);
        fileSize += 1;
      }
      if (fileSize >= maxFileSize) {
        throw new RuntimeException("Maximum number of write reached.");
      }
    }

    private static void writeBytesToFile(String path, int length) {
      ResourceId newFileResourceId = FileSystems.matchNewResource(path, false);
      try (ByteArrayInputStream in = new ByteArrayInputStream(new byte[length]);
          ReadableByteChannel readerChannel = Channels.newChannel(in);
          WritableByteChannel writerChannel =
              FileSystems.create(newFileResourceId, MimeTypes.TEXT)) {
        ByteStreams.copy(readerChannel, writerChannel);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    private final Path writePath;
    private final long interval;
    private static final long maxFileSize = 1000;
  }

  private static class CheckPathFn implements SerializableFunction<Iterable<Metadata>, Void> {

    @Override
    public Void apply(Iterable<Metadata> input) {
      long countFirst = 0; // count the matches of file "first"
      long countSecond = 0; // count the matches of file "second"
      long sumSecondSize = 0; // sum the sizes of file "second"
      long maxSecondSize = 0; // max size observed for file "second"
      for (MatchResult.Metadata metadata : input) {
        String filename = Objects.requireNonNull(metadata.resourceId().getFilename());
        switch (filename) {
          case "first":
            countFirst += 1;
            break;
          case "second":
            countSecond += 1;
            sumSecondSize += metadata.sizeBytes();
            maxSecondSize = Math.max(maxSecondSize, metadata.sizeBytes());
            break;
          default:
            throw new AssertionError("Get unexpected filename " + filename);
        }
      }
      // file "first" is expected to appear exactly once
      assertEquals(1, countFirst);
      // file "second" is expected to appear more than once
      assertEquals(true, countSecond > 1);
      // file "second" is expected to appear in growing sizes each time by one byte
      assertEquals((maxSecondSize * 2L - countSecond + 1) * countSecond / 2, sumSecondSize);

      return null;
    }
  }

  @BeforeClass
  public static void setUp() throws Exception {
    options = TestPipeline.testingPipelineOptions().as(TestPipelineOptions.class);
    assertNotNull(options.getTempRoot());
    options.setTempLocation(options.getTempRoot() + "/GcsMatchIT");
    GcsOptions gcsOptions = options.as(GcsOptions.class);
    String dstFolderName =
        gcsOptions.getGcpTempLocation()
            + String.format("/testGcsMatchContinuously.%tF-%<tH-%<tM-%<tS-%<tL/", new Date());
    watchPath = GcsPath.fromUri(dstFolderName);
  }

  /** Integration test for TextIO.MatchAll watching for file updates in gcs filesystem. */
  @Test
  public void testGcsMatchContinuously() {
    Pipeline p = Pipeline.create(options);
    PCollection<GcsPath> path = p.apply(Create.of(Collections.singletonList(watchPath)));
    PCollection<MatchResult.Metadata> records =
        path.apply(
                MapElements.into(TypeDescriptors.strings())
                    .via((gcsPath) -> gcsPath.resolve("*").toString()))
            .apply(
                "matchAll updated",
                FileIO.matchAll()
                    .continuously(
                        Duration.millis(500),
                        Watch.Growth.afterTotalOf(Duration.standardSeconds(10)),
                        true));

    PAssert.that(records).satisfies(new CheckPathFn());

    Thread writer = new WriteToPathContinuously(watchPath, 2000);
    writer.start();
    PipelineResult result = p.run();
    State state = result.waitUntilFinish();

    writer.interrupt();
    assertEquals(State.DONE, state);
  }

  @AfterClass
  public static void tearDown() throws IOException {
    MatchResult matchResult = FileSystems.match(watchPath.resolve("*").toString());
    ImmutableList<ResourceId> resourceIdList =
        FluentIterable.from(matchResult.metadata())
            .transform(metadata -> (metadata.resourceId()))
            .toList();
    // delete temporary files
    FileSystems.delete(resourceIdList);
    // delete temporary folder
    FileSystems.delete(
        Collections.singletonList(FileSystems.matchNewResource(watchPath.toString(), true)));
  }

  private static TestPipelineOptions options;
  private static GcsPath watchPath;
}
