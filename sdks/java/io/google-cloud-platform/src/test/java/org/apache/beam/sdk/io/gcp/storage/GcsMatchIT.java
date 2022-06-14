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
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.ByteStreams;
import org.joda.time.Duration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class GcsMatchIT {
  /** DoFn that writes test files to Gcs System when first time called. */
  private static class WriteToGcsFn extends DoFn<GcsPath, Void> {
    public WriteToGcsFn(long waitSec) {
      this.waitSec = waitSec;
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      GcsPath writePath = context.element();
      assert writePath != null;
      Thread writer =
          new Thread(
              () -> {
                try {
                  // Write test files to writePath
                  Thread.sleep(waitSec * 1000);
                  writeBytesToFile(writePath.resolve("first").toString(), 42);
                  Thread.sleep(1000);
                  writeBytesToFile(writePath.resolve("first").toString(), 99);
                  writeBytesToFile(writePath.resolve("second").toString(), 42);
                  Thread.sleep(1000);
                  writeBytesToFile(writePath.resolve("first").toString(), 37);
                  writeBytesToFile(writePath.resolve("second").toString(), 42);
                  writeBytesToFile(writePath.resolve("third").toString(), 99);
                } catch (InterruptedException e) {
                  throw new RuntimeException(e);
                }
              });
      writer.start();
    }

    private final long waitSec;
  }

  /** Integration test for TextIO.MatchAll watching for file updates in gcs filesystem. */
  @Test
  public void testGcsMatchContinuously() {
    TestPipelineOptions options =
        TestPipeline.testingPipelineOptions().as(TestPipelineOptions.class);
    assertNotNull(options.getTempRoot());
    options.setTempLocation(options.getTempRoot() + "/GcsMatchIT");
    GcsOptions gcsOptions = options.as(GcsOptions.class);
    String dstFolderName =
        gcsOptions.getGcpTempLocation()
            + String.format("/testGcsMatchContinuously.%tF-%<tH-%<tM-%<tS-%<tL/", new Date());
    final GcsPath watchPath = GcsPath.fromUri(dstFolderName);
    long waitSec = 7;
    if (options.getRunner() == DirectRunner.class) {
      // save some time testing on direct runner
      waitSec = 1;
    }

    Pipeline p = Pipeline.create(options);
    PCollection<GcsPath> path = p.apply(Create.of(Collections.singletonList(watchPath)));
    PCollection<String> filenames =
        path.apply(
                MapElements.into(TypeDescriptors.strings())
                    .via((gcsPath) -> gcsPath.resolve("*").toString()))
            .apply(
                "matchAll updated",
                FileIO.matchAll()
                    .continuously(
                        Duration.millis(250),
                        Watch.Growth.afterTimeSinceNewOutput(Duration.standardSeconds(waitSec + 2)),
                        true))
            .apply(
                "pick up file names",
                MapElements.into(TypeDescriptors.strings())
                    .via((metadata) -> metadata.resourceId().getFilename()));

    path.apply(ParDo.of(new WriteToGcsFn(waitSec)));

    List<String> expectedMatchAllUpdated =
        Arrays.asList("first", "first", "first", "second", "second", "third");
    PAssert.that(filenames).containsInAnyOrder(expectedMatchAllUpdated);

    PipelineResult result = p.run();

    State state = result.waitUntilFinish();
    assertEquals(State.DONE, state);
  }

  private static void writeBytesToFile(String gcsPath, int length) {
    ResourceId newFileResourceId = FileSystems.matchNewResource(gcsPath, false);
    try (ByteArrayInputStream in = new ByteArrayInputStream(new byte[length]);
        ReadableByteChannel readerChannel = Channels.newChannel(in);
        WritableByteChannel writerChannel = FileSystems.create(newFileResourceId, MimeTypes.TEXT)) {
      ByteStreams.copy(readerChannel, writerChannel);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
