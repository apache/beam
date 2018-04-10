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
package org.apache.beam.sdk.io;

import static org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions.RESOLVE_FILE;
import static org.hamcrest.Matchers.isA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.zip.GZIPOutputStream;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.UsesSplittableParDo;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link FileIO}. */
@RunWith(JUnit4.class)
public class FileIOTest implements Serializable {
  @Rule public transient TestPipeline p = TestPipeline.create();

  @Rule public transient TemporaryFolder tmpFolder = new TemporaryFolder();

  @Rule public transient ExpectedException thrown = ExpectedException.none();

  @Test
  @Category(NeedsRunner.class)
  public void testMatchAndMatchAll() throws IOException {
    Path firstPath = tmpFolder.newFile("first").toPath();
    Path secondPath = tmpFolder.newFile("second").toPath();
    int firstSize = 37;
    int secondSize = 42;
    Files.write(firstPath, new byte[firstSize]);
    Files.write(secondPath, new byte[secondSize]);

    PAssert.that(
            p.apply(
                "Match existing",
                FileIO.match().filepattern(tmpFolder.getRoot().getAbsolutePath() + "/*")))
        .containsInAnyOrder(metadata(firstPath, firstSize), metadata(secondPath, secondSize));
    PAssert.that(
            p.apply(
                "Match existing with provider",
                FileIO.match()
                    .filepattern(p.newProvider(tmpFolder.getRoot().getAbsolutePath() + "/*"))))
        .containsInAnyOrder(metadata(firstPath, firstSize), metadata(secondPath, secondSize));
    PAssert.that(
            p.apply("Create existing", Create.of(tmpFolder.getRoot().getAbsolutePath() + "/*"))
                .apply("MatchAll existing", FileIO.matchAll()))
        .containsInAnyOrder(metadata(firstPath, firstSize), metadata(secondPath, secondSize));

    PAssert.that(
            p.apply(
                "Match non-existing ALLOW",
                FileIO.match()
                    .filepattern(tmpFolder.getRoot().getAbsolutePath() + "/blah")
                    .withEmptyMatchTreatment(EmptyMatchTreatment.ALLOW)))
        .containsInAnyOrder();
    PAssert.that(
            p.apply(
                    "Create non-existing",
                    Create.of(tmpFolder.getRoot().getAbsolutePath() + "/blah"))
                .apply(
                    "MatchAll non-existing ALLOW",
                    FileIO.matchAll().withEmptyMatchTreatment(EmptyMatchTreatment.ALLOW)))
        .containsInAnyOrder();

    PAssert.that(
            p.apply(
                "Match non-existing ALLOW_IF_WILDCARD",
                FileIO.match()
                    .filepattern(tmpFolder.getRoot().getAbsolutePath() + "/blah*")
                    .withEmptyMatchTreatment(EmptyMatchTreatment.ALLOW_IF_WILDCARD)))
        .containsInAnyOrder();
    PAssert.that(
            p.apply(
                    "Create non-existing wildcard + explicit",
                    Create.of(tmpFolder.getRoot().getAbsolutePath() + "/blah*"))
                .apply(
                    "MatchAll non-existing ALLOW_IF_WILDCARD",
                    FileIO.matchAll()
                        .withEmptyMatchTreatment(EmptyMatchTreatment.ALLOW_IF_WILDCARD)))
        .containsInAnyOrder();
    PAssert.that(
            p.apply(
                    "Create non-existing wildcard + default",
                    Create.of(tmpFolder.getRoot().getAbsolutePath() + "/blah*"))
                .apply("MatchAll non-existing default", FileIO.matchAll()))
        .containsInAnyOrder();

    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testMatchDisallowEmptyDefault() throws IOException {
    p.apply("Match", FileIO.match().filepattern(tmpFolder.getRoot().getAbsolutePath() + "/*"));

    thrown.expectCause(isA(FileNotFoundException.class));
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testMatchDisallowEmptyExplicit() throws IOException {
    p.apply(
        FileIO.match()
            .filepattern(tmpFolder.getRoot().getAbsolutePath() + "/*")
            .withEmptyMatchTreatment(EmptyMatchTreatment.DISALLOW));

    thrown.expectCause(isA(FileNotFoundException.class));
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testMatchDisallowEmptyNonWildcard() throws IOException {
    p.apply(
        FileIO.match()
            .filepattern(tmpFolder.getRoot().getAbsolutePath() + "/blah")
            .withEmptyMatchTreatment(EmptyMatchTreatment.ALLOW_IF_WILDCARD));

    thrown.expectCause(isA(FileNotFoundException.class));
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testMatchAllDisallowEmptyExplicit() throws IOException {
    p.apply(Create.of(tmpFolder.getRoot().getAbsolutePath() + "/*"))
        .apply(FileIO.matchAll().withEmptyMatchTreatment(EmptyMatchTreatment.DISALLOW));
    thrown.expectCause(isA(FileNotFoundException.class));
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testMatchAllDisallowEmptyNonWildcard() throws IOException {
    p.apply(Create.of(tmpFolder.getRoot().getAbsolutePath() + "/blah"))
        .apply(FileIO.matchAll().withEmptyMatchTreatment(EmptyMatchTreatment.ALLOW_IF_WILDCARD));
    thrown.expectCause(isA(FileNotFoundException.class));
    p.run();
  }

  @Test
  @Category({NeedsRunner.class, UsesSplittableParDo.class})
  public void testMatchWatchForNewFiles() throws IOException, InterruptedException {
    final Path basePath = tmpFolder.getRoot().toPath().resolve("watch");
    basePath.toFile().mkdir();
    PCollection<MatchResult.Metadata> matchMetadata =
        p.apply(
            FileIO.match()
                .filepattern(basePath.resolve("*").toString())
                .continuously(
                    Duration.millis(100),
                    Watch.Growth.afterTimeSinceNewOutput(Duration.standardSeconds(3))));
    PCollection<MatchResult.Metadata> matchAllMetadata =
        p.apply(Create.of(basePath.resolve("*").toString()))
            .apply(
                FileIO.matchAll()
                    .continuously(
                        Duration.millis(100),
                        Watch.Growth.afterTimeSinceNewOutput(Duration.standardSeconds(3))));

    Thread writer =
        new Thread(
            () -> {
              try {
                Thread.sleep(1000);
                Files.write(basePath.resolve("first"), new byte[42]);
                Thread.sleep(300);
                Files.write(basePath.resolve("second"), new byte[37]);
                Thread.sleep(300);
                Files.write(basePath.resolve("third"), new byte[99]);
              } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
              }
            });
    writer.start();

    List<MatchResult.Metadata> expected =
        Arrays.asList(
            metadata(basePath.resolve("first"), 42),
            metadata(basePath.resolve("second"), 37),
            metadata(basePath.resolve("third"), 99));
    PAssert.that(matchMetadata).containsInAnyOrder(expected);
    PAssert.that(matchAllMetadata).containsInAnyOrder(expected);
    p.run();

    writer.join();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testRead() throws IOException {
    final String path = tmpFolder.newFile("file").getAbsolutePath();
    final String pathGZ = tmpFolder.newFile("file.gz").getAbsolutePath();
    Files.write(new File(path).toPath(), "Hello world".getBytes());
    try (Writer writer =
        new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(pathGZ)))) {
      writer.write("Hello world");
    }

    PCollection<MatchResult.Metadata> matches = p.apply("Match", FileIO.match().filepattern(path));
    PCollection<FileIO.ReadableFile> decompressedAuto =
        matches.apply("Read AUTO", FileIO.readMatches().withCompression(Compression.AUTO));
    PCollection<FileIO.ReadableFile> decompressedDefault =
        matches.apply("Read default", FileIO.readMatches());
    PCollection<FileIO.ReadableFile> decompressedUncompressed =
        matches.apply(
            "Read UNCOMPRESSED", FileIO.readMatches().withCompression(Compression.UNCOMPRESSED));
    for (PCollection<FileIO.ReadableFile> c :
        Arrays.asList(decompressedAuto, decompressedDefault, decompressedUncompressed)) {
      PAssert.thatSingleton(c)
          .satisfies(
              input -> {
                assertEquals(path, input.getMetadata().resourceId().toString());
                assertEquals("Hello world".length(), input.getMetadata().sizeBytes());
                assertEquals(Compression.UNCOMPRESSED, input.getCompression());
                assertTrue(input.getMetadata().isReadSeekEfficient());
                try {
                  assertEquals("Hello world", input.readFullyAsUTF8String());
                } catch (IOException e) {
                  throw new RuntimeException(e);
                }
                return null;
              });
    }

    PCollection<MatchResult.Metadata> matchesGZ =
        p.apply("Match GZ", FileIO.match().filepattern(pathGZ));
    PCollection<FileIO.ReadableFile> compressionAuto =
        matchesGZ.apply("Read GZ AUTO", FileIO.readMatches().withCompression(Compression.AUTO));
    PCollection<FileIO.ReadableFile> compressionDefault =
        matchesGZ.apply("Read GZ default", FileIO.readMatches());
    PCollection<FileIO.ReadableFile> compressionGzip =
        matchesGZ.apply("Read GZ GZIP", FileIO.readMatches().withCompression(Compression.GZIP));
    for (PCollection<FileIO.ReadableFile> c :
        Arrays.asList(compressionAuto, compressionDefault, compressionGzip)) {
      PAssert.thatSingleton(c)
          .satisfies(
              input -> {
                assertEquals(pathGZ, input.getMetadata().resourceId().toString());
                assertFalse(input.getMetadata().sizeBytes() == "Hello world".length());
                assertEquals(Compression.GZIP, input.getCompression());
                assertFalse(input.getMetadata().isReadSeekEfficient());
                try {
                  assertEquals("Hello world", input.readFullyAsUTF8String());
                } catch (IOException e) {
                  throw new RuntimeException(e);
                }
                return null;
              });
    }

    p.run();
  }

  private static MatchResult.Metadata metadata(Path path, int size) {
    return MatchResult.Metadata.builder()
        .setResourceId(FileSystems.matchNewResource(path.toString(), false /* isDirectory */))
        .setIsReadSeekEfficient(true)
        .setSizeBytes(size)
        .build();
  }

  private static FileIO.Write.FileNaming resolveFileNaming(FileIO.Write<?, ?> write)
      throws Exception {
    return write.resolveFileNamingFn().getClosure().apply(null, null);
  }

  private static String getDefaultFileName(FileIO.Write<?, ?> write) throws Exception {
    return resolveFileNaming(write).getFilename(null, null, 0, 0, null);
  }

  @Test
  public void testFilenameFnResolution() throws Exception {
    FileIO.Write.FileNaming foo = (window, pane, numShards, shardIndex, compression) -> "foo";

    String expected =
        FileSystems.matchNewResource("test", true).resolve("foo", RESOLVE_FILE).toString();
    assertEquals(
        "Filenames should be resolved within a relative directory if '.to' is invoked",
        expected,
        getDefaultFileName(FileIO.writeDynamic().to("test").withNaming(o -> foo)));
    assertEquals(
        "Filenames should be resolved within a relative directory if '.to' is invoked",
        expected,
        getDefaultFileName(FileIO.write().to("test").withNaming(foo)));

    assertEquals(
        "Filenames should be resolved as the direct result of the filenaming function if '.to' "
            + "is not invoked",
        "foo",
        getDefaultFileName(FileIO.writeDynamic().withNaming(o -> foo)));
    assertEquals(
        "Filenames should be resolved as the direct result of the filenaming function if '.to' "
            + "is not invoked",
        "foo",
        getDefaultFileName(FileIO.write().withNaming(foo)));

    assertEquals(
        "Default to the defaultNaming if a filenaming isn't provided for a non-dynamic write",
        "output-00000-of-00000",
        resolveFileNaming(FileIO.write())
            .getFilename(
                GlobalWindow.INSTANCE,
                PaneInfo.ON_TIME_AND_ONLY_FIRING,
                0,
                0,
                Compression.UNCOMPRESSED));

    assertEquals(
        "Default Naming should take prefix and suffix into account if provided",
        "foo-00000-of-00000.bar",
        resolveFileNaming(FileIO.write().withPrefix("foo").withSuffix(".bar"))
            .getFilename(
                GlobalWindow.INSTANCE,
                PaneInfo.ON_TIME_AND_ONLY_FIRING,
                0,
                0,
                Compression.UNCOMPRESSED));

    assertEquals(
        "Filenames should be resolved within a relative directory if '.to' is invoked, "
            + "even with default naming",
        FileSystems.matchNewResource("test", true)
            .resolve("output-00000-of-00000", RESOLVE_FILE)
            .toString(),
        resolveFileNaming(FileIO.write().to("test"))
            .getFilename(
                GlobalWindow.INSTANCE,
                PaneInfo.ON_TIME_AND_ONLY_FIRING,
                0,
                0,
                Compression.UNCOMPRESSED));
  }
}
