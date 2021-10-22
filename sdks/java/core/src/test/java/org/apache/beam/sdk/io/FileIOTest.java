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
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.FileTime;
import java.util.Arrays;
import java.util.List;
import java.util.zip.GZIPOutputStream;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.UsesUnboundedSplittableParDo;
import org.apache.beam.sdk.transforms.Contextful;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Requirements;
import org.apache.beam.sdk.transforms.SerializableFunctions;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Charsets;
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
    long firstModified = 1541097000L;
    long secondModified = 1541098000L;
    Files.write(firstPath, new byte[firstSize]);
    Files.write(secondPath, new byte[secondSize]);
    Files.setLastModifiedTime(firstPath, FileTime.fromMillis(firstModified));
    Files.setLastModifiedTime(secondPath, FileTime.fromMillis(secondModified));
    MatchResult.Metadata firstMetadata = metadata(firstPath, firstSize, firstModified);
    MatchResult.Metadata secondMetadata = metadata(secondPath, secondSize, secondModified);

    PAssert.that(
            p.apply(
                "Match existing",
                FileIO.match().filepattern(tmpFolder.getRoot().getAbsolutePath() + "/*")))
        .containsInAnyOrder(firstMetadata, secondMetadata);
    PAssert.that(
            p.apply(
                "Match existing with provider",
                FileIO.match()
                    .filepattern(p.newProvider(tmpFolder.getRoot().getAbsolutePath() + "/*"))))
        .containsInAnyOrder(firstMetadata, secondMetadata);
    PAssert.that(
            p.apply("Create existing", Create.of(tmpFolder.getRoot().getAbsolutePath() + "/*"))
                .apply("MatchAll existing", FileIO.matchAll()))
        .containsInAnyOrder(firstMetadata, secondMetadata);

    PAssert.that(
            p.apply(
                "Match non-existing ALLOW",
                FileIO.match()
                    .filepattern(tmpFolder.getRoot().getAbsolutePath() + "/blah")
                    .withEmptyMatchTreatment(EmptyMatchTreatment.ALLOW)))
        .empty();
    PAssert.that(
            p.apply(
                    "Create non-existing",
                    Create.of(tmpFolder.getRoot().getAbsolutePath() + "/blah"))
                .apply(
                    "MatchAll non-existing ALLOW",
                    FileIO.matchAll().withEmptyMatchTreatment(EmptyMatchTreatment.ALLOW)))
        .empty();

    PAssert.that(
            p.apply(
                "Match non-existing ALLOW_IF_WILDCARD",
                FileIO.match()
                    .filepattern(tmpFolder.getRoot().getAbsolutePath() + "/blah*")
                    .withEmptyMatchTreatment(EmptyMatchTreatment.ALLOW_IF_WILDCARD)))
        .empty();
    PAssert.that(
            p.apply(
                    "Create non-existing wildcard + explicit",
                    Create.of(tmpFolder.getRoot().getAbsolutePath() + "/blah*"))
                .apply(
                    "MatchAll non-existing ALLOW_IF_WILDCARD",
                    FileIO.matchAll()
                        .withEmptyMatchTreatment(EmptyMatchTreatment.ALLOW_IF_WILDCARD)))
        .empty();
    PAssert.that(
            p.apply(
                    "Create non-existing wildcard + default",
                    Create.of(tmpFolder.getRoot().getAbsolutePath() + "/blah*"))
                .apply("MatchAll non-existing default", FileIO.matchAll()))
        .empty();

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
  @Category({NeedsRunner.class, UsesUnboundedSplittableParDo.class})
  public void testMatchWatchForNewFiles() throws IOException, InterruptedException {
    // Write some files to a "source" directory.
    final Path sourcePath = tmpFolder.getRoot().toPath().resolve("source");
    sourcePath.toFile().mkdir();
    Files.write(sourcePath.resolve("first"), new byte[42]);
    Files.write(sourcePath.resolve("second"), new byte[37]);
    Files.write(sourcePath.resolve("third"), new byte[99]);

    // Create a "watch" directory that the pipeline will copy files into.
    final Path watchPath = tmpFolder.getRoot().toPath().resolve("watch");
    watchPath.toFile().mkdir();
    PCollection<MatchResult.Metadata> matchMetadata =
        p.apply(
            FileIO.match()
                .filepattern(watchPath.resolve("*").toString())
                .continuously(
                    Duration.millis(100),
                    Watch.Growth.afterTimeSinceNewOutput(Duration.standardSeconds(3))));
    PCollection<MatchResult.Metadata> matchAllMetadata =
        p.apply(Create.of(watchPath.resolve("*").toString()))
            .apply(
                FileIO.matchAll()
                    .continuously(
                        Duration.millis(100),
                        Watch.Growth.afterTimeSinceNewOutput(Duration.standardSeconds(3))));
    assertEquals(PCollection.IsBounded.UNBOUNDED, matchMetadata.isBounded());
    assertEquals(PCollection.IsBounded.UNBOUNDED, matchAllMetadata.isBounded());

    // Copy the files to the "watch" directory, preserving the lastModifiedTime;
    // the COPY_ATTRIBUTES option ensures that we will at a minimum copy lastModifiedTime.
    CopyOption[] copyOptions = {StandardCopyOption.COPY_ATTRIBUTES};
    Thread writer =
        new Thread(
            () -> {
              try {
                Thread.sleep(1000);
                Files.copy(sourcePath.resolve("first"), watchPath.resolve("first"), copyOptions);
                Thread.sleep(300);
                Files.copy(sourcePath.resolve("second"), watchPath.resolve("second"), copyOptions);
                Thread.sleep(300);
                Files.copy(sourcePath.resolve("third"), watchPath.resolve("third"), copyOptions);
              } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
              }
            });
    writer.start();

    // We fetch lastModifiedTime from the files in the "source" directory to avoid a race condition
    // with the writer thread.
    List<MatchResult.Metadata> expected =
        Arrays.asList(
            metadata(
                watchPath.resolve("first"), 42, lastModifiedMillis(sourcePath.resolve("first"))),
            metadata(
                watchPath.resolve("second"), 37, lastModifiedMillis(sourcePath.resolve("second"))),
            metadata(
                watchPath.resolve("third"), 99, lastModifiedMillis(sourcePath.resolve("third"))));
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
    Files.write(new File(path).toPath(), "Hello world".getBytes(Charsets.UTF_8));
    try (Writer writer =
        new OutputStreamWriter(
            new GZIPOutputStream(new FileOutputStream(pathGZ)), Charsets.UTF_8)) {
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

  private static MatchResult.Metadata metadata(Path path, int size, long lastModifiedMillis) {
    return MatchResult.Metadata.builder()
        .setResourceId(FileSystems.matchNewResource(path.toString(), false /* isDirectory */))
        .setIsReadSeekEfficient(true)
        .setSizeBytes(size)
        .setLastModifiedMillis(lastModifiedMillis)
        .build();
  }

  private static long lastModifiedMillis(Path path) throws IOException {
    return Files.getLastModifiedTime(path).toMillis();
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

  @Test
  @Category(NeedsRunner.class)
  public void testFileIoDynamicNaming() throws IOException {
    // Test for BEAM-6407.

    String outputFileName = tmpFolder.newFile().getAbsolutePath();
    PCollectionView<String> outputFileNameView =
        p.apply("outputFileName", Create.of(outputFileName)).apply(View.asSingleton());

    Contextful.Fn<String, FileIO.Write.FileNaming> fileNaming =
        (element, c) ->
            (window, pane, numShards, shardIndex, compression) ->
                c.sideInput(outputFileNameView) + "-" + shardIndex;

    p.apply(Create.of(""))
        .apply(
            "WriteDynamicFilename",
            FileIO.<String, String>writeDynamic()
                .by(SerializableFunctions.constant(""))
                .withDestinationCoder(StringUtf8Coder.of())
                .via(TextIO.sink())
                .withTempDirectory(tmpFolder.newFolder().getAbsolutePath())
                .withNaming(
                    Contextful.of(
                        fileNaming, Requirements.requiresSideInputs(outputFileNameView))));

    // We need to run the TestPipeline with the default options.
    p.run(PipelineOptionsFactory.create()).waitUntilFinish();
    assertTrue(
        "Output file shard 0 exists after pipeline completes",
        new File(outputFileName + "-0").exists());
  }
}
