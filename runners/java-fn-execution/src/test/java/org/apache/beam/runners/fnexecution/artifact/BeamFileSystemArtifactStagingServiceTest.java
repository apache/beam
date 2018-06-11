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
package org.apache.beam.runners.fnexecution.artifact;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.JsonFormat;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.ArtifactChunk;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.ArtifactMetadata;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.CommitManifestRequest;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.CommitManifestResponse;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.Manifest;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.ProxyManifest;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.ProxyManifest.Builder;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.ProxyManifest.Location;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.PutArtifactMetadata;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.PutArtifactRequest;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.PutArtifactResponse;
import org.apache.beam.model.jobmanagement.v1.ArtifactStagingServiceGrpc;
import org.apache.beam.model.jobmanagement.v1.ArtifactStagingServiceGrpc.ArtifactStagingServiceStub;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.InProcessServerFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link BeamFileSystemArtifactStagingService}.
 */
@RunWith(JUnit4.class)
public class BeamFileSystemArtifactStagingServiceTest {

  private static final Joiner JOINER = Joiner.on("");
  private static final Charset CHARSET = StandardCharsets.UTF_8;
  private static final int DATA_1KB = 1 << 10;
  private GrpcFnServer<BeamFileSystemArtifactStagingService> server;
  private BeamFileSystemArtifactStagingService artifactStagingService;
  private ArtifactStagingServiceStub stub;
  private Path srcDir;
  private Path destDir;

  @Before
  public void setUp() throws Exception {
    artifactStagingService = new BeamFileSystemArtifactStagingService();
    server = GrpcFnServer
        .allocatePortAndCreateFor(artifactStagingService, InProcessServerFactory.create());
    stub =
        ArtifactStagingServiceGrpc.newStub(
            InProcessChannelBuilder.forName(server.getApiServiceDescriptor().getUrl()).build());

    srcDir = Files.createTempDirectory("BFSTemp");
    destDir = Files.createTempDirectory("BFDTemp");

  }

  @After
  public void tearDown() throws Exception {
    if (server != null) {
      server.close();
    }
    if (artifactStagingService != null) {
      artifactStagingService.close();
    }
    deleteDir(srcDir, "BFSTemp");
    deleteDir(destDir, "BFDTemp");
    server = null;
    artifactStagingService = null;
    stub = null;
  }

  private void deleteDir(Path dir, String sanityString) throws IOException {
    checkArgument(dir != null && dir.toAbsolutePath().toString().contains(sanityString),
        "Invalid directory.");
    Files.walkFileTree(dir, new SimpleFileVisitor<Path>() {
      @Override
      public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
        Files.deleteIfExists(file);
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
        Files.deleteIfExists(dir);
        return FileVisitResult.CONTINUE;
      }
    });
  }

  private void putArtifact(String stagingSessionToken, final String filePath, final String fileName)
      throws Exception {
    StreamObserver<PutArtifactRequest> outputStreamObserver = stub
        .putArtifact(new StreamObserver<PutArtifactResponse>() {
          @Override
          public void onNext(PutArtifactResponse putArtifactResponse) {
            Assert.fail("OnNext should never be called.");
          }

          @Override
          public void onError(Throwable throwable) {
            throwable.printStackTrace();
            Assert.fail("OnError should never be called.");
          }

          @Override
          public void onCompleted() {
          }
        });
    outputStreamObserver.onNext(PutArtifactRequest.newBuilder().setMetadata(
        PutArtifactMetadata.newBuilder()
            .setMetadata(ArtifactMetadata.newBuilder().setName(fileName).build())
            .setStagingSessionToken(stagingSessionToken)).build());

    FileInputStream fileInputStream = new FileInputStream(filePath);
    byte[] buffer = new byte[DATA_1KB]; // 1kb chunk
    int read = fileInputStream.read(buffer);
    while (read != -1) {
      outputStreamObserver.onNext(PutArtifactRequest.newBuilder().setData(
          ArtifactChunk.newBuilder().setData(ByteString.copyFrom(buffer, 0, read)).build())
          .build());
      read = fileInputStream.read(buffer);
    }
    outputStreamObserver.onCompleted();
  }

  private String commitManifest(String stagingSessionToken, List<ArtifactMetadata> artifacts)
      throws Exception {
    CompletableFuture<String> stagingTokenFuture = new CompletableFuture<>();
    stub.commitManifest(
        CommitManifestRequest.newBuilder().setStagingSessionToken(stagingSessionToken)
            .setManifest(Manifest.newBuilder().addAllArtifact(artifacts).build()).build(),
        new StreamObserver<CommitManifestResponse>() {
          @Override
          public void onNext(CommitManifestResponse commitManifestResponse) {
            stagingTokenFuture.complete(commitManifestResponse.getStagingToken());
          }

          @Override
          public void onError(Throwable throwable) {
            Assert.fail("OnError should never be called.");
          }

          @Override
          public void onCompleted() {

          }
        });
    return stagingTokenFuture.get(1, TimeUnit.SECONDS);

  }

  @Test
  public void generateStagingSessionTokenTest() throws Exception {
    String basePath = destDir.toAbsolutePath().toString();
    String stagingToken = BeamFileSystemArtifactStagingService
        .generateStagingSessionToken("abc123", basePath);
    Assert.assertEquals(
        "{\"sessionId\":\"abc123\",\"basePath\":\"" + basePath + "\"}", stagingToken);
  }

  @Test
  public void putArtifactsSingleSmallFileTest() throws Exception {
    String fileName = "file1";
    String stagingSession = "123";
    String stagingSessionToken = BeamFileSystemArtifactStagingService
        .generateStagingSessionToken(stagingSession, destDir.toUri().getPath());
    Path srcFilePath = Paths.get(srcDir.toString(), fileName).toAbsolutePath();
    Files.write(srcFilePath, "some_test".getBytes(CHARSET));
    putArtifact(stagingSessionToken, srcFilePath.toString(), fileName);
    String stagingToken = commitManifest(stagingSessionToken,
        Collections.singletonList(ArtifactMetadata.newBuilder().setName(fileName).build()));
    Assert.assertEquals(
        Paths.get(destDir.toAbsolutePath().toString(), stagingSession,
            BeamFileSystemArtifactStagingService.MANIFEST),
        Paths.get(stagingToken));
    assertFiles(Collections.singleton(fileName), stagingToken);
  }

  @Test
  public void putArtifactsMultipleFilesTest() throws Exception {
    String stagingSession = "123";
    Map<String, Integer> files = ImmutableMap.<String, Integer>builder()
        .put("file5cb", (DATA_1KB / 2) /*500b*/)
        .put("file1kb", DATA_1KB /*1 kb*/)
        .put("file15cb", (DATA_1KB * 3) / 2  /*1.5 kb*/)
        .put("nested/file1kb", DATA_1KB /*1 kb*/)
        .put("file10kb", 10 * DATA_1KB /*10 kb*/)
        .put("file100kb", 100 * DATA_1KB /*100 kb*/)
        .build();

    final String text = "abcdefghinklmop\n";
    files.forEach((fileName, size) -> {
      Path filePath = Paths.get(srcDir.toString(), fileName).toAbsolutePath();
      try {
        Files.createDirectories(filePath.getParent());
        Files.write(filePath,
            Strings.repeat(text, Double.valueOf(Math.ceil(size * 1.0 / text.length())).intValue())
                .getBytes(CHARSET));
      } catch (IOException ignored) {
      }
    });
    String stagingSessionToken = BeamFileSystemArtifactStagingService
        .generateStagingSessionToken(stagingSession, destDir.toUri().getPath());

    List<ArtifactMetadata> metadata = new ArrayList<>();
    for (String fileName : files.keySet()) {
      putArtifact(stagingSessionToken,
          Paths.get(srcDir.toString(), fileName).toAbsolutePath().toString(), fileName);
      metadata.add(ArtifactMetadata.newBuilder().setName(fileName).build());
    }

    String stagingToken = commitManifest(stagingSessionToken, metadata);
    Assert.assertEquals(
        Paths.get(destDir.toAbsolutePath().toString(), stagingSession, "MANIFEST").toString(),
        stagingToken);
    assertFiles(files.keySet(), stagingToken);
  }

  @Test
  public void putArtifactsMultipleFilesConcurrentlyTest() throws Exception {
    String stagingSession = "123";
    Map<String, Integer> files = ImmutableMap.<String, Integer>builder()
        .put("file5cb", (DATA_1KB / 2) /*500b*/)
        .put("file1kb", DATA_1KB /*1 kb*/)
        .put("file15cb", (DATA_1KB * 3) / 2  /*1.5 kb*/)
        .put("nested/file1kb", DATA_1KB /*1 kb*/)
        .put("file10kb", 10 * DATA_1KB /*10 kb*/)
        .put("file100kb", 100 * DATA_1KB /*100 kb*/)
        .build();

    final String text = "abcdefghinklmop\n";
    files.forEach((fileName, size) -> {
      Path filePath = Paths.get(srcDir.toString(), fileName).toAbsolutePath();
      try {
        Files.createDirectories(filePath.getParent());
        Files.write(filePath,
            Strings.repeat(text, Double.valueOf(Math.ceil(size * 1.0 / text.length())).intValue())
                .getBytes(CHARSET));
      } catch (IOException ignored) {
      }
    });
    String stagingSessionToken = BeamFileSystemArtifactStagingService
        .generateStagingSessionToken(stagingSession, destDir.toUri().getPath());

    List<ArtifactMetadata> metadata = new ArrayList<>();
    ExecutorService executorService = Executors.newFixedThreadPool(8);
    try {
      for (String fileName : files.keySet()) {
        executorService.execute(() -> {
          try {
            putArtifact(stagingSessionToken,
                Paths.get(srcDir.toString(), fileName).toAbsolutePath().toString(), fileName);
          } catch (Exception e) {
            Assert.fail(e.getMessage());
          }
          metadata.add(ArtifactMetadata.newBuilder().setName(fileName).build());
        });
      }
    } finally {
      executorService.shutdown();
      executorService.awaitTermination(2, TimeUnit.SECONDS);
    }

    String stagingToken = commitManifest(stagingSessionToken, metadata);
    Assert.assertEquals(
        Paths.get(destDir.toAbsolutePath().toString(), stagingSession, "MANIFEST").toString(),
        stagingToken);
    assertFiles(files.keySet(), stagingToken);
  }

  @Test
  public void putArtifactsMultipleFilesConcurrentSessionsTest() throws Exception {
    String stagingSession1 = "123";
    String stagingSession2 = "abc";
    Map<String, Integer> files1 = ImmutableMap.<String, Integer>builder()
        .put("file5cb", (DATA_1KB / 2) /*500b*/)
        .put("file1kb", DATA_1KB /*1 kb*/)
        .put("file15cb", (DATA_1KB * 3) / 2  /*1.5 kb*/)
        .build();
    Map<String, Integer> files2 = ImmutableMap.<String, Integer>builder()
        .put("nested/file1kb", DATA_1KB /*1 kb*/)
        .put("file10kb", 10 * DATA_1KB /*10 kb*/)
        .put("file100kb", 100 * DATA_1KB /*100 kb*/)
        .build();

    final String text = "abcdefghinklmop\n";
    ImmutableMap.<String, Integer>builder().putAll(files1).putAll(files2).build()
        .forEach((fileName, size) -> {
          Path filePath = Paths.get(srcDir.toString(), fileName).toAbsolutePath();
          try {
            Files.createDirectories(filePath.getParent());
            Files.write(filePath,
                Strings
                    .repeat(text, Double.valueOf(Math.ceil(size * 1.0 / text.length())).intValue())
                    .getBytes(CHARSET));
          } catch (IOException ignored) {
          }
        });
    String stagingSessionToken1 = BeamFileSystemArtifactStagingService
        .generateStagingSessionToken(stagingSession1, destDir.toUri().getPath());
    String stagingSessionToken2 = BeamFileSystemArtifactStagingService
        .generateStagingSessionToken(stagingSession2, destDir.toUri().getPath());

    List<ArtifactMetadata> metadata1 = new ArrayList<>();
    List<ArtifactMetadata> metadata2 = new ArrayList<>();
    ExecutorService executorService = Executors.newFixedThreadPool(8);
    try {
      Iterator<String> iterator1 = files1.keySet().iterator();
      Iterator<String> iterator2 = files2.keySet().iterator();
      while (iterator1.hasNext() && iterator2.hasNext()) {
        String fileName1 = iterator1.next();
        String fileName2 = iterator2.next();
        executorService.execute(() -> {
          try {
            putArtifact(stagingSessionToken1,
                Paths.get(srcDir.toString(), fileName1).toAbsolutePath().toString(), fileName1);
            putArtifact(stagingSessionToken2,
                Paths.get(srcDir.toString(), fileName2).toAbsolutePath().toString(), fileName2);
          } catch (Exception e) {
            Assert.fail(e.getMessage());
          }
          metadata1.add(ArtifactMetadata.newBuilder().setName(fileName1).build());
          metadata2.add(ArtifactMetadata.newBuilder().setName(fileName2).build());
        });
      }
    } finally {
      executorService.shutdown();
      executorService.awaitTermination(2, TimeUnit.SECONDS);
    }

    String stagingToken1 = commitManifest(stagingSessionToken1, metadata1);
    String stagingToken2 = commitManifest(stagingSessionToken2, metadata2);
    Assert.assertEquals(
        Paths.get(destDir.toAbsolutePath().toString(), stagingSession1, "MANIFEST").toString(),
        stagingToken1);
    Assert.assertEquals(
        Paths.get(destDir.toAbsolutePath().toString(), stagingSession2, "MANIFEST").toString(),
        stagingToken2);
    assertFiles(files1.keySet(), stagingToken1);
    assertFiles(files2.keySet(), stagingToken2);
  }

  private void assertFiles(Set<String> files, String stagingToken) throws IOException {
    Builder proxyManifestBuilder = ProxyManifest.newBuilder();
    JsonFormat.parser().merge(
        JOINER.join(Files.readAllLines(Paths.get(stagingToken), CHARSET)),
        proxyManifestBuilder);
    ProxyManifest proxyManifest = proxyManifestBuilder.build();
    Assert.assertEquals("Files in locations does not match actual file list.", files,
        proxyManifest.getLocationList().stream().map(Location::getName)
            .collect(Collectors.toSet()));
    Assert.assertEquals("Duplicate file entries in locations.", files.size(),
        proxyManifest.getLocationCount());
    for (Location location : proxyManifest.getLocationList()) {
      String expectedContent = JOINER.join(Files
          .readAllLines(Paths.get(srcDir.toString(), location.getName()), CHARSET));
      String actualContent = JOINER
          .join(Files.readAllLines(Paths.get(location.getUri()), CHARSET));
      Assert.assertEquals(expectedContent, actualContent);
    }
  }
}
