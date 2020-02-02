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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.ArtifactChunk;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.ArtifactMetadata;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.CommitManifestRequest;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.GetArtifactRequest;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.GetManifestRequest;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.GetManifestResponse;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.Manifest;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.ProxyManifest;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.ProxyManifest.Location;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.PutArtifactMetadata;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.PutArtifactRequest;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.PutArtifactResponse;
import org.apache.beam.model.jobmanagement.v1.ArtifactRetrievalServiceGrpc;
import org.apache.beam.model.jobmanagement.v1.ArtifactRetrievalServiceGrpc.ArtifactRetrievalServiceBlockingStub;
import org.apache.beam.model.jobmanagement.v1.ArtifactRetrievalServiceGrpc.ArtifactRetrievalServiceStub;
import org.apache.beam.model.jobmanagement.v1.ArtifactStagingServiceGrpc;
import org.apache.beam.model.jobmanagement.v1.ArtifactStagingServiceGrpc.ArtifactStagingServiceBlockingStub;
import org.apache.beam.model.jobmanagement.v1.ArtifactStagingServiceGrpc.ArtifactStagingServiceStub;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.InProcessServerFactory;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.ManagedChannel;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.inprocess.InProcessChannelBuilder;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.hash.Hashing;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.ByteStreams;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link BeamFileSystemArtifactStagingService} and {@link
 * BeamFileSystemArtifactRetrievalService}.
 */
@RunWith(JUnit4.class)
public class BeamFileSystemArtifactServicesTest {
  private static final int DATA_1KB = 1 << 10;
  private GrpcFnServer<BeamFileSystemArtifactStagingService> stagingServer;
  private BeamFileSystemArtifactStagingService stagingService;
  private GrpcFnServer<BeamFileSystemArtifactRetrievalService> retrievalServer;
  private BeamFileSystemArtifactRetrievalService retrievalService;
  private ArtifactStagingServiceStub stagingStub;
  private ArtifactStagingServiceBlockingStub stagingBlockingStub;
  private ArtifactRetrievalServiceStub retrievalStub;
  private ArtifactRetrievalServiceBlockingStub retrievalBlockingStub;
  private Path stagingDir;
  private Path originalDir;
  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  @Before
  public void setUp() throws Exception {
    stagingService = new BeamFileSystemArtifactStagingService();
    stagingServer =
        GrpcFnServer.allocatePortAndCreateFor(stagingService, InProcessServerFactory.create());
    ManagedChannel stagingChannel =
        InProcessChannelBuilder.forName(stagingServer.getApiServiceDescriptor().getUrl()).build();
    stagingStub = ArtifactStagingServiceGrpc.newStub(stagingChannel);
    stagingBlockingStub = ArtifactStagingServiceGrpc.newBlockingStub(stagingChannel);

    retrievalService = new BeamFileSystemArtifactRetrievalService();
    retrievalServer =
        GrpcFnServer.allocatePortAndCreateFor(retrievalService, InProcessServerFactory.create());
    ManagedChannel retrievalChannel =
        InProcessChannelBuilder.forName(retrievalServer.getApiServiceDescriptor().getUrl()).build();
    retrievalStub = ArtifactRetrievalServiceGrpc.newStub(retrievalChannel);
    retrievalBlockingStub = ArtifactRetrievalServiceGrpc.newBlockingStub(retrievalChannel);

    originalDir = tempFolder.newFolder("original").toPath();
    stagingDir = tempFolder.newFolder("staging").toPath();
  }

  @After
  public void tearDown() throws Exception {
    if (stagingServer != null) {
      stagingServer.close();
    }
    if (stagingService != null) {
      stagingService.close();
    }
    if (retrievalServer != null) {
      retrievalServer.close();
    }
    if (retrievalService != null) {
      retrievalService.close();
    }
  }

  private void putArtifact(String stagingSessionToken, final String filePath, final String fileName)
      throws Exception {
    CompletableFuture<Boolean> complete = new CompletableFuture<>();
    StreamObserver<PutArtifactRequest> outputStreamObserver =
        stagingStub.putArtifact(
            new StreamObserver<PutArtifactResponse>() {
              @Override
              public void onNext(PutArtifactResponse putArtifactResponse) {
                // Do nothing.
              }

              @Override
              public void onError(Throwable throwable) {
                throwable.printStackTrace();
                Assert.fail("OnError should never be called.");
              }

              @Override
              public void onCompleted() {
                complete.complete(Boolean.TRUE);
              }
            });
    outputStreamObserver.onNext(
        PutArtifactRequest.newBuilder()
            .setMetadata(
                PutArtifactMetadata.newBuilder()
                    .setMetadata(ArtifactMetadata.newBuilder().setName(fileName).build())
                    .setStagingSessionToken(stagingSessionToken))
            .build());

    try (FileInputStream fileInputStream = new FileInputStream(filePath)) {
      byte[] buffer = new byte[DATA_1KB];
      int len;
      while ((len = fileInputStream.read(buffer)) != -1) {
        outputStreamObserver.onNext(
            PutArtifactRequest.newBuilder()
                .setData(
                    ArtifactChunk.newBuilder().setData(ByteString.copyFrom(buffer, 0, len)).build())
                .build());
      }
      outputStreamObserver.onCompleted();
      complete.get(10, TimeUnit.SECONDS);
    }
  }

  private String commitManifest(String stagingSessionToken, List<ArtifactMetadata> artifacts) {
    return stagingBlockingStub
        .commitManifest(
            CommitManifestRequest.newBuilder()
                .setStagingSessionToken(stagingSessionToken)
                .setManifest(Manifest.newBuilder().addAllArtifact(artifacts).build())
                .build())
        .getRetrievalToken();
  }

  @Test
  public void generateStagingSessionTokenTest() throws Exception {
    String basePath = stagingDir.toAbsolutePath().toString();
    String stagingToken =
        BeamFileSystemArtifactStagingService.generateStagingSessionToken("abc123", basePath);
    Assert.assertEquals(
        "{\"sessionId\":\"abc123\",\"basePath\":\"" + basePath + "\"}", stagingToken);
  }

  void checkCleanup(String stagingSessionToken, String stagingSession) throws Exception {
    Assert.assertTrue(
        Files.exists(Paths.get(stagingDir.toAbsolutePath().toString(), stagingSession)));
    stagingService.removeArtifacts(stagingSessionToken);
    Assert.assertFalse(
        Files.exists(Paths.get(stagingDir.toAbsolutePath().toString(), stagingSession)));
  }

  @Test
  public void noArtifactsTest() throws Exception {
    String stagingSession = "123";
    String stagingSessionToken =
        BeamFileSystemArtifactStagingService.generateStagingSessionToken(
            stagingSession, stagingDir.toUri().getPath());
    String stagingToken = commitManifest(stagingSessionToken, Collections.emptyList());
    Assert.assertEquals(AbstractArtifactStagingService.NO_ARTIFACTS_STAGED_TOKEN, stagingToken);
    Assert.assertFalse(
        Files.exists(Paths.get(stagingDir.toAbsolutePath().toString(), stagingSession)));

    GetManifestResponse retrievedManifest =
        retrievalBlockingStub.getManifest(
            GetManifestRequest.newBuilder().setRetrievalToken(stagingToken).build());
    Assert.assertEquals(
        "Manifest with 0 artifacts", 0, retrievedManifest.getManifest().getArtifactCount());
  }

  @Test
  public void putArtifactsSingleSmallFileTest() throws Exception {
    String fileName = "file1";
    String stagingSession = "123";
    String stagingSessionToken =
        BeamFileSystemArtifactStagingService.generateStagingSessionToken(
            stagingSession, stagingDir.toUri().getPath());
    Path srcFilePath = Paths.get(originalDir.toString(), fileName).toAbsolutePath();
    Files.write(srcFilePath, "some_test".getBytes(StandardCharsets.UTF_8));
    putArtifact(stagingSessionToken, srcFilePath.toString(), fileName);
    String stagingToken =
        commitManifest(
            stagingSessionToken,
            Collections.singletonList(ArtifactMetadata.newBuilder().setName(fileName).build()));
    Assert.assertEquals(
        Paths.get(
            stagingDir.toAbsolutePath().toString(),
            stagingSession,
            BeamFileSystemArtifactStagingService.MANIFEST),
        Paths.get(stagingToken));
    assertFiles(Collections.singleton(fileName), stagingToken);
    checkCleanup(stagingSessionToken, stagingSession);
  }

  @Test
  public void putArtifactsMultipleFilesTest() throws Exception {
    String stagingSession = "123";
    Map<String, Integer> files =
        ImmutableMap.<String, Integer>builder()
            .put("file5cb", (DATA_1KB / 2) /*500b*/)
            .put("file1kb", DATA_1KB /*1 kb*/)
            .put("file15cb", (DATA_1KB * 3) / 2 /*1.5 kb*/)
            .put("nested/file1kb", DATA_1KB /*1 kb*/)
            .put("file10kb", 10 * DATA_1KB /*10 kb*/)
            .put("file100kb", 100 * DATA_1KB /*100 kb*/)
            .build();
    Map<String, String> hashes = Maps.newHashMap();

    final String text = "abcdefghinklmop\n";
    files.forEach(
        (fileName, size) -> {
          Path filePath = Paths.get(originalDir.toString(), fileName).toAbsolutePath();
          try {
            Files.createDirectories(filePath.getParent());
            byte[] contents =
                Strings.repeat(
                        text, Double.valueOf(Math.ceil(size * 1.0 / text.length())).intValue())
                    .getBytes(StandardCharsets.UTF_8);
            Files.write(filePath, contents);
            hashes.put(fileName, Hashing.sha256().hashBytes(contents).toString());
          } catch (IOException ignored) {
          }
        });
    String stagingSessionToken =
        BeamFileSystemArtifactStagingService.generateStagingSessionToken(
            stagingSession, stagingDir.toUri().getPath());

    List<ArtifactMetadata> metadata = new ArrayList<>();
    for (String fileName : files.keySet()) {
      putArtifact(
          stagingSessionToken,
          Paths.get(originalDir.toString(), fileName).toAbsolutePath().toString(),
          fileName);
      metadata.add(
          ArtifactMetadata.newBuilder().setName(fileName).setSha256(hashes.get(fileName)).build());
    }

    String retrievalToken = commitManifest(stagingSessionToken, metadata);
    Assert.assertEquals(
        Paths.get(stagingDir.toAbsolutePath().toString(), stagingSession, "MANIFEST").toString(),
        retrievalToken);
    assertFiles(files.keySet(), retrievalToken);

    checkCleanup(stagingSessionToken, stagingSession);
  }

  @Test
  public void putArtifactsMultipleFilesConcurrentlyTest() throws Exception {
    String stagingSession = "123";
    Map<String, Integer> files =
        ImmutableMap.<String, Integer>builder()
            .put("file5cb", (DATA_1KB / 2) /*500b*/)
            .put("file1kb", DATA_1KB /*1 kb*/)
            .put("file15cb", (DATA_1KB * 3) / 2 /*1.5 kb*/)
            .put("nested/file1kb", DATA_1KB /*1 kb*/)
            .put("file10kb", 10 * DATA_1KB /*10 kb*/)
            .put("file100kb", 100 * DATA_1KB /*100 kb*/)
            .build();

    final String text = "abcdefghinklmop\n";
    files.forEach(
        (fileName, size) -> {
          Path filePath = Paths.get(originalDir.toString(), fileName).toAbsolutePath();
          try {
            Files.createDirectories(filePath.getParent());
            Files.write(
                filePath,
                Strings.repeat(
                        text, Double.valueOf(Math.ceil(size * 1.0 / text.length())).intValue())
                    .getBytes(StandardCharsets.UTF_8));
          } catch (IOException ignored) {
          }
        });
    String stagingSessionToken =
        BeamFileSystemArtifactStagingService.generateStagingSessionToken(
            stagingSession, stagingDir.toUri().getPath());

    List<ArtifactMetadata> metadata = Collections.synchronizedList(new ArrayList<>());
    ExecutorService executorService = Executors.newFixedThreadPool(8);
    try {
      for (String fileName : files.keySet()) {
        executorService.execute(
            () -> {
              try {
                putArtifact(
                    stagingSessionToken,
                    Paths.get(originalDir.toString(), fileName).toAbsolutePath().toString(),
                    fileName);
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

    String retrievalToken = commitManifest(stagingSessionToken, metadata);
    Assert.assertEquals(
        Paths.get(stagingDir.toAbsolutePath().toString(), stagingSession, "MANIFEST").toString(),
        retrievalToken);
    assertFiles(files.keySet(), retrievalToken);

    checkCleanup(stagingSessionToken, stagingSession);
  }

  @Test
  public void putArtifactsMultipleFilesConcurrentSessionsTest() throws Exception {
    String stagingSession1 = "123";
    String stagingSession2 = "abc";
    Map<String, Integer> files1 =
        ImmutableMap.<String, Integer>builder()
            .put("file5cb", (DATA_1KB / 2) /*500b*/)
            .put("file1kb", DATA_1KB /*1 kb*/)
            .put("file15cb", (DATA_1KB * 3) / 2 /*1.5 kb*/)
            .build();
    Map<String, Integer> files2 =
        ImmutableMap.<String, Integer>builder()
            .put("nested/file1kb", DATA_1KB /*1 kb*/)
            .put("file10kb", 10 * DATA_1KB /*10 kb*/)
            .put("file100kb", 100 * DATA_1KB /*100 kb*/)
            .build();

    final String text = "abcdefghinklmop\n";
    ImmutableMap.<String, Integer>builder()
        .putAll(files1)
        .putAll(files2)
        .build()
        .forEach(
            (fileName, size) -> {
              Path filePath = Paths.get(originalDir.toString(), fileName).toAbsolutePath();
              try {
                Files.createDirectories(filePath.getParent());
                Files.write(
                    filePath,
                    Strings.repeat(
                            text, Double.valueOf(Math.ceil(size * 1.0 / text.length())).intValue())
                        .getBytes(StandardCharsets.UTF_8));
              } catch (IOException ignored) {
              }
            });
    String stagingSessionToken1 =
        BeamFileSystemArtifactStagingService.generateStagingSessionToken(
            stagingSession1, stagingDir.toUri().getPath());
    String stagingSessionToken2 =
        BeamFileSystemArtifactStagingService.generateStagingSessionToken(
            stagingSession2, stagingDir.toUri().getPath());

    List<ArtifactMetadata> metadata1 = Collections.synchronizedList(new ArrayList<>());
    List<ArtifactMetadata> metadata2 = Collections.synchronizedList(new ArrayList<>());
    ExecutorService executorService = Executors.newFixedThreadPool(8);
    try {
      Iterator<String> iterator1 = files1.keySet().iterator();
      Iterator<String> iterator2 = files2.keySet().iterator();
      while (iterator1.hasNext() && iterator2.hasNext()) {
        String fileName1 = iterator1.next();
        String fileName2 = iterator2.next();
        executorService.execute(
            () -> {
              try {
                putArtifact(
                    stagingSessionToken1,
                    Paths.get(originalDir.toString(), fileName1).toAbsolutePath().toString(),
                    fileName1);
                putArtifact(
                    stagingSessionToken2,
                    Paths.get(originalDir.toString(), fileName2).toAbsolutePath().toString(),
                    fileName2);
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

    String retrievalToken1 = commitManifest(stagingSessionToken1, metadata1);
    String retrievalToken2 = commitManifest(stagingSessionToken2, metadata2);
    Assert.assertEquals(
        Paths.get(stagingDir.toAbsolutePath().toString(), stagingSession1, "MANIFEST").toString(),
        retrievalToken1);
    Assert.assertEquals(
        Paths.get(stagingDir.toAbsolutePath().toString(), stagingSession2, "MANIFEST").toString(),
        retrievalToken2);
    assertFiles(files1.keySet(), retrievalToken1);
    assertFiles(files2.keySet(), retrievalToken2);

    checkCleanup(stagingSessionToken1, stagingSession1);
    checkCleanup(stagingSessionToken2, stagingSession2);
  }

  private void assertFiles(Set<String> files, String retrievalToken) throws Exception {
    ProxyManifest proxyManifest =
        BeamFileSystemArtifactRetrievalService.loadManifest(retrievalToken);
    GetManifestResponse retrievedManifest =
        retrievalBlockingStub.getManifest(
            GetManifestRequest.newBuilder().setRetrievalToken(retrievalToken).build());
    Assert.assertEquals(
        "Manifest in proxy manifest file doesn't match the retrieved manifest",
        proxyManifest.getManifest(),
        retrievedManifest.getManifest());
    Assert.assertEquals(
        "Files in locations does not match actual file list.",
        files,
        proxyManifest.getLocationList().stream()
            .map(Location::getName)
            .collect(Collectors.toSet()));
    Assert.assertEquals(
        "Duplicate file entries in locations.", files.size(), proxyManifest.getLocationCount());
    for (Location location : proxyManifest.getLocationList()) {
      String expectedContent =
          readFile(
              Paths.get(originalDir.toString(), location.getName()).toAbsolutePath().toString());
      String actualStagedContent = readFile(location.getUri());
      Assert.assertEquals(
          "Staged content doesn't match expected content for " + location.getName(),
          expectedContent,
          actualStagedContent);
      String retrievedContent = retrieveArtifact(location.getName(), retrievalToken);
      Assert.assertEquals(
          "Retrieved content doesn't match expected content for " + location.getName(),
          expectedContent,
          retrievedContent);
    }
  }

  private static String readFile(String path) throws IOException {
    try (InputStream stream =
        Channels.newInputStream(
            FileSystems.open(FileSystems.matchNewResource(path, false /* isDirectory */)))) {
      return new String(ByteStreams.toByteArray(stream), StandardCharsets.UTF_8);
    }
  }

  private String retrieveArtifact(String name, String retrievalToken)
      throws ExecutionException, InterruptedException {
    CompletableFuture<ByteString> result = new CompletableFuture<>();
    retrievalStub.getArtifact(
        GetArtifactRequest.newBuilder().setRetrievalToken(retrievalToken).setName(name).build(),
        new StreamObserver<ArtifactChunk>() {
          private ByteString data = ByteString.EMPTY;

          @Override
          public void onNext(ArtifactChunk artifactChunk) {
            data = data.concat(artifactChunk.getData());
          }

          @Override
          public void onError(Throwable throwable) {
            result.completeExceptionally(throwable);
          }

          @Override
          public void onCompleted() {
            result.complete(data);
          }
        });
    return result.get().toStringUtf8();
  }
}
