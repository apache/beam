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

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.Charset;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi;
import org.apache.beam.model.jobmanagement.v1.ArtifactRetrievalServiceGrpc;
import org.apache.beam.model.jobmanagement.v1.ArtifactStagingServiceGrpc;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.InProcessServerFactory;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.ManagedChannel;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.inprocess.InProcessChannelBuilder;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Charsets;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link ClassLoaderArtifactRetrievalService} and {@link
 * JavaFilesystemArtifactStagingService}.
 */
@RunWith(JUnit4.class)
public class ClassLoaderArtifactServiceTest {

  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  private static final int ARTIFACT_CHUNK_SIZE = 100;

  private static final Charset BIJECTIVE_CHARSET = Charsets.ISO_8859_1;

  public interface ArtifactServicePair extends AutoCloseable {

    String getStagingToken(String nonce);

    ArtifactStagingServiceGrpc.ArtifactStagingServiceStub createStagingStub() throws Exception;

    ArtifactStagingServiceGrpc.ArtifactStagingServiceBlockingStub createStagingBlockingStub()
        throws Exception;

    ArtifactRetrievalServiceGrpc.ArtifactRetrievalServiceStub createRetrievalStub()
        throws Exception;

    ArtifactRetrievalServiceGrpc.ArtifactRetrievalServiceBlockingStub createRetrievalBlockingStub()
        throws Exception;
  }

  /**
   * An ArtifactServicePair that loads artifacts into a jar file and then serves them up via a
   * ClassLoader using out of that jar.
   */
  private ArtifactServicePair classLoaderService() throws IOException {
    return new ArtifactServicePair() {

      Path jarPath = Paths.get(tempFolder.newFile("jar.jar").getPath());

      // These are initialized when the staging service is requested.
      FileSystem jarFilesystem;
      JavaFilesystemArtifactStagingService stagingService;
      GrpcFnServer<JavaFilesystemArtifactStagingService> stagingServer;
      ClassLoaderArtifactRetrievalService retrievalService;
      GrpcFnServer<ClassLoaderArtifactRetrievalService> retrievalServer;

      // These are initialized when the retrieval service is requested, closing the jar file
      // created above.
      ArtifactStagingServiceGrpc.ArtifactStagingServiceStub stagingStub;
      ArtifactStagingServiceGrpc.ArtifactStagingServiceBlockingStub stagingBlockingStub;
      ArtifactRetrievalServiceGrpc.ArtifactRetrievalServiceStub retrievalStub;
      ArtifactRetrievalServiceGrpc.ArtifactRetrievalServiceBlockingStub retrievalBlockingStub;

      @Override
      public void close() throws Exception {
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

      @Override
      public String getStagingToken(String nonce) {
        return "/path/to/subdir" + nonce.hashCode();
      }

      private void startStagingService() throws Exception {
        try (FileOutputStream fileOut = new FileOutputStream(jarPath.toString())) {
          try (ZipOutputStream zipOut = new ZipOutputStream(fileOut)) {
            ZipEntry zipEntry = new ZipEntry("someFile");
            zipOut.putNextEntry(zipEntry);
            zipOut.write(new byte[] {'s', 't', 'u', 'f', 'f'});
            zipOut.closeEntry();
          }
        }
        jarFilesystem =
            FileSystems.newFileSystem(
                URI.create("jar:file:" + jarPath.toString()), ImmutableMap.of());
        JavaFilesystemArtifactStagingService stagingService =
            new JavaFilesystemArtifactStagingService(jarFilesystem, "/path/to/root");
        GrpcFnServer<JavaFilesystemArtifactStagingService> stagingServer =
            GrpcFnServer.allocatePortAndCreateFor(stagingService, InProcessServerFactory.create());
        ManagedChannel stagingChannel =
            InProcessChannelBuilder.forName(stagingServer.getApiServiceDescriptor().getUrl())
                .build();
        stagingStub = ArtifactStagingServiceGrpc.newStub(stagingChannel);
        stagingBlockingStub = ArtifactStagingServiceGrpc.newBlockingStub(stagingChannel);
      }

      @Override
      public ArtifactStagingServiceGrpc.ArtifactStagingServiceStub createStagingStub()
          throws Exception {
        if (stagingStub == null) {
          startStagingService();
        }
        return stagingStub;
      }

      @Override
      public ArtifactStagingServiceGrpc.ArtifactStagingServiceBlockingStub
          createStagingBlockingStub() throws Exception {
        if (stagingBlockingStub == null) {
          startStagingService();
        }
        return stagingBlockingStub;
      }

      public void startupRetrievalService() throws Exception {
        jarFilesystem.close();
        retrievalService =
            new ClassLoaderArtifactRetrievalService(
                new URLClassLoader(new URL[] {jarPath.toUri().toURL()}));
        retrievalServer =
            GrpcFnServer.allocatePortAndCreateFor(
                retrievalService, InProcessServerFactory.create());
        ManagedChannel retrievalChannel =
            InProcessChannelBuilder.forName(retrievalServer.getApiServiceDescriptor().getUrl())
                .build();
        retrievalStub = ArtifactRetrievalServiceGrpc.newStub(retrievalChannel);
        retrievalBlockingStub = ArtifactRetrievalServiceGrpc.newBlockingStub(retrievalChannel);
      }

      @Override
      public ArtifactRetrievalServiceGrpc.ArtifactRetrievalServiceStub createRetrievalStub()
          throws Exception {
        if (retrievalStub == null) {
          startupRetrievalService();
        }
        return retrievalStub;
      }

      @Override
      public ArtifactRetrievalServiceGrpc.ArtifactRetrievalServiceBlockingStub
          createRetrievalBlockingStub() throws Exception {
        if (retrievalBlockingStub == null) {
          startupRetrievalService();
        }
        return retrievalBlockingStub;
      }
    };
  }

  private ArtifactApi.ArtifactMetadata putArtifact(
      ArtifactStagingServiceGrpc.ArtifactStagingServiceStub stagingStub,
      String stagingSessionToken,
      String name,
      String contents)
      throws InterruptedException, ExecutionException, TimeoutException {
    ArtifactApi.ArtifactMetadata metadata =
        ArtifactApi.ArtifactMetadata.newBuilder().setName(name).build();
    CompletableFuture<Void> complete = new CompletableFuture<>();
    StreamObserver<ArtifactApi.PutArtifactRequest> outputStreamObserver =
        stagingStub.putArtifact(
            new StreamObserver<ArtifactApi.PutArtifactResponse>() {

              @Override
              public void onNext(ArtifactApi.PutArtifactResponse putArtifactResponse) {
                // Do nothing.
              }

              @Override
              public void onError(Throwable th) {
                complete.completeExceptionally(th);
              }

              @Override
              public void onCompleted() {
                complete.complete(null);
              }
            });
    outputStreamObserver.onNext(
        ArtifactApi.PutArtifactRequest.newBuilder()
            .setMetadata(
                ArtifactApi.PutArtifactMetadata.newBuilder()
                    .setMetadata(metadata)
                    .setStagingSessionToken(stagingSessionToken))
            .build());

    byte[] byteContents = contents.getBytes(BIJECTIVE_CHARSET);
    for (int start = 0; start < byteContents.length; start += ARTIFACT_CHUNK_SIZE) {
      outputStreamObserver.onNext(
          ArtifactApi.PutArtifactRequest.newBuilder()
              .setData(
                  ArtifactApi.ArtifactChunk.newBuilder()
                      .setData(
                          ByteString.copyFrom(
                              byteContents,
                              start,
                              Math.min(byteContents.length - start, ARTIFACT_CHUNK_SIZE)))
                      .build())
              .build());
    }
    outputStreamObserver.onCompleted();
    complete.get(10, TimeUnit.SECONDS);
    return metadata;
  }

  private String commitManifest(
      ArtifactStagingServiceGrpc.ArtifactStagingServiceBlockingStub stagingStub,
      String stagingToken,
      List<ArtifactApi.ArtifactMetadata> artifacts) {
    return stagingStub
        .commitManifest(
            ArtifactApi.CommitManifestRequest.newBuilder()
                .setStagingSessionToken(stagingToken)
                .setManifest(ArtifactApi.Manifest.newBuilder().addAllArtifact(artifacts))
                .build())
        .getRetrievalToken();
  }

  private String getArtifact(
      ArtifactRetrievalServiceGrpc.ArtifactRetrievalServiceStub retrievalStub,
      String retrievalToken,
      String name)
      throws ExecutionException, InterruptedException {
    CompletableFuture<String> result = new CompletableFuture<>();
    retrievalStub.getArtifact(
        ArtifactApi.GetArtifactRequest.newBuilder()
            .setRetrievalToken(retrievalToken)
            .setName(name)
            .build(),
        new StreamObserver<ArtifactApi.ArtifactChunk>() {

          private ByteArrayOutputStream all = new ByteArrayOutputStream();

          @Override
          public void onNext(ArtifactApi.ArtifactChunk artifactChunk) {
            try {
              all.write(artifactChunk.getData().toByteArray());
            } catch (IOException exn) {
              Assert.fail("ByteArrayOutputStream threw exception: " + exn);
            }
          }

          @Override
          public void onError(Throwable th) {
            result.completeExceptionally(th);
          }

          @Override
          public void onCompleted() {
            result.complete(new String(all.toByteArray(), BIJECTIVE_CHARSET));
          }
        });
    return result.get();
  }

  private String stageArtifacts(
      ArtifactServicePair service, String stagingToken, Map<String, String> artifacts)
      throws Exception {
    ArtifactStagingServiceGrpc.ArtifactStagingServiceStub stagingStub = service.createStagingStub();
    ArtifactStagingServiceGrpc.ArtifactStagingServiceBlockingStub stagingBlockingStub =
        service.createStagingBlockingStub();
    List<ArtifactApi.ArtifactMetadata> artifactMetadatas = new ArrayList<>();
    for (Map.Entry<String, String> entry : artifacts.entrySet()) {
      artifactMetadatas.add(
          putArtifact(stagingStub, stagingToken, entry.getKey(), entry.getValue()));
    }
    return commitManifest(stagingBlockingStub, stagingToken, artifactMetadatas);
  }

  private void checkArtifacts(
      ArtifactServicePair service, String retrievalToken, Map<String, String> artifacts)
      throws Exception {
    ArtifactRetrievalServiceGrpc.ArtifactRetrievalServiceStub retrievalStub =
        service.createRetrievalStub();
    ArtifactRetrievalServiceGrpc.ArtifactRetrievalServiceBlockingStub retrievalBlockingStub =
        service.createRetrievalBlockingStub();
    ArtifactApi.Manifest manifest =
        retrievalBlockingStub
            .getManifest(
                ArtifactApi.GetManifestRequest.newBuilder()
                    .setRetrievalToken(retrievalToken)
                    .build())
            .getManifest();
    Assert.assertEquals(manifest.getArtifactCount(), artifacts.size());
    for (ArtifactApi.ArtifactMetadata artifact : manifest.getArtifactList()) {
      String contents = getArtifact(retrievalStub, retrievalToken, artifact.getName());
      Assert.assertEquals(artifacts.get(artifact.getName()), contents);
    }
  }

  private void runTest(ArtifactServicePair service, Map<String, String> artifacts)
      throws Exception {
    checkArtifacts(
        service, stageArtifacts(service, service.getStagingToken("nonce"), artifacts), artifacts);
  }

  private Map<String, String> identityMap(String... keys) {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    for (String key : keys) {
      builder.put(key, key);
    }
    return builder.build();
  }

  @Test
  public void testBasic() throws Exception {
    try (ArtifactServicePair service = classLoaderService()) {
      runTest(service, ImmutableMap.of("a", "Aa", "b", "Bbb", "c", "C"));
    }
  }

  @Test
  public void testOddFilenames() throws Exception {
    try (ArtifactServicePair service = classLoaderService()) {
      runTest(
          service,
          identityMap(
              "some whitespace\n\t",
              "some whitespace\n",
              "nullTerminated\0",
              "nullTerminated\0\0",
              "../../../../../../../slashes",
              "..\\..\\..\\..\\..\\..\\..\\backslashes",
              "/private"));
    }
  }

  @Test
  public void testMultipleChunks() throws Exception {
    try (ArtifactServicePair service = classLoaderService()) {
      byte[] contents = new byte[ARTIFACT_CHUNK_SIZE * 9 / 2];
      for (int i = 0; i < contents.length; i++) {
        contents[i] = (byte) (i * i + Integer.MAX_VALUE / (i + 1));
      }
      runTest(service, ImmutableMap.of("filename", new String(contents, BIJECTIVE_CHARSET)));
    }
  }

  @Test
  public void testMultipleTokens() throws Exception {
    try (ArtifactServicePair service = classLoaderService()) {
      Map<String, String> artifacts1 = ImmutableMap.of("a", "a1", "b", "b");
      Map<String, String> artifacts2 = ImmutableMap.of("a", "a2", "c", "c");
      String token1 = stageArtifacts(service, service.getStagingToken("1"), artifacts1);
      String token2 = stageArtifacts(service, service.getStagingToken("2"), artifacts2);
      checkArtifacts(service, token1, artifacts1);
      checkArtifacts(service, token2, artifacts2);
    }
  }
}
