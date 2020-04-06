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

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi;
import org.apache.beam.model.jobmanagement.v1.ArtifactStagingServiceGrpc;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.fnexecution.FnService;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.Status;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.StatusException;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Splitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ArtifactStagingService
    extends ArtifactStagingServiceGrpc.ArtifactStagingServiceImplBase implements FnService {

  private static final Logger LOG = LoggerFactory.getLogger(ArtifactStagingService.class);

  private final ArtifactDestinationProvider destinationProvider;

  private final ConcurrentMap<String, Map<String, List<RunnerApi.ArtifactInformation>>> toStage =
      new ConcurrentHashMap<>();

  private final ConcurrentMap<String, Map<String, List<RunnerApi.ArtifactInformation>>> staged =
      new ConcurrentHashMap<>();

  public ArtifactStagingService(ArtifactDestinationProvider destinationProvider) {
    this.destinationProvider = destinationProvider;
  }

  /**
   * Registers a set of artifacts to be staged with this service.
   *
   * <p>A client (e.g. a Beam SDK) is expected to connect to this service with the given staging
   * token and offer resolution and retrieval of this set of artifacts.
   *
   * @param stagingToken a staging token for this job
   * @param artifacts all artifacts to stage, keyed by environment
   */
  public void registerJob(
      String stagingToken, Map<String, List<RunnerApi.ArtifactInformation>> artifacts) {
    assert !toStage.containsKey(stagingToken);
    toStage.put(stagingToken, artifacts);
  }

  /**
   * Returns the rewritten artifacts associated with this job, keyed by environment.
   *
   * <p>This should be called after the client has finished offering artifacts.
   *
   * @param stagingToken a staging token for this job
   */
  public Map<String, List<RunnerApi.ArtifactInformation>> getStagedArtifacts(String stagingToken) {
    toStage.remove(stagingToken);
    return staged.remove(stagingToken);
  }

  /** Provides a concrete location to which artifacts can be staged on retrieval. */
  public interface ArtifactDestinationProvider {
    ArtifactDestination getDestination(String stagingToken, String name) throws IOException;
  }

  /**
   * A pairing of a newly created artifact type and an ouptut stream that will be readable at that
   * type.
   */
  @AutoValue
  public abstract static class ArtifactDestination {
    public static ArtifactDestination create(
        String typeUrn, ByteString typePayload, OutputStream out) {
      return new AutoValue_ArtifactStagingService_ArtifactDestination(typeUrn, typePayload, out);
    }

    public static ArtifactDestination fromFile(String path) throws IOException {
      return fromFile(
          path,
          Channels.newOutputStream(
              FileSystems.create(
                  FileSystems.matchNewResource(path, false /* isDirectory */), MimeTypes.BINARY)));
    }

    public static ArtifactDestination fromFile(String path, OutputStream out) {
      return create(
          ArtifactRetrievalService.FILE_ARTIFACT_URN,
          RunnerApi.ArtifactFilePayload.newBuilder().setPath(path).build().toByteString(),
          out);
    }

    public abstract String getTypeUrn();

    public abstract ByteString getTypePayload();

    public abstract OutputStream getOutputStream();
  }

  /**
   * An ArtifactDestinationProvider that places new artifacts as files in a Beam filesystem.
   *
   * @param root the directory in which to place all artifacts
   */
  public static ArtifactDestinationProvider beamFilesystemArtifactDestinationProvider(String root) {
    return (statingToken, name) -> {
      ResourceId path =
          FileSystems.matchNewResource(root, true)
              .resolve(statingToken, ResolveOptions.StandardResolveOptions.RESOLVE_DIRECTORY)
              .resolve(name, ResolveOptions.StandardResolveOptions.RESOLVE_FILE);
      return ArtifactDestination.fromFile(path.toString());
    };
  }

  private static enum State {
    START,
    RESOLVE,
    GET,
    DONE,
    ERROR,
  }

  @Override
  public StreamObserver<ArtifactApi.ArtifactResponseWrapper> reverseArtifactRetrievalService(
      StreamObserver<ArtifactApi.ArtifactRequestWrapper> responseObserver) {

    return new StreamObserver<ArtifactApi.ArtifactResponseWrapper>() {

      State state = State.START;
      String stagingToken;
      Map<String, List<RunnerApi.ArtifactInformation>> toResolve;
      Map<String, List<RunnerApi.ArtifactInformation>> staged;
      Queue<String> pendingResolves;
      String currentEnvironment;
      int nameIndex;
      Queue<RunnerApi.ArtifactInformation> pendingGets;
      OutputStream currentOutputStream;

      @Override
      public synchronized void onNext(ArtifactApi.ArtifactResponseWrapper responseWrapper) {
        switch (state) {
          case START:
            stagingToken = responseWrapper.getStagingToken();
            LOG.info("Staging artifacts for {}.", stagingToken);
            toResolve = toStage.get(stagingToken);
            staged = new ConcurrentHashMap<>();
            pendingResolves = new ArrayDeque<>();
            pendingResolves.addAll(toResolve.keySet());
            resolveNextEnvironment(responseObserver);
            break;

          case RESOLVE:
            {
              currentEnvironment = pendingResolves.remove();
              staged.put(currentEnvironment, new ArrayList<>());
              pendingGets = new ArrayDeque<>();
              for (RunnerApi.ArtifactInformation artifact :
                  responseWrapper.getResolveArtifactResponse().getReplacementsList()) {
                Optional<RunnerApi.ArtifactInformation> fetched = getLocal(artifact);
                if (fetched.isPresent()) {
                  staged.get(currentEnvironment).add(fetched.get());
                } else {
                  pendingGets.add(artifact);
                  responseObserver.onNext(
                      ArtifactApi.ArtifactRequestWrapper.newBuilder()
                          .setGetArtifact(
                              ArtifactApi.GetArtifactRequest.newBuilder().setArtifact(artifact))
                          .build());
                }
              }
              LOG.info(
                  "Getting {} artifacts for {}.{}.",
                  pendingGets.size(),
                  stagingToken,
                  pendingResolves.peek());
              if (pendingGets.isEmpty()) {
                resolveNextEnvironment(responseObserver);
              } else {
                state = State.GET;
              }
              break;
            }

          case GET:
            if (currentOutputStream == null) {
              RunnerApi.ArtifactInformation currentArtifact = pendingGets.remove();
              String name = createFilename(nameIndex++, currentEnvironment, currentArtifact);
              try {
                ArtifactDestination dest = destinationProvider.getDestination(stagingToken, name);
                LOG.debug(
                    "Storing artifacts for {}.{} at {}",
                    stagingToken,
                    pendingResolves.peek(),
                    name);
                staged
                    .get(currentEnvironment)
                    .add(
                        RunnerApi.ArtifactInformation.newBuilder()
                            .setTypeUrn(dest.getTypeUrn())
                            .setTypePayload(dest.getTypePayload())
                            .setRoleUrn(currentArtifact.getRoleUrn())
                            .setRolePayload(currentArtifact.getRolePayload())
                            .build());
                currentOutputStream = dest.getOutputStream();
              } catch (Exception exn) {
                responseObserver.onError(exn);
              }
            }
            try {
              currentOutputStream.write(
                  responseWrapper.getGetArtifactResponse().getData().toByteArray());
            } catch (IOException exn) {
              responseObserver.onError(exn);
            }
            if (responseWrapper.getIsLast()) {
              try {
                currentOutputStream.close();
              } catch (IOException exn) {
                onError(exn);
              }
              currentOutputStream = null;
              if (pendingGets.isEmpty()) {
                resolveNextEnvironment(responseObserver);
              } else {
                LOG.debug("Waiting for {}", pendingGets.peek());
              }
            }
            break;

          default:
            responseObserver.onError(
                new StatusException(
                    Status.INVALID_ARGUMENT.withDescription("Illegal state " + state)));
        }
      }

      private void resolveNextEnvironment(
          StreamObserver<ArtifactApi.ArtifactRequestWrapper> responseObserver) {
        if (pendingResolves.isEmpty()) {
          ArtifactStagingService.this.staged.put(stagingToken, staged);
          state = State.DONE;
          LOG.info("Artifacts fully staged for {}.", stagingToken);
          responseObserver.onCompleted();
        } else {
          state = State.RESOLVE;
          LOG.info("Resolving artifacts for {}.{}.", stagingToken, pendingResolves.peek());
          responseObserver.onNext(
              ArtifactApi.ArtifactRequestWrapper.newBuilder()
                  .setResolveArtifact(
                      ArtifactApi.ResolveArtifactsRequest.newBuilder()
                          .addAllArtifacts(toResolve.get(pendingResolves.peek())))
                  .build());
        }
      }

      /**
       * Return an alternative artifact if we do not need to get this over the artifact API, or
       * possibly at all.
       */
      private Optional<RunnerApi.ArtifactInformation> getLocal(
          RunnerApi.ArtifactInformation artifact) {
        return Optional.empty();
      }

      /**
       * Attempts to provide a reasonable filename for the artifact.
       *
       * @param index a monotonically increasing index, which provides uniqueness
       * @param environment the environment id
       * @param artifact the artifact itself
       */
      private String createFilename(
          int index, String environment, RunnerApi.ArtifactInformation artifact) {
        String path;
        try {
          if (artifact.getRoleUrn().equals(ArtifactRetrievalService.STAGING_TO_ARTIFACT_URN)) {
            path =
                RunnerApi.ArtifactStagingToRolePayload.parseFrom(artifact.getRolePayload())
                    .getStagedName();
          } else if (artifact.getTypeUrn().equals(ArtifactRetrievalService.FILE_ARTIFACT_URN)) {
            path = RunnerApi.ArtifactFilePayload.parseFrom(artifact.getTypePayload()).getPath();
          } else if (artifact.getTypeUrn().equals(ArtifactRetrievalService.URL_ARTIFACT_URN)) {
            path = RunnerApi.ArtifactUrlPayload.parseFrom(artifact.getTypePayload()).getUrl();
          } else {
            path = "artifact";
          }
        } catch (InvalidProtocolBufferException exn) {
          throw new RuntimeException(exn);
        }
        // Limit to the last contiguous alpha-numeric sequence. In particular, this will exclude
        // all path separators.
        List<String> components = Splitter.onPattern("[^A-Za-z-_.]]").splitToList(path);
        String base = components.get(components.size() - 1);
        return clip(String.format("%d-%s-%s", index, clip(environment, 25), base), 100);
      }

      private String clip(String s, int maxLength) {
        return s.length() < maxLength ? s : s.substring(0, maxLength);
      }

      @Override
      public void onError(Throwable throwable) {
        LOG.error("Error staging artifacts", throwable);
        state = State.ERROR;
      }

      @Override
      public void onCompleted() {
        assert state == State.DONE;
      }
    };
  }

  @Override
  public void close() throws Exception {
    // Nothing to close.
  }

  /**
   * Lazily stages artifacts by letting an ArtifactStagingService resolve and request artifacts.
   *
   * @param retrievalService an ArtifactRetrievalService used to resolve and retrieve artifacts
   * @param stagingService an ArtifactStagingService stub which will request artifacts
   * @param stagingToken the staging token of the job whose artifacts will be retrieved
   * @throws InterruptedException
   * @throws IOException
   */
  public static void offer(
      ArtifactRetrievalService retrievalService,
      ArtifactStagingServiceGrpc.ArtifactStagingServiceStub stagingService,
      String stagingToken)
      throws InterruptedException, IOException {
    StagingRequestObserver requestObserver = new StagingRequestObserver(retrievalService);
    requestObserver.responseObserver =
        stagingService.reverseArtifactRetrievalService(requestObserver);
    requestObserver.responseObserver.onNext(
        ArtifactApi.ArtifactResponseWrapper.newBuilder().setStagingToken(stagingToken).build());
    requestObserver.waitUntilDone();
    if (requestObserver.error != null) {
      if (requestObserver.error instanceof IOException) {
        throw (IOException) requestObserver.error;
      } else {
        throw new IOException(requestObserver.error);
      }
    }
  }

  /** Actually implements the reverse retrieval protocol. */
  private static class StagingRequestObserver
      implements StreamObserver<ArtifactApi.ArtifactRequestWrapper> {

    private ArtifactRetrievalService retrievalService;

    public StagingRequestObserver(ArtifactRetrievalService retrievalService) {
      this.retrievalService = retrievalService;
    }

    CountDownLatch latch = new CountDownLatch(1);
    StreamObserver<ArtifactApi.ArtifactResponseWrapper> responseObserver;
    Throwable error;

    @Override
    public void onNext(ArtifactApi.ArtifactRequestWrapper requestWrapper) {
      assert responseObserver != null;
      if (requestWrapper.hasResolveArtifact()) {
        retrievalService.resolveArtifacts(
            requestWrapper.getResolveArtifact(),
            new StreamObserver<ArtifactApi.ResolveArtifactsResponse>() {

              @Override
              public void onNext(ArtifactApi.ResolveArtifactsResponse resolveArtifactsResponse) {
                responseObserver.onNext(
                    ArtifactApi.ArtifactResponseWrapper.newBuilder()
                        .setResolveArtifactResponse(resolveArtifactsResponse)
                        .build());
              }

              @Override
              public void onError(Throwable throwable) {
                responseObserver.onError(throwable);
              }

              @Override
              public void onCompleted() {}
            });
      } else if (requestWrapper.hasGetArtifact()) {
        retrievalService.getArtifact(
            requestWrapper.getGetArtifact(),
            new StreamObserver<ArtifactApi.GetArtifactResponse>() {

              @Override
              public void onNext(ArtifactApi.GetArtifactResponse getArtifactResponse) {
                responseObserver.onNext(
                    ArtifactApi.ArtifactResponseWrapper.newBuilder()
                        .setGetArtifactResponse(getArtifactResponse)
                        .build());
              }

              @Override
              public void onError(Throwable throwable) {
                responseObserver.onError(throwable);
              }

              @Override
              public void onCompleted() {
                responseObserver.onNext(
                    ArtifactApi.ArtifactResponseWrapper.newBuilder()
                        .setGetArtifactResponse(
                            ArtifactApi.GetArtifactResponse.newBuilder().build())
                        .setIsLast(true)
                        .build());
              }
            });
      } else {
        responseObserver.onError(
            new StatusException(
                Status.INVALID_ARGUMENT.withDescription(
                    "Expected either a resolve or get request.")));
      }
    }

    @Override
    public void onError(Throwable throwable) {
      error = throwable;
      latch.countDown();
    }

    @Override
    public void onCompleted() {
      responseObserver.onCompleted();
      latch.countDown();
    }

    public void waitUntilDone() throws InterruptedException {
      latch.await();
    }
  }
}
