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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi;
import org.apache.beam.model.jobmanagement.v1.ArtifactStagingServiceGrpc;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.fnexecution.FnService;
import org.apache.beam.sdk.fn.IdGenerator;
import org.apache.beam.sdk.fn.IdGenerators;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MoveOptions;
import org.apache.beam.sdk.io.fs.ResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.Status;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.StatusException;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Charsets;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Splitter;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.hash.Hashing;
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

  public void removeStagedArtifacts(String stagingToken) throws IOException {
    destinationProvider.removeStagedArtifacts(stagingToken);
  }

  /** Provides a concrete location to which artifacts can be staged on retrieval. */
  public interface ArtifactDestinationProvider {
    ArtifactDestination getDestination(String stagingToken, String name) throws IOException;

    void removeStagedArtifacts(String stagingToken) throws IOException;
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
    return new ArtifactDestinationProvider() {
      @Override
      public ArtifactDestination getDestination(String stagingToken, String name)
          throws IOException {
        ResourceId path =
            stagingDir(stagingToken)
                .resolve(name, ResolveOptions.StandardResolveOptions.RESOLVE_FILE);
        return ArtifactDestination.fromFile(path.toString());
      }

      @Override
      public void removeStagedArtifacts(String stagingToken) throws IOException {
        // TODO(robertwb): Consider adding recursive delete.
        ResourceId stagingDir = stagingDir(stagingToken);
        List<ResourceId> toDelete = new ArrayList<>();
        for (MatchResult match :
            FileSystems.matchResources(
                ImmutableList.of(
                    stagingDir.resolve("*", ResolveOptions.StandardResolveOptions.RESOLVE_FILE)))) {
          for (MatchResult.Metadata m : match.metadata()) {
            toDelete.add(m.resourceId());
          }
        }
        FileSystems.delete(toDelete, MoveOptions.StandardMoveOptions.IGNORE_MISSING_FILES);
        FileSystems.delete(
            ImmutableList.of(stagingDir), MoveOptions.StandardMoveOptions.IGNORE_MISSING_FILES);
      }

      private ResourceId stagingDir(String stagingToken) {
        return FileSystems.matchNewResource(root, true)
            .resolve(
                Hashing.sha256().hashString(stagingToken, Charsets.UTF_8).toString(),
                ResolveOptions.StandardResolveOptions.RESOLVE_DIRECTORY);
      }
    };
  }

  private enum State {
    START,
    RESOLVE,
    GET,
    GETCHUNK,
    DONE,
    ERROR,
  }

  /**
   * Like the standard Semaphore, but allows an aquire to go over the limit if there is any room.
   *
   * <p>Also allows setting an error, to avoid issues with un-released aquires after error.
   */
  private static class OverflowingSemaphore {
    private int totalPermits;
    private int usedPermits;
    private Exception exception;

    public OverflowingSemaphore(int totalPermits) {
      this.totalPermits = totalPermits;
      this.usedPermits = 0;
    }

    synchronized void aquire(int permits) throws Exception {
      while (usedPermits >= totalPermits) {
        if (exception != null) {
          throw exception;
        }
        this.wait();
      }
      usedPermits += permits;
    }

    synchronized void release(int permits) {
      usedPermits -= permits;
      this.notifyAll();
    }

    synchronized void setException(Exception exception) {
      this.exception = exception;
      this.notifyAll();
    }
  }

  /** A task that pulls bytes off a queue and actually writes them to a staging location. */
  private class StoreArtifact implements Callable<RunnerApi.ArtifactInformation> {

    private String stagingToken;
    private String name;
    private RunnerApi.ArtifactInformation originalArtifact;
    private BlockingQueue<ByteString> bytesQueue;
    private OverflowingSemaphore totalPendingBytes;

    public StoreArtifact(
        String stagingToken,
        String name,
        RunnerApi.ArtifactInformation originalArtifact,
        BlockingQueue<ByteString> bytesQueue,
        OverflowingSemaphore totalPendingBytes) {
      this.stagingToken = stagingToken;
      this.name = name;
      this.originalArtifact = originalArtifact;
      this.bytesQueue = bytesQueue;
      this.totalPendingBytes = totalPendingBytes;
    }

    @Override
    public RunnerApi.ArtifactInformation call() throws IOException {
      try {
        ArtifactDestination dest = destinationProvider.getDestination(stagingToken, name);
        LOG.debug("Storing artifact for {}.{} at {}", stagingToken, name, dest);
        ByteString chunk = bytesQueue.take();
        while (chunk.size() > 0) {
          totalPendingBytes.release(chunk.size());
          chunk.writeTo(dest.getOutputStream());
          chunk = bytesQueue.take();
        }
        dest.getOutputStream().close();
        return originalArtifact
            .toBuilder()
            .setTypeUrn(dest.getTypeUrn())
            .setTypePayload(dest.getTypePayload())
            .build();
      } catch (IOException | InterruptedException exn) {
        // As this thread will no longer be draining the queue, we don't want to get stuck writing
        // to it.
        totalPendingBytes.setException(exn);
        LOG.error("Exception staging artifacts", exn);
        if (exn instanceof IOException) {
          throw (IOException) exn;
        } else {
          throw new RuntimeException(exn);
        }
      }
    }
  }

  @Override
  public StreamObserver<ArtifactApi.ArtifactResponseWrapper> reverseArtifactRetrievalService(
      StreamObserver<ArtifactApi.ArtifactRequestWrapper> responseObserver) {

    return new StreamObserver<ArtifactApi.ArtifactResponseWrapper>() {

      /** The maximum number of parallel threads to use to stage. */
      public static final int THREAD_POOL_SIZE = 10;

      /** The maximum number of bytes to buffer across all writes before throttling. */
      public static final int MAX_PENDING_BYTES = 100 << 20; // 100 MB

      IdGenerator idGenerator = IdGenerators.incrementingLongs();

      String stagingToken;
      Map<String, List<RunnerApi.ArtifactInformation>> toResolve;
      Map<String, List<Future<RunnerApi.ArtifactInformation>>> stagedFutures;
      ExecutorService stagingExecutor;
      OverflowingSemaphore totalPendingBytes;

      State state = State.START;
      Queue<String> pendingResolves;
      String currentEnvironment;
      Queue<RunnerApi.ArtifactInformation> pendingGets;
      BlockingQueue<ByteString> currentOutput;

      @Override
      @SuppressFBWarnings(value = "SF_SWITCH_FALLTHROUGH", justification = "fallthrough intended")
      // May be called by different threads for the same request; synchronized for memory
      // synchronization.
      public synchronized void onNext(ArtifactApi.ArtifactResponseWrapper responseWrapper) {
        switch (state) {
          case START:
            stagingToken = responseWrapper.getStagingToken();
            LOG.info("Staging artifacts for {}.", stagingToken);
            toResolve = toStage.get(stagingToken);
            if (toResolve == null) {
              responseObserver.onError(
                  new StatusException(
                      Status.INVALID_ARGUMENT.withDescription(
                          "Unknown staging token " + stagingToken)));
              return;
            }
            stagedFutures = new ConcurrentHashMap<>();
            pendingResolves = new ArrayDeque<>();
            pendingResolves.addAll(toResolve.keySet());
            stagingExecutor = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
            totalPendingBytes = new OverflowingSemaphore(MAX_PENDING_BYTES);
            resolveNextEnvironment(responseObserver);
            break;

          case RESOLVE:
            {
              currentEnvironment = pendingResolves.remove();
              stagedFutures.put(currentEnvironment, new ArrayList<>());
              pendingGets = new ArrayDeque<>();
              for (RunnerApi.ArtifactInformation artifact :
                  responseWrapper.getResolveArtifactResponse().getReplacementsList()) {
                Optional<RunnerApi.ArtifactInformation> fetched = getLocal(artifact);
                if (fetched.isPresent()) {
                  stagedFutures
                      .get(currentEnvironment)
                      .add(CompletableFuture.completedFuture(fetched.get()));
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
            RunnerApi.ArtifactInformation currentArtifact = pendingGets.remove();
            String name = createFilename(currentEnvironment, currentArtifact);
            try {
              LOG.debug("Storing artifacts for {} as {}", stagingToken, name);
              currentOutput = new ArrayBlockingQueue<ByteString>(100);
              stagedFutures
                  .get(currentEnvironment)
                  .add(
                      stagingExecutor.submit(
                          new StoreArtifact(
                              stagingToken,
                              name,
                              currentArtifact,
                              currentOutput,
                              totalPendingBytes)));
            } catch (Exception exn) {
              LOG.error("Error submitting.", exn);
              responseObserver.onError(exn);
            }
            state = State.GETCHUNK;
            // fall through

          case GETCHUNK:
            try {
              ByteString chunk = responseWrapper.getGetArtifactResponse().getData();
              if (chunk.size() > 0) { // Make sure we don't accidentally send the EOF value.
                totalPendingBytes.aquire(chunk.size());
                currentOutput.put(chunk);
              }
              if (responseWrapper.getIsLast()) {
                currentOutput.put(ByteString.EMPTY); // The EOF value.
                if (pendingGets.isEmpty()) {
                  resolveNextEnvironment(responseObserver);
                } else {
                  state = State.GET;
                  LOG.debug("Waiting for {}", pendingGets.peek());
                }
              }
            } catch (Exception exn) {
              LOG.error("Error submitting.", exn);
              onError(exn);
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
          finishStaging(responseObserver);
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

      private void finishStaging(
          StreamObserver<ArtifactApi.ArtifactRequestWrapper> responseObserver) {
        LOG.debug("Finishing staging for {}.", stagingToken);
        Map<String, List<RunnerApi.ArtifactInformation>> staged = new HashMap<>();
        try {
          for (Map.Entry<String, List<Future<RunnerApi.ArtifactInformation>>> entry :
              stagedFutures.entrySet()) {
            List<RunnerApi.ArtifactInformation> envStaged = new ArrayList<>();
            for (Future<RunnerApi.ArtifactInformation> future : entry.getValue()) {
              envStaged.add(future.get());
            }
            staged.put(entry.getKey(), envStaged);
          }
          ArtifactStagingService.this.staged.put(stagingToken, staged);
          stagingExecutor.shutdown();
          state = State.DONE;
          LOG.info("Artifacts fully staged for {}.", stagingToken);
          responseObserver.onCompleted();
        } catch (Exception exn) {
          LOG.error("Error staging artifacts", exn);
          responseObserver.onError(exn);
          state = State.ERROR;
          return;
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
      private String createFilename(String environment, RunnerApi.ArtifactInformation artifact) {
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
        return clip(
            String.format("%s-%s-%s", idGenerator.getId(), clip(environment, 25), base), 100);
      }

      private String clip(String s, int maxLength) {
        return s.length() < maxLength ? s : s.substring(0, maxLength);
      }

      @Override
      public void onError(Throwable throwable) {
        stagingExecutor.shutdownNow();
        LOG.error("Error staging artifacts", throwable);
        state = State.ERROR;
      }

      @Override
      public void onCompleted() {
        Preconditions.checkArgument(state == State.DONE);
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
      throws ExecutionException, InterruptedException {
    new StagingDriver(retrievalService, stagingService, stagingToken).getCompletionFuture().get();
  }

  /** Actually implements the reverse retrieval protocol. */
  private static class StagingDriver implements StreamObserver<ArtifactApi.ArtifactRequestWrapper> {

    private final ArtifactRetrievalService retrievalService;
    private final StreamObserver<ArtifactApi.ArtifactResponseWrapper> responseObserver;
    private final CompletableFuture<Void> completionFuture;

    public StagingDriver(
        ArtifactRetrievalService retrievalService,
        ArtifactStagingServiceGrpc.ArtifactStagingServiceStub stagingService,
        String stagingToken) {
      this.retrievalService = retrievalService;
      completionFuture = new CompletableFuture<Void>();
      responseObserver = stagingService.reverseArtifactRetrievalService(this);
      responseObserver.onNext(
          ArtifactApi.ArtifactResponseWrapper.newBuilder().setStagingToken(stagingToken).build());
    }

    public CompletableFuture<?> getCompletionFuture() {
      return completionFuture;
    }

    @Override
    public void onNext(ArtifactApi.ArtifactRequestWrapper requestWrapper) {

      if (completionFuture.isCompletedExceptionally()) {
        try {
          completionFuture.get();
        } catch (Throwable th) {
          responseObserver.onError(th);
          return;
        }
      }

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
                completionFuture.completeExceptionally(throwable);
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
                completionFuture.completeExceptionally(throwable);
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
        Throwable exn =
            new StatusException(
                Status.INVALID_ARGUMENT.withDescription(
                    "Expected either a resolve or get request."));
        completionFuture.completeExceptionally(exn);
        responseObserver.onError(exn);
      }
    }

    @Override
    public void onError(Throwable throwable) {
      completionFuture.completeExceptionally(throwable);
    }

    @Override
    public void onCompleted() {
      responseObserver.onCompleted();
      completionFuture.complete(null);
    }
  }
}
