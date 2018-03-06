package org.apache.beam.runners.flink.execution;


import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.ArtifactChunk;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.Manifest;
import org.apache.beam.runners.flink.FlinkCachedArtifactNames;
import org.apache.beam.runners.fnexecution.artifact.ArtifactSource;
import org.apache.flink.api.common.cache.DistributedCache;

/**
 * An {@link org.apache.beam.runners.fnexecution.artifact.ArtifactSource} that draws artifacts
 * from the Flink Distributed File Cache {@link org.apache.flink.api.common.cache.DistributedCache}.
 */
public class CachedArtifactSource implements ArtifactSource {
  private static final int DEFAULT_CHUNK_SIZE_BYTES = 2 * 1024 * 1024;

  public static CachedArtifactSource createDefault(DistributedCache cache) {
    return new CachedArtifactSource(cache, FlinkCachedArtifactNames.createDefault());
  }

  public static CachedArtifactSource forToken(DistributedCache cache, String artifactToken) {
    return new CachedArtifactSource(cache, FlinkCachedArtifactNames.forToken(artifactToken));
  }

  private final DistributedCache cache;
  private final FlinkCachedArtifactNames paths;

  private CachedArtifactSource(DistributedCache cache, FlinkCachedArtifactNames paths) {
    this.cache = cache;
    this.paths = paths;
  }

  @Override
  public Manifest getManifest() throws IOException {
    String path = paths.getManifestHandle();
    File manifest = cache.getFile(path);
    try (BufferedInputStream fStream = new BufferedInputStream(new FileInputStream(manifest))) {
      return Manifest.parseFrom(fStream);

    }
  }

  @Override
  public void getArtifact(String name, StreamObserver<ArtifactChunk> responseObserver) {
    String path = paths.getArtifactHandle(name);
    File artifact = cache.getFile(path);
    try (FileInputStream fStream = new FileInputStream(artifact)) {
      byte[] buffer = new byte[DEFAULT_CHUNK_SIZE_BYTES];
      for (int bytesRead = fStream.read(buffer); bytesRead > 0; bytesRead = fStream.read(buffer)) {
        ByteString data = ByteString.copyFrom(buffer, 0, bytesRead);
        responseObserver.onNext(ArtifactChunk.newBuilder().setData(data).build());
      }
      responseObserver.onCompleted();
    } catch (FileNotFoundException e) {
      responseObserver.onError(
          Status.INVALID_ARGUMENT
              .withDescription(String.format("No such artifact %s", name))
              .withCause(e)
              .asException());
    } catch (Exception e) {
      responseObserver.onError(
          Status.INTERNAL
              .withDescription(
                  String.format("Could not retrieve artifact with name %s", name))
              .withCause(e)
              .asException());
    }
  }
}
