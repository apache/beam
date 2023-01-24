package org.apache.beam.runners.core.construction;

import java.util.Iterator;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi;

public interface ArtifactServiceClient extends AutoCloseable {
    ArtifactApi.ResolveArtifactsResponse resolveArtifacts(ArtifactApi.ResolveArtifactsRequest request);
    Iterator<ArtifactApi.GetArtifactResponse> getArtifact(ArtifactApi.GetArtifactRequest request);
}