package org.apache.beam.runners.flink;

/**
 * Determines artifact path names within the
 * {@link org.apache.flink.api.common.cache.DistributedCache}.
 */
public class FlinkCachedArtifactNames {
  private static final String DEFAULT_ARTIFACT_TOKEN = "default";

  public static FlinkCachedArtifactNames createDefault() {
    return new FlinkCachedArtifactNames(DEFAULT_ARTIFACT_TOKEN);
  }

  public static FlinkCachedArtifactNames forToken(String artifactToken) {
    return new FlinkCachedArtifactNames(artifactToken);
  }

  private final String token;

  private FlinkCachedArtifactNames(String token) {
    this.token = token;
  }

  public String getArtifactHandle(String name) {
    return String.format("ARTIFACT_%s_%s", token, name);
  }

  public String getManifestHandle() {
    return String.format("MANIFEST_%s", token);
  }
}
