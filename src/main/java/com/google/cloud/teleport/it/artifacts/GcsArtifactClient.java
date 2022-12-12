/*
 * Copyright (C) 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.it.artifacts;

import static com.google.cloud.teleport.it.artifacts.ArtifactUtils.createRunId;
import static com.google.common.base.Preconditions.checkArgument;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.re2j.Pattern;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client for working with test artifacts stored in Google Cloud Storage.
 *
 * <p>Tests should store this as a static value of the class and call {@link
 * ArtifactClient#cleanupRun()} in the {@code @AfterClass} method.
 */
public final class GcsArtifactClient implements ArtifactClient {
  private static final Logger LOG = LoggerFactory.getLogger(GcsArtifactClient.class);

  private final Storage client;
  private final String bucket;
  private final String testClassName;
  private final String runId;

  public GcsArtifactClient(Builder builder) {
    this.client = builder.client;
    this.bucket = builder.bucket;
    this.testClassName = builder.testClassName;
    this.runId = createRunId();
  }

  /** Returns a new {@link Builder} for configuring a client. */
  public static Builder builder(Storage client, String bucket, String testClassName) {
    checkArgument(!bucket.equals(""));
    checkArgument(!testClassName.equals(""));

    return new Builder(client, bucket, testClassName);
  }

  @Override
  public String runId() {
    return runId;
  }

  @Override
  public Artifact createArtifact(String artifactName, byte[] contents) {
    String path = joinPathParts(testClassName, runId, artifactName);
    return handleCreate(path, contents);
  }

  @Override
  public Artifact uploadArtifact(String artifactName, String localPath) throws IOException {
    return uploadArtifact(artifactName, Paths.get(localPath));
  }

  @Override
  public Artifact uploadArtifact(String artifactName, Path localPath) throws IOException {
    LOG.info(
        "Uploading '{}' to file '{}' under '{}'",
        localPath,
        artifactName,
        joinPathParts(testClassName, runId));
    return createArtifact(artifactName, Files.readAllBytes(localPath));
  }

  /**
   * Helper for creating an artifact.
   *
   * @param path the full path under the bucket
   * @param contents the contents of the artifact
   * @return a representation of the artifact
   */
  private Artifact handleCreate(String path, byte[] contents) {
    LOG.info("Uploading {} bytes to '{}' under bucket '{}'", contents.length, path, bucket);

    BlobId id = BlobId.of(bucket, path);
    BlobInfo info = BlobInfo.newBuilder(id).build();
    Blob blob = client.create(info, contents);
    LOG.info(
        "Successfully uploaded {} bytes to '{}' under bucket '{}'", contents.length, path, bucket);

    return new GcsArtifact(blob);
  }

  @Override
  public List<Artifact> listArtifacts(String prefix, Pattern regex) {
    String listFrom = joinPathParts(testClassName, runId, prefix);
    LOG.info("Listing everything under '{}' that matches '{}'", listFrom, regex.pattern());

    List<Artifact> matched = new ArrayList<>();
    Page<Blob> firstPage = getFirstPage(listFrom);
    consumePages(
        firstPage,
        blobs -> {
          for (Blob blob : blobs) {
            if (regex.matches(blob.getName())) {
              matched.add(new GcsArtifact(blob));
            }
          }
        });

    return matched;
  }

  @Override
  public void cleanupRun() {
    String path = joinPathParts(testClassName, runId);
    LOG.info("Cleaning up everything under '{}' under bucket '{}'", path, bucket);

    Page<Blob> firstPage = getFirstPage(path);
    consumePages(
        firstPage,
        blobs -> {
          // For testability, use the Iterable<BlobId> overload
          List<BlobId> blobIds = new ArrayList<>();
          for (Blob blob : blobs) {
            blobIds.add(blob.getBlobId());
          }
          if (blobIds.isEmpty()) {
            return;
          }

          List<Boolean> deleted = client.delete(blobIds);
          for (int i = 0; i < deleted.size(); ++i) {
            if (deleted.get(i)) {
              LOG.info("Blob '{}' was deleted", blobIds.get(i).getName());
            } else {
              LOG.warn("Blob '{}' not deleted", blobIds.get(i).getName());
            }
          }
        });
  }

  private void consumePages(Page<Blob> firstPage, Consumer<Iterable<Blob>> consumeBlobs) {
    Page<Blob> currentPage = firstPage;
    while (true) {
      consumeBlobs.accept(currentPage.getValues());
      if (currentPage.hasNextPage()) {
        currentPage = currentPage.getNextPage();
      } else {
        break;
      }
    }
  }

  private Page<Blob> getFirstPage(String prefix) {
    return client.list(bucket, BlobListOption.prefix(prefix));
  }

  private static String joinPathParts(String... parts) {
    return String.join("/", parts);
  }

  /** Builder for {@link GcsArtifactClient}. */
  public static final class Builder {
    private final Storage client;
    private final String bucket;
    private final String testClassName;

    private Builder(Storage client, String bucket, String testClassName) {
      this.client = client;
      this.bucket = bucket;
      this.testClassName = testClassName;
    }

    // TODO(zhoufek): Let users control the page size and other configurations

    public GcsArtifactClient build() {
      return new GcsArtifactClient(this);
    }
  }
}
