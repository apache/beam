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
package org.apache.beam.it.gcp.storage;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.api.gax.paging.Page;
import com.google.auth.Credentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Notification;
import com.google.cloud.storage.NotificationInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.StorageException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import org.apache.beam.it.common.ResourceManager;
import org.apache.beam.it.gcp.artifacts.Artifact;
import org.apache.beam.it.gcp.artifacts.ArtifactClient;
import org.apache.beam.it.gcp.artifacts.GcsArtifact;
import org.apache.beam.it.gcp.artifacts.utils.ArtifactUtils;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client for working with test artifacts stored in Google Cloud Storage (GCS).
 *
 * <p>Tests should store this as a static value of the class and call {@link
 * ArtifactClient#cleanupAll()} in the {@code @AfterClass} method.
 */
public final class GcsResourceManager implements ArtifactClient, ResourceManager {
  private static final Logger LOG = LoggerFactory.getLogger(GcsResourceManager.class);
  private final List<String> managedTempDirs = new ArrayList<>();
  private final List<Notification> notificationList = new ArrayList<>();

  private final Storage client;
  private final String bucket;
  private final String testClassName;
  private final String runId;

  public GcsResourceManager(Builder builder) {
    this.client = ArtifactUtils.createStorageClient(builder.credentials);
    this.bucket = builder.bucket;
    this.testClassName = builder.testClassName;
    this.runId = ArtifactUtils.createRunId();

    managedTempDirs.add(joinPathParts(testClassName, runId));
  }

  @VisibleForTesting
  GcsResourceManager(Storage client, String bucket, String testClassName) {
    this.client = client;
    this.bucket = bucket;
    this.testClassName = testClassName;
    this.runId = ArtifactUtils.createRunId();

    managedTempDirs.add(joinPathParts(testClassName, runId));
  }

  /** Returns a new {@link Builder} for configuring a client. */
  public static Builder builder(String bucket, String testClassName, Credentials credentials) {
    checkArgument(!bucket.equals(""));
    checkArgument(!testClassName.equals(""));

    return new Builder(bucket, testClassName, credentials);
  }

  @Override
  public String runId() {
    return runId;
  }

  @Override
  public String getPathForArtifact(String artifactName) {
    return joinPathParts(testClassName, runId, artifactName);
  }

  @Override
  public Artifact createArtifact(String artifactName, String contents) {
    return this.createArtifact(artifactName, contents.getBytes(StandardCharsets.UTF_8));
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
   * Copies a file from a local path to a specified object name in Google Cloud Storage.
   *
   * @param localPath the path of the file to be copied.
   * @param objectName the name of the object to be created in Google Cloud Storage.
   * @return the URI of the copied object in Google Cloud Storage.
   * @throws IOException if there is an error reading the file at the specified local path.
   */
  public Artifact copyFileToGcs(Path localPath, String objectName) throws IOException {
    return createArtifact(objectName, Files.readAllBytes(localPath));
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
  public List<Artifact> listArtifacts(TestName testName, Pattern regex) {
    return listArtifacts(testName.getMethodName(), regex);
  }

  @Override
  public List<Artifact> listArtifacts(String prefix, Pattern regex) {
    String listFrom = joinPathParts(testClassName, runId, prefix);
    LOG.info(
        "Listing everything under 'gs://{}/{}' that matches '{}'",
        bucket,
        listFrom,
        regex.pattern());

    List<Artifact> matched = new ArrayList<>();
    Page<Blob> firstPage = getFirstPage(listFrom);
    consumePages(
        firstPage,
        blobs -> {
          for (Blob blob : blobs) {
            if (regex.matcher(blob.getName()).matches()) {
              matched.add(new GcsArtifact(blob));
            }
          }
        });

    return matched;
  }

  /**
   * Creates a new notification for the given topic and GCS prefix.
   *
   * @param topicName the name of the Pub/Sub topic to which the notification should be sent.
   * @param gcsPrefix the prefix of the object names to which the notification applies.
   * @return the created notification.
   */
  public Notification createNotification(String topicName, String gcsPrefix) {
    NotificationInfo notificationInfo =
        NotificationInfo.newBuilder(topicName)
            .setEventTypes(NotificationInfo.EventType.OBJECT_FINALIZE)
            .setObjectNamePrefix(gcsPrefix)
            .setPayloadFormat(NotificationInfo.PayloadFormat.JSON_API_V1)
            .build();
    try {
      Notification notification = client.createNotification(bucket, notificationInfo);
      LOG.info("Successfully created notification {}", notification);
      notificationList.add(notification);
      return notification;
    } catch (StorageException e) {
      throw new RuntimeException(
          String.format(
              "Unable to create notification for bucket %s. Notification: %s",
              bucket, notificationInfo),
          e);
    }
  }

  /**
   * Register a temporary directory that will be cleaned up after test.
   *
   * @param dirName name of the temporary directory
   */
  public void registerTempDir(String dirName) {
    managedTempDirs.add(dirName);
  }

  @Override
  public synchronized void cleanupAll() {
    if (notificationList.size() > 0) {
      for (Notification notification : notificationList) {
        client.deleteNotification(bucket, notification.getNotificationId());
      }
    }

    if (managedTempDirs.size() > 0) {
      LOG.info("managed temp dir size : {}", managedTempDirs.size());
      for (String tempDir : managedTempDirs) {
        LOG.info("Cleaning up everything under '{}' under bucket '{}'", tempDir, bucket);
        Page<Blob> firstPage = getFirstPage(tempDir);
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
                  LOG.debug("Blob '{}' was deleted", blobIds.get(i).getName());
                } else {
                  LOG.warn("Blob '{}' not deleted", blobIds.get(i).getName());
                }
              }
            });
      }
    }
    managedTempDirs.clear();
    notificationList.clear();
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

  /** Builder for {@link GcsResourceManager}. */
  public static final class Builder {
    private final String bucket;
    private final String testClassName;
    private Credentials credentials;

    private Builder(String bucket, String testClassName, Credentials credentials) {
      this.bucket = bucket;
      this.testClassName = testClassName;
      this.credentials = credentials;
    }

    public Builder setCredentials(Credentials credentials) {
      this.credentials = credentials;
      return this;
    }

    // TODO(zhoufek): Let users control the page size and other configurations

    public GcsResourceManager build() {
      return new GcsResourceManager(this);
    }
  }
}
