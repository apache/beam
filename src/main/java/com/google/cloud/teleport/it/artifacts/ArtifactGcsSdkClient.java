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

import static com.google.common.collect.ImmutableList.toImmutableList;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.common.collect.ImmutableList;
import com.google.re2j.Pattern;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.StreamSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Client for working with test artifacts which uses the GCS SDK. */
public final class ArtifactGcsSdkClient implements ArtifactClient {
  private static final Logger LOG = LoggerFactory.getLogger(ArtifactGcsSdkClient.class);

  private final Storage client;

  public ArtifactGcsSdkClient(Storage client) {
    this.client = client;
  }

  @Override
  public Blob uploadArtifact(String bucket, String gcsPath, String localPath) throws IOException {
    LOG.info("Uploading {} to {} under {}", localPath, gcsPath, bucket);
    BlobId id = BlobId.of(bucket, gcsPath);
    BlobInfo info = BlobInfo.newBuilder(id).build();

    byte[] contents = Files.readAllBytes(Paths.get(localPath));

    return client.create(info, contents);
  }

  @Override
  public List<Blob> listArtifacts(String bucket, String testDirPath, Pattern regex) {
    List<Blob> result = new ArrayList<>();
    consumeTestDir(
        bucket,
        testDirPath,
        blobs -> {
          for (Blob blob : blobs) {
            if (regex.matches(blob.getName())) {
              result.add(blob);
            }
          }
        });
    return result;
  }

  @Override
  public void deleteTestDir(String bucket, String testDirPath) {
    LOG.info("Deleting everything in {} under {}", testDirPath, bucket);
    consumeTestDir(
        bucket,
        testDirPath,
        blobs -> {
          // Go through the Iterable<BlobId> overload, since the other ones make it very difficult
          // to
          // do thorough testing with Mockito
          ImmutableList<BlobId> blobIds =
              StreamSupport.stream(blobs.spliterator(), false)
                  .map(Blob::getBlobId)
                  .collect(toImmutableList());
          if (blobIds.isEmpty()) {
            return;
          }
          List<Boolean> deleted = client.delete(blobIds);
          for (int i = 0; i < blobIds.size(); ++i) {
            if (!deleted.get(i)) {
              LOG.warn("Blob {} not deleted", blobIds.get(i).getName());
            }
          }
        });
  }

  private void consumeTestDir(
      String bucket, String testDirPath, Consumer<Iterable<Blob>> consumeBlobs) {
    Page<Blob> blobs = getFirstTestDirPage(bucket, testDirPath);
    while (true) {
      consumeBlobs.accept(blobs.getValues());

      if (blobs.hasNextPage()) {
        blobs = blobs.getNextPage();
      } else {
        break;
      }
    }
  }

  private Page<Blob> getFirstTestDirPage(String bucket, String testDirPath) {
    return client.list(bucket, BlobListOption.prefix(testDirPath));
  }
}
