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
package org.apache.beam.sdk.util;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;

import com.google.api.services.storage.model.Bucket;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import org.apache.beam.sdk.options.CloudResourceManagerOptions;
import org.apache.beam.sdk.options.GcsOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.gcsfs.GcsPath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for handling default GCS buckets.
 */
public class DefaultBucket {
  static final Logger LOG = LoggerFactory.getLogger(DefaultBucket.class);

  static final String DEFAULT_REGION = "us-central1";

  /**
   * Creates a default bucket or verifies the existence and proper access control
   * of an existing default bucket.  Returns the location if successful.
   */
  public static String tryCreateDefaultBucket(PipelineOptions options) {
    GcsOptions gcpOptions = options.as(GcsOptions.class);

    final String projectId = gcpOptions.getProject();
    checkArgument(!isNullOrEmpty(projectId),
                  "--project is a required option.");

    // Look up the project number, to create a default bucket with a stable
    // name with no special characters.
    long projectNumber = 0L;
    try {
      projectNumber = gcpOptions.as(CloudResourceManagerOptions.class)
          .getGcpProjectUtil().getProjectNumber(projectId);
    } catch (IOException e) {
      throw new RuntimeException("Unable to verify project with ID " + projectId, e);
    }
    String region = DEFAULT_REGION;
    if (!isNullOrEmpty(gcpOptions.getZone())) {
      region = getRegionFromZone(gcpOptions.getZone());
    }
    final String bucketName =
      "dataflow-staging-" + region + "-" + projectNumber;
    LOG.info("No staging location provided, attempting to use default bucket: {}",
             bucketName);
    Bucket bucket = new Bucket()
      .setName(bucketName)
      .setLocation(region);
    // Always try to create the bucket before checking access, so that we do not
    // race with other pipelines that may be attempting to do the same thing.
    try {
      gcpOptions.getGcsUtil().createBucket(projectId, bucket);
    } catch (FileAlreadyExistsException e) {
      LOG.debug("Bucket '{}'' already exists, verifying access.", bucketName);
    } catch (IOException e) {
      throw new RuntimeException("Unable create default bucket.", e);
    }

    // Once the bucket is expected to exist, verify that it is correctly owned
    // by the project executing the job.
    try {
      long owner = gcpOptions.getGcsUtil().bucketOwner(
        GcsPath.fromComponents(bucketName, ""));
      checkArgument(
        owner == projectNumber,
        "Bucket owner does not match the project from --project:"
        + " %s vs. %s", owner, projectNumber);
    } catch (IOException e) {
      throw new RuntimeException(
        "Unable to determine the owner of the default bucket at gs://" + bucketName, e);
    }
    return "gs://" + bucketName;
  }

  @VisibleForTesting
  static String getRegionFromZone(String zone) {
    String[] zoneParts = zone.split("-");
    checkArgument(zoneParts.length >= 2, "Invalid zone provided: %s", zone);
    return zoneParts[0] + "-" + zoneParts[1];
  }
}
