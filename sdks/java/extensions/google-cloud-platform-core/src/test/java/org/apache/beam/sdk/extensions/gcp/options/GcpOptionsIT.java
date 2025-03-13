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
package org.apache.beam.sdk.extensions.gcp.options;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

import com.google.api.services.cloudresourcemanager.CloudResourceManager;
import com.google.api.services.storage.model.Bucket;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Random;
import org.apache.beam.sdk.extensions.gcp.util.GcsUtil;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Integration tests for {@link GcpOptions}. These tests are designed to run against production
 * Google Cloud Storage.
 *
 * <p>This is a runnerless integration test, even though the Beam IT framework assumes one. Thus,
 * this test should only be run against single runner (such as DirectRunner).
 */
@RunWith(JUnit4.class)
public class GcpOptionsIT {
  /** Tests the creation of a default bucket in a project. */
  @Test
  public void testCreateDefaultBucket() throws IOException {
    TestPipelineOptions options =
        TestPipeline.testingPipelineOptions().as(TestPipelineOptions.class);

    CloudResourceManager crmClient =
        GcpOptions.GcpTempLocationFactory.newCloudResourceManagerClient(
                options.as(CloudResourceManagerOptions.class))
            .build();

    GcsOptions gcsOptions = options.as(GcsOptions.class);
    GcsUtil gcsUtil = gcsOptions.getGcsUtil();

    Random rand = new Random();
    // Add a random number to the prefix to avoid collision if multiple test instances
    // are run at the same time. To avoid too many dangling buckets if bucket removal fails,
    // we limit the max number of possible bucket names in this test to 1000.
    String bucketNamePrefix = "gcp-options-it-" + rand.nextInt(1000);

    String bucketName =
        String.join(
            "-",
            GcpOptions.GcpTempLocationFactory.getDefaultBucketNameStubs(
                options, crmClient, bucketNamePrefix));

    // remove existing default bucket if any
    try {
      Bucket oldBucket = gcsUtil.getBucket(GcsPath.fromUri("gs://" + bucketName));
      gcsUtil.removeBucket(oldBucket);
    } catch (FileNotFoundException e) {
      // the bucket to be created does not exist, which is good news
    }

    String tempLocation =
        GcpOptions.GcpTempLocationFactory.tryCreateDefaultBucketWithPrefix(
            options, crmClient, bucketNamePrefix);

    GcsPath gcsPath = GcsPath.fromUri(tempLocation);
    Bucket bucket = gcsUtil.getBucket(gcsPath);
    assertNotNull(bucket);
    // verify the soft delete policy is disabled
    assertEquals(bucket.getSoftDeletePolicy().getRetentionDurationSeconds(), Long.valueOf(0L));

    gcsUtil.removeBucket(bucket);
    assertThrows(FileNotFoundException.class, () -> gcsUtil.getBucket(gcsPath));
  }
}
