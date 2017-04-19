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

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

import com.google.api.services.storage.model.Bucket;
import java.io.IOException;
import org.apache.beam.sdk.options.CloudResourceManagerOptions;
import org.apache.beam.sdk.options.GcpOptions;
import org.apache.beam.sdk.options.GcsOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.util.gcsfs.GcsPath;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.MockitoAnnotations;
import org.mockito.MockitoAnnotations.Mock;

/** Tests for DefaultBucket. */
@RunWith(JUnit4.class)
public class DefaultBucketTest {
  @Rule public ExpectedException thrown = ExpectedException.none();

  private PipelineOptions options;
  @Mock
  private GcsUtil gcsUtil;
  @Mock
  private GcpProjectUtil gcpUtil;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    options = PipelineOptionsFactory.create();
    options.as(GcsOptions.class).setGcsUtil(gcsUtil);
    options.as(CloudResourceManagerOptions.class).setGcpProjectUtil(gcpUtil);
    options.as(GcpOptions.class).setProject("foo");
    options.as(GcpOptions.class).setZone("us-north1-a");
  }

  @Test
  public void testCreateBucket() {
    String bucket = DefaultBucket.tryCreateDefaultBucket(options);
    assertEquals("gs://dataflow-staging-us-north1-0", bucket);
  }

  @Test
  public void testCreateBucketProjectLookupFails() throws IOException {
    when(gcpUtil.getProjectNumber("foo")).thenThrow(new IOException("badness"));

    thrown.expect(RuntimeException.class);
    thrown.expectMessage("Unable to verify project");
    DefaultBucket.tryCreateDefaultBucket(options);
  }

  @Test
  public void testCreateBucketCreateBucketFails() throws IOException {
    doThrow(new IOException("badness")).when(
      gcsUtil).createBucket(any(String.class), any(Bucket.class));

    thrown.expect(RuntimeException.class);
    thrown.expectMessage("Unable create default bucket");
    DefaultBucket.tryCreateDefaultBucket(options);
  }

  @Test
  public void testCannotGetBucketOwner() throws IOException {
    when(gcsUtil.bucketOwner(any(GcsPath.class)))
      .thenThrow(new IOException("badness"));

    thrown.expect(RuntimeException.class);
    thrown.expectMessage("Unable to determine the owner");
    DefaultBucket.tryCreateDefaultBucket(options);
  }

  @Test
  public void testProjectMismatch() throws IOException {
    when(gcsUtil.bucketOwner(any(GcsPath.class))).thenReturn(5L);

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Bucket owner does not match the project");
    DefaultBucket.tryCreateDefaultBucket(options);
  }

  @Test
  public void regionFromZone() throws IOException {
    assertEquals("us-central1", DefaultBucket.getRegionFromZone("us-central1-a"));
    assertEquals("asia-east", DefaultBucket.getRegionFromZone("asia-east-a"));
  }
}
