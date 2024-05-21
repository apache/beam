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

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.internal.matchers.ThrowableMessageMatcher.hasMessage;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.services.cloudresourcemanager.CloudResourceManager;
import com.google.api.services.cloudresourcemanager.CloudResourceManager.Projects;
import com.google.api.services.cloudresourcemanager.CloudResourceManager.Projects.Get;
import com.google.api.services.cloudresourcemanager.model.Project;
import com.google.api.services.storage.model.Bucket;
import com.google.api.services.storage.model.Bucket.SoftDeletePolicy;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.beam.sdk.extensions.gcp.auth.TestCredential;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions.DefaultProjectFactory;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions.GcpTempLocationFactory;
import org.apache.beam.sdk.extensions.gcp.storage.NoopPathValidator;
import org.apache.beam.sdk.extensions.gcp.util.GcsUtil;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.RestoreSystemProperties;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.Files;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Tests for {@link GcpOptions}. */
public class GcpOptionsTest {

  /** Tests for the majority of methods. */
  @RunWith(JUnit4.class)
  public static class CommonTests {
    @Rule public TestRule restoreSystemProperties = new RestoreSystemProperties();
    @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();
    @Rule public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testGetProjectFromCloudSdkConfigEnv() throws Exception {
      Map<String, String> environment =
          ImmutableMap.of("CLOUDSDK_CONFIG", tmpFolder.getRoot().getAbsolutePath());
      assertEquals("test-project", runGetProjectTest(tmpFolder.newFile("properties"), environment));
    }

    @Test
    public void testGetProjectFromAppDataEnv() throws Exception {
      Map<String, String> environment =
          ImmutableMap.of("APPDATA", tmpFolder.getRoot().getAbsolutePath());
      System.setProperty("os.name", "windows");
      assertEquals(
          "test-project",
          runGetProjectTest(new File(tmpFolder.newFolder("gcloud"), "properties"), environment));
    }

    @Test
    public void testGetProjectFromUserHomeEnvOld() throws Exception {
      Map<String, String> environment = ImmutableMap.of();
      System.setProperty("user.home", tmpFolder.getRoot().getAbsolutePath());
      assertEquals(
          "test-project",
          runGetProjectTest(
              new File(tmpFolder.newFolder(".config", "gcloud"), "properties"), environment));
    }

    @Test
    public void testGetProjectFromUserHomeEnv() throws Exception {
      Map<String, String> environment = ImmutableMap.of();
      System.setProperty("user.home", tmpFolder.getRoot().getAbsolutePath());
      assertEquals(
          "test-project",
          runGetProjectTest(
              new File(
                  tmpFolder.newFolder(".config", "gcloud", "configurations"), "config_default"),
              environment));
    }

    @Test
    public void testGetProjectFromUserHomeOldAndNewPrefersNew() throws Exception {
      Map<String, String> environment = ImmutableMap.of();
      System.setProperty("user.home", tmpFolder.getRoot().getAbsolutePath());
      makePropertiesFileWithProject(
          new File(tmpFolder.newFolder(".config", "gcloud"), "properties"), "old-project");
      assertEquals(
          "test-project",
          runGetProjectTest(
              new File(
                  tmpFolder.newFolder(".config", "gcloud", "configurations"), "config_default"),
              environment));
    }

    @Test
    public void testUnableToGetDefaultProject() throws Exception {
      System.setProperty("user.home", tmpFolder.getRoot().getAbsolutePath());
      DefaultProjectFactory projectFactory = spy(new DefaultProjectFactory());
      when(projectFactory.getEnvironment()).thenReturn(ImmutableMap.of());
      assertNull(projectFactory.create(PipelineOptionsFactory.create()));
    }

    @Test
    public void testEmptyGcpTempLocation() throws Exception {
      GcpOptions options = PipelineOptionsFactory.as(GcpOptions.class);
      options.setGcpCredential(new TestCredential());
      options.setProject("");
      thrown.expect(IllegalArgumentException.class);
      thrown.expectMessage("--project is a required option");
      options.getGcpTempLocation();
    }

    @Test
    public void testDefaultGcpTempLocation() throws Exception {
      GcpOptions options = PipelineOptionsFactory.as(GcpOptions.class);
      String tempLocation = "gs://bucket";
      options.setTempLocation(tempLocation);
      options.as(GcsOptions.class).setPathValidatorClass(NoopPathValidator.class);
      assertEquals(tempLocation, options.getGcpTempLocation());
    }

    @Test
    public void testDefaultGcpTempLocationInvalid() throws Exception {
      GcpOptions options = PipelineOptionsFactory.as(GcpOptions.class);
      options.setTempLocation("file://");
      thrown.expect(IllegalArgumentException.class);
      thrown.expectMessage(
          "Error constructing default value for gcpTempLocation: tempLocation is not"
              + " a valid GCS path");
      options.getGcpTempLocation();
    }

    private static void makePropertiesFileWithProject(File path, String projectId)
        throws IOException {
      String properties =
          String.format(
              "[core]%n"
                  + "account = test-account@google.com%n"
                  + "project = %s%n"
                  + "%n"
                  + "[dataflow]%n"
                  + "magic = true%n",
              projectId);
      Files.asCharSink(path, StandardCharsets.UTF_8).write(properties);
    }

    private static String runGetProjectTest(File path, Map<String, String> environment)
        throws Exception {
      makePropertiesFileWithProject(path, "test-project");
      DefaultProjectFactory projectFactory = spy(new DefaultProjectFactory());
      when(projectFactory.getEnvironment()).thenReturn(environment);
      return projectFactory.create(PipelineOptionsFactory.create());
    }
  }

  /** Tests related to determining the GCP temp location. */
  @RunWith(JUnit4.class)
  public static class GcpTempLocationTest {
    @Rule public ExpectedException thrown = ExpectedException.none();
    @Mock private GcsUtil mockGcsUtil;
    @Mock private CloudResourceManager mockCrmClient;
    @Mock private Projects mockProjects;
    @Mock private Get mockGet;
    private Project fakeProject;
    private PipelineOptions options;

    @Before
    public void setUp() throws Exception {
      MockitoAnnotations.initMocks(this);
      options = PipelineOptionsFactory.create();
      options.as(GcsOptions.class).setGcsUtil(mockGcsUtil);
      options.as(GcpOptions.class).setProject("foo");
      options.as(GcpOptions.class).setZone("us-north1-a");
      when(mockCrmClient.projects()).thenReturn(mockProjects);
      when(mockProjects.get(any(String.class))).thenReturn(mockGet);
      fakeProject = new Project().setProjectNumber(1L);
    }

    @Test
    public void testDefaultGcpTempLocationDoesNotExist() throws IOException {
      String tempLocation = "gs://does/not/exist";
      options.setTempLocation(tempLocation);
      doThrow(new FileNotFoundException())
          .when(mockGcsUtil)
          .verifyBucketAccessible(any(GcsPath.class));
      thrown.expect(IllegalArgumentException.class);
      thrown.expectMessage(
          "Error constructing default value for gcpTempLocation: tempLocation is not"
              + " a valid GCS path");
      thrown.expectCause(
          hasMessage(containsString("Output path does not exist or is not writeable")));

      options.as(GcpOptions.class).getGcpTempLocation();
    }

    @Test
    public void testCreateBucket() throws Exception {
      doReturn(fakeProject).when(mockGet).execute();
      when(mockGcsUtil.bucketOwner(any(GcsPath.class))).thenReturn(1L);

      String bucket = GcpTempLocationFactory.tryCreateDefaultBucket(options, mockCrmClient);
      assertEquals("gs://dataflow-staging-us-north1-1/temp/", bucket);

      ArgumentCaptor<Bucket> bucketArg = ArgumentCaptor.forClass(Bucket.class);
      verify(mockGcsUtil, times(1)).createBucket(anyString(), bucketArg.capture());

      // verify that the soft delete policy is disabled in the default bucket
      assertEquals(
          bucketArg.getValue().getSoftDeletePolicy().getRetentionDurationSeconds(),
          Long.valueOf(0L));
    }

    @Test
    public void testTempLocationWithSoftDeletePolicy() throws IOException {
      Bucket bucket = new Bucket();
      bucket.setSoftDeletePolicy(new SoftDeletePolicy().setRetentionDurationSeconds(1L));
      when(mockGcsUtil.getBucket(any(GcsPath.class))).thenReturn(bucket);

      String tempLocation = "gs://bucket_with_soft_delete";
      options.setTempLocation(tempLocation);
      options.as(GcsOptions.class).setPathValidatorClass(NoopPathValidator.class);

      GcpOptions gcpOptions = options.as(GcpOptions.class);
      assertEquals(tempLocation, gcpOptions.getGcpTempLocation());

      assertTrue(GcpTempLocationFactory.isSoftDeletePolicyEnabled(options, tempLocation));
    }

    @Test
    public void testTempLocationWithoutSoftDeletePolicy() throws IOException {
      Bucket bucket = new Bucket();
      bucket.setSoftDeletePolicy(new SoftDeletePolicy().setRetentionDurationSeconds(0L));
      when(mockGcsUtil.getBucket(any(GcsPath.class))).thenReturn(bucket);

      String tempLocation = "gs://bucket_without_soft_delete";
      options.setTempLocation(tempLocation);
      options.as(GcsOptions.class).setPathValidatorClass(NoopPathValidator.class);

      GcpOptions gcpOptions = options.as(GcpOptions.class);
      assertEquals(tempLocation, gcpOptions.getGcpTempLocation());

      assertFalse(GcpTempLocationFactory.isSoftDeletePolicyEnabled(options, tempLocation));
    }

    @Test
    public void testCreateBucketProjectLookupFails() throws Exception {
      doThrow(new IOException("badness")).when(mockGet).execute();

      thrown.expect(RuntimeException.class);
      thrown.expectMessage("Unable to verify project");
      GcpTempLocationFactory.tryCreateDefaultBucket(options, mockCrmClient);
    }

    @Test
    public void testCreateBucketCreateBucketFails() throws Exception {
      doReturn(fakeProject).when(mockGet).execute();
      doThrow(new IOException("badness"))
          .when(mockGcsUtil)
          .createBucket(any(String.class), any(Bucket.class));

      thrown.expect(RuntimeException.class);
      thrown.expectMessage("Unable create default bucket");
      GcpTempLocationFactory.tryCreateDefaultBucket(options, mockCrmClient);
    }

    @Test
    public void testCannotGetBucketOwner() throws Exception {
      doReturn(fakeProject).when(mockGet).execute();
      when(mockGcsUtil.bucketOwner(any(GcsPath.class))).thenThrow(new IOException("badness"));

      thrown.expect(RuntimeException.class);
      thrown.expectMessage("Unable to determine the owner");
      GcpTempLocationFactory.tryCreateDefaultBucket(options, mockCrmClient);
    }

    @Test
    public void testProjectMismatch() throws Exception {
      doReturn(fakeProject).when(mockGet).execute();
      when(mockGcsUtil.bucketOwner(any(GcsPath.class))).thenReturn(5L);

      thrown.expect(IllegalArgumentException.class);
      thrown.expectMessage("Bucket owner does not match the project");
      GcpTempLocationFactory.tryCreateDefaultBucket(options, mockCrmClient);
    }

    @Test
    public void testCreateBucketCreateWithKmsFails() throws Exception {
      doReturn(fakeProject).when(mockGet).execute();
      options.as(GcpOptions.class).setDataflowKmsKey("kms_key");

      thrown.expect(RuntimeException.class);
      thrown.expectMessage("dataflowKmsKey");
      GcpTempLocationFactory.tryCreateDefaultBucket(options, mockCrmClient);
    }

    @Test
    public void regionFromZone() throws Exception {
      assertEquals("us-central1", GcpTempLocationFactory.getRegionFromZone("us-central1-a"));
      assertEquals("asia-east", GcpTempLocationFactory.getRegionFromZone("asia-east-a"));
    }
  }
}
