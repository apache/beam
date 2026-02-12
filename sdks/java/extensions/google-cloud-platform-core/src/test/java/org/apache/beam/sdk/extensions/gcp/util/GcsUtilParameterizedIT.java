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
package org.apache.beam.sdk.extensions.gcp.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.api.gax.paging.Page;
import com.google.api.services.storage.model.Bucket;
import com.google.api.services.storage.model.Objects;
import com.google.api.services.storage.model.StorageObject;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BucketInfo;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileAlreadyExistsException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.extensions.gcp.util.GcsUtilV2.MissingStrategy;
import org.apache.beam.sdk.extensions.gcp.util.GcsUtilV2.OverwriteStrategy;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.testing.UsesKms;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

/**
 * Integration tests for {@link GcsUtil}. These tests are designed to run against production Google
 * Cloud Storage.
 *
 * <p>This is a runnerless integration test, even though the Beam IT framework assumes one. Thus,
 * this test should only be run against single runner (such as DirectRunner).
 */
@RunWith(Parameterized.class)
@Category(UsesKms.class)
public class GcsUtilParameterizedIT {

  @Parameters(name = "{0}")
  public static Iterable<String> data() {
    return Arrays.asList("use_gcsutil_v1", "use_gcsutil_v2");
  }

  @Parameter public String experiment;

  private TestPipelineOptions options;
  private GcsUtil gcsUtil;

  @Before
  public void setUp() {
    options = TestPipeline.testingPipelineOptions().as(TestPipelineOptions.class);

    // set the experimental flag.
    ExperimentalOptions experimentalOptions = options.as(ExperimentalOptions.class);
    experimentalOptions.setExperiments(Collections.singletonList(experiment));

    GcsOptions gcsOptions = options.as(GcsOptions.class);
    gcsUtil = gcsOptions.getGcsUtil();
  }

  @Test
  public void testFileSize() throws IOException {
    final GcsPath gcsPath = GcsPath.fromUri("gs://apache-beam-samples/shakespeare/kinglear.txt");
    final long expectedSize = 157283L;

    assertEquals(expectedSize, gcsUtil.fileSize(gcsPath));
  }

  @Test
  public void testGetObjectOrGetBlob() throws IOException {
    final GcsPath existingPath =
        GcsPath.fromUri("gs://apache-beam-samples/shakespeare/kinglear.txt");
    final String expectedCRC = "s0a3Tg==";

    String crc;
    if (experiment.equals("use_gcsutil_v2")) {
      Blob blob = gcsUtil.getBlob(existingPath);
      crc = blob.getCrc32c();
    } else {
      StorageObject obj = gcsUtil.getObject(existingPath);
      crc = obj.getCrc32c();
    }
    assertEquals(expectedCRC, crc);

    final GcsPath nonExistentPath =
        GcsPath.fromUri("gs://my-random-test-bucket-12345/unknown-12345.txt");
    final GcsPath forbiddenPath = GcsPath.fromUri("gs://test-bucket/unknown-12345.txt");

    if (experiment.equals("use_gcsutil_v2")) {
      assertThrows(FileNotFoundException.class, () -> gcsUtil.getBlob(nonExistentPath));
      // For V2, we are returning AccessDeniedException (a subclass of IOException) for forbidden
      // paths.
      assertThrows(AccessDeniedException.class, () -> gcsUtil.getBlob(forbiddenPath));
    } else {
      assertThrows(FileNotFoundException.class, () -> gcsUtil.getObject(nonExistentPath));
      assertThrows(IOException.class, () -> gcsUtil.getObject(forbiddenPath));
    }
  }

  @Test
  public void testGetObjectsOrGetBlobs() throws IOException {
    final GcsPath existingPath =
        GcsPath.fromUri("gs://apache-beam-samples/shakespeare/kinglear.txt");
    final GcsPath nonExistentPath =
        GcsPath.fromUri("gs://my-random-test-bucket-12345/unknown-12345.txt");
    final List<GcsPath> paths = Arrays.asList(existingPath, nonExistentPath);

    if (experiment.equals("use_gcsutil_v2")) {
      List<GcsUtilV2.BlobResult> results = gcsUtil.getBlobs(paths);
      assertEquals(2, results.size());
      assertTrue(results.get(0).blob() != null);
      assertTrue(results.get(0).ioException() == null);
      assertTrue(results.get(1).blob() == null);
      assertTrue(results.get(1).ioException() != null);
    } else {
      List<GcsUtil.StorageObjectOrIOException> results = gcsUtil.getObjects(paths);
      assertEquals(2, results.size());
      assertTrue(results.get(0).storageObject() != null);
      assertTrue(results.get(0).ioException() == null);
      assertTrue(results.get(1).storageObject() == null);
      assertTrue(results.get(1).ioException() != null);
    }
  }

  @Test
  public void testListObjectsOrListBlobs() throws IOException {
    final String bucket = "apache-beam-samples";
    final String prefix = "shakespeare/kingrichard";

    List<String> names;
    if (experiment.equals("use_gcsutil_v2")) {
      Page<Blob> blobs = gcsUtil.listBlobs(bucket, prefix, null);
      names = blobs.streamAll().map(blob -> blob.getName()).collect(Collectors.toList());
    } else {
      Objects objs = gcsUtil.listObjects(bucket, prefix, null);
      names = objs.getItems().stream().map(obj -> obj.getName()).collect(Collectors.toList());
    }
    assertEquals(
        Arrays.asList("shakespeare/kingrichardii.txt", "shakespeare/kingrichardiii.txt"), names);

    final String randomPrefix = "my-random-prefix/random";
    if (experiment.equals("use_gcsutil_v2")) {
      Page<Blob> blobs = gcsUtil.listBlobs(bucket, randomPrefix, null);
      assertEquals(0, blobs.streamAll().count());
    } else {
      Objects objs = gcsUtil.listObjects(bucket, randomPrefix, null);
      assertEquals(null, objs.getItems());
    }
  }

  @Test
  public void testExpand() throws IOException {
    final GcsPath existingPattern =
        GcsPath.fromUri("gs://apache-beam-samples/shakespeare/kingrichardii*.txt");
    List<GcsPath> paths = gcsUtil.expand(existingPattern);

    assertEquals(
        Arrays.asList(
            GcsPath.fromUri("gs://apache-beam-samples/shakespeare/kingrichardii.txt"),
            GcsPath.fromUri("gs://apache-beam-samples/shakespeare/kingrichardiii.txt")),
        paths);

    final GcsPath nonExistentPattern1 =
        GcsPath.fromUri("gs://apache-beam-samples/my_random_folder/random*.txt");
    assertTrue(gcsUtil.expand(nonExistentPattern1).isEmpty());

    final GcsPath nonExistentPattern2 =
        GcsPath.fromUri("gs://apache-beam-samples/shakespeare/king*.csv");
    assertTrue(gcsUtil.expand(nonExistentPattern2).isEmpty());
  }

  @Test
  public void testGetBucketOrGetBucketWithOptions() throws IOException {
    final GcsPath existingPath = GcsPath.fromUri("gs://apache-beam-samples");

    String bucket;
    if (experiment.equals("use_gcsutil_v2")) {
      bucket = gcsUtil.getBucketWithOptions(existingPath).getName();
    } else {
      bucket = gcsUtil.getBucket(existingPath).getName();
    }
    assertEquals("apache-beam-samples", bucket);

    final GcsPath nonExistentPath = GcsPath.fromUri("gs://my-random-test-bucket-12345");
    final GcsPath forbiddenPath = GcsPath.fromUri("gs://test-bucket");

    if (experiment.equals("use_gcsutil_v2")) {
      assertThrows(
          FileNotFoundException.class, () -> gcsUtil.getBucketWithOptions(nonExistentPath));
      assertThrows(AccessDeniedException.class, () -> gcsUtil.getBucketWithOptions(forbiddenPath));
    } else {
      assertThrows(FileNotFoundException.class, () -> gcsUtil.getBucket(nonExistentPath));
      assertThrows(AccessDeniedException.class, () -> gcsUtil.getBucket(forbiddenPath));
    }
  }

  @Test
  public void testBucketAccessible() throws IOException {
    final GcsPath existingPath = GcsPath.fromUri("gs://apache-beam-samples");
    final GcsPath nonExistentPath = GcsPath.fromUri("gs://my-random-test-bucket-12345");
    final GcsPath forbiddenPath = GcsPath.fromUri("gs://test-bucket");

    assertEquals(true, gcsUtil.bucketAccessible(existingPath));
    assertEquals(false, gcsUtil.bucketAccessible(nonExistentPath));
    assertEquals(false, gcsUtil.bucketAccessible(forbiddenPath));
  }

  @Test
  public void testBucketOwner() throws IOException {
    final GcsPath existingPath = GcsPath.fromUri("gs://apache-beam-samples");
    final long expectedProjectNumber = 844138762903L; // apache-beam-testing
    assertEquals(expectedProjectNumber, gcsUtil.bucketOwner(existingPath));

    final GcsPath nonExistentPath = GcsPath.fromUri("gs://my-random-test-bucket-12345");
    final GcsPath forbiddenPath = GcsPath.fromUri("gs://test-bucket");
    assertThrows(FileNotFoundException.class, () -> gcsUtil.bucketOwner(nonExistentPath));
    assertThrows(AccessDeniedException.class, () -> gcsUtil.bucketOwner(forbiddenPath));
  }

  @Test
  public void testCreateAndRemoveBucket() throws IOException {
    final GcsPath gcsPath = GcsPath.fromUri("gs://apache-beam-test-bucket-12345");

    if (experiment.equals("use_gcsutil_v2")) {
      BucketInfo bucketInfo = BucketInfo.of(gcsPath.getBucket());
      try {
        assertFalse(gcsUtil.bucketAccessible(gcsPath));
        gcsUtil.createBucket(bucketInfo);
        assertTrue(gcsUtil.bucketAccessible(gcsPath));

        // raise exception when the bucket already exists during creation
        assertThrows(FileAlreadyExistsException.class, () -> gcsUtil.createBucket(bucketInfo));

        assertTrue(gcsUtil.bucketAccessible(gcsPath));
        gcsUtil.removeBucket(bucketInfo);
        assertFalse(gcsUtil.bucketAccessible(gcsPath));

        // raise exception when the bucket does not exist during removal
        assertThrows(FileNotFoundException.class, () -> gcsUtil.removeBucket(bucketInfo));
      } finally {
        // clean up and ignore errors no matter what
        try {
          gcsUtil.removeBucket(bucketInfo);
        } catch (IOException e) {
        }
      }
    } else {
      Bucket bucket = new Bucket().setName(gcsPath.getBucket());
      GcsOptions gcsOptions = options.as(GcsOptions.class);
      String projectId = gcsOptions.getProject();
      try {
        assertFalse(gcsUtil.bucketAccessible(gcsPath));
        gcsUtil.createBucket(projectId, bucket);
        assertTrue(gcsUtil.bucketAccessible(gcsPath));

        // raise exception when the bucket already exists during creation
        assertThrows(
            FileAlreadyExistsException.class, () -> gcsUtil.createBucket(projectId, bucket));

        assertTrue(gcsUtil.bucketAccessible(gcsPath));
        gcsUtil.removeBucket(bucket);
        assertFalse(gcsUtil.bucketAccessible(gcsPath));

        // raise exception when the bucket does not exist during removal
        assertThrows(FileNotFoundException.class, () -> gcsUtil.removeBucket(bucket));
      } finally {
        // clean up and ignore errors no matter what
        try {
          gcsUtil.removeBucket(bucket);
        } catch (IOException e) {
        }
      }
    }
  }

  @Test
  public void testCopyAndRemove() throws IOException {
    String existingBucket = "apache-beam-temp-bucket-12345";
    List<GcsPath> srcPaths =
        Arrays.asList(
            GcsPath.fromUri("gs://apache-beam-samples/shakespeare/kingrichardii.txt"),
            GcsPath.fromUri("gs://apache-beam-samples/shakespeare/kingrichardiii.txt"));

    // create a bucket first
    try {
      createBucketHelper(existingBucket);
      if (experiment.equals("use_gcsutil_v2")) {
        testCopyAndRemoveV2(existingBucket, srcPaths);
      } else {
        testCopyAndRemoveV1(existingBucket, srcPaths);
      }
    } catch (IOException e) {
      throw e;
    } finally {
      tearDownBucketHelper(existingBucket);
    }
  }

  private void testCopyAndRemoveV2(String existingBucket, List<GcsPath> srcPaths)
      throws IOException {
    String nonExistentBucket = "my-random-test-bucket-12345";
    List<GcsPath> dstPaths =
        srcPaths.stream()
            .map(o -> GcsPath.fromComponents(existingBucket, o.getObject()))
            .collect(Collectors.toList());
    gcsUtil.copyV2(srcPaths, dstPaths);
    assertExists(dstPaths.get(0));
    assertExists(dstPaths.get(1));

    // copy from existing files to an existing bucket
    // No exception on SAFE_OVERWRITE and ALWAYS_OVERWRITE
    gcsUtil.copyV2(srcPaths, dstPaths);
    gcsUtil.copy(srcPaths, dstPaths, OverwriteStrategy.ALWAYS_OVERWRITE);

    // raise exception on FAIL_IF_EXISTS
    assertThrows(
        FileAlreadyExistsException.class,
        () -> gcsUtil.copy(srcPaths, dstPaths, OverwriteStrategy.FAIL_IF_EXISTS));

    // remove the existing files
    gcsUtil.removeV2(dstPaths);

    // remove the already-deleted files
    // No exception on IGNORE_IF_MISSING
    gcsUtil.removeV2(dstPaths);

    // raise exception on FAIL_IF_MISSING
    assertThrows(
        FileNotFoundException.class,
        () -> gcsUtil.remove(dstPaths, MissingStrategy.FAIL_IF_MISSING));

    final List<GcsPath> wrongDstPaths =
        srcPaths.stream()
            .map(o -> GcsPath.fromComponents(nonExistentBucket, o.getObject()))
            .collect(Collectors.toList());

    // copy from existing files to an nonexistent bucket. Raise exception.
    assertThrows(FileNotFoundException.class, () -> gcsUtil.copyV2(srcPaths, wrongDstPaths));

    // remove files from an nonexistent bucket. No exception.
    gcsUtil.removeV2(wrongDstPaths);

    final List<GcsPath> wrongSrcPaths =
        Arrays.asList(GcsPath.fromUri("gs://apache-beam-samples/shakespeare/some-random-name.txt"));

    // missing source file. Raise exception.
    assertThrows(
        FileNotFoundException.class,
        () -> gcsUtil.copy(wrongSrcPaths, wrongSrcPaths, OverwriteStrategy.SAFE_OVERWRITE));
  }

  private void testCopyAndRemoveV1(String existingBucket, List<GcsPath> srcPaths)
      throws IOException {
    String nonExistentBucket = "my-random-test-bucket-12345";
    List<String> srcList = srcPaths.stream().map(o -> o.toString()).collect(Collectors.toList());

    // copy from existing files to an existing bucket
    List<String> dstList =
        srcPaths.stream()
            .map(o -> String.format("gs://%s/%s", existingBucket, o.getObject()))
            .collect(Collectors.toList());
    gcsUtil.copy(srcList, dstList);

    assertExists(GcsPath.fromUri(dstList.get(0)));
    assertExists(GcsPath.fromUri(dstList.get(1)));

    // copy from existing files to an existing bucket, but target files exist. No exception.
    gcsUtil.copy(srcList, dstList);

    // remove the existing files
    gcsUtil.remove(dstList);

    // remove the already-deleted files. No exception.
    gcsUtil.remove(dstList);

    final List<String> wrongDstList =
        srcPaths.stream()
            .map(o -> String.format("gs://%s/%s", nonExistentBucket, o.getObject()))
            .collect(Collectors.toList());

    // copy from existing files to an nonexistent bucket. Raise exception.
    assertThrows(FileNotFoundException.class, () -> gcsUtil.copy(srcList, wrongDstList));

    // remove files from an nonexistent bucket. No exception.
    gcsUtil.remove(dstList);

    final List<String> wrongSrcList =
        Arrays.asList("gs://apache-beam-samples/shakespeare/some-random-name.txt");

    // missing source file, Raise exception
    assertThrows(FileNotFoundException.class, () -> gcsUtil.copy(wrongSrcList, wrongSrcList));
  }

  private void createBucketHelper(String bucketName) throws IOException {
    if (experiment.equals("use_gcsutil_v2")) {
      gcsUtil.createBucket(BucketInfo.of(bucketName));
    } else {
      GcsOptions gcsOptions = options.as(GcsOptions.class);
      gcsUtil.createBucket(gcsOptions.getProject(), new Bucket().setName(bucketName));
    }
  }

  private void tearDownBucketHelper(String bucketName) {
    try {
      // Use "**" in the pattern to match any characters including "/".
      List<GcsPath> paths =
          gcsUtil.expand(GcsPath.fromUri(String.format("gs://%s/**", bucketName)));
      if (experiment.equals("use_gcsutil_v2")) {
        gcsUtil.remove(paths, MissingStrategy.SKIP_IF_MISSING);
        gcsUtil.removeBucket(BucketInfo.of(bucketName));
      } else {
        gcsUtil.remove(paths.stream().map(GcsPath::toString).collect(Collectors.toList()));
        gcsUtil.removeBucket(new Bucket().setName(bucketName));
      }
    } catch (IOException e) {
    }
  }

  private void assertExists(GcsPath path) throws IOException {
    if (experiment.equals("use_gcsutil_v2")) {
      gcsUtil.getBlob(path);
    } else {
      gcsUtil.getObject(path);
    }
  }
}
