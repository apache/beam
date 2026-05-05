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
import org.apache.beam.sdk.io.fs.MoveOptions;
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

  private List<GcsPath> createTestBucketHelper(String bucketName) throws IOException {
    final List<GcsPath> originPaths =
        Arrays.asList(
            GcsPath.fromUri("gs://apache-beam-samples/shakespeare/kingrichardii.txt"),
            GcsPath.fromUri("gs://apache-beam-samples/shakespeare/kingrichardiii.txt"));

    final List<GcsPath> testPaths =
        originPaths.stream()
            .map(o -> GcsPath.fromComponents(bucketName, o.getObject()))
            .collect(Collectors.toList());

    // create bucket and copy some initial files into there
    if (experiment.equals("use_gcsutil_v2")) {
      gcsUtil.createBucket(BucketInfo.of(bucketName));

      gcsUtil.copyV2(originPaths, testPaths);
    } else {
      GcsOptions gcsOptions = options.as(GcsOptions.class);
      gcsUtil.createBucket(gcsOptions.getProject(), new Bucket().setName(bucketName));

      final List<String> originList =
          originPaths.stream().map(o -> o.toString()).collect(Collectors.toList());
      final List<String> testList =
          testPaths.stream().map(o -> o.toString()).collect(Collectors.toList());
      gcsUtil.copy(originList, testList);
    }

    return testPaths;
  }

  private void tearDownTestBucketHelper(String bucketName) {
    try {
      // use "**" in the pattern to match any characters including "/".
      final List<GcsPath> paths =
          gcsUtil.expand(GcsPath.fromUri(String.format("gs://%s/**", bucketName)));
      if (experiment.equals("use_gcsutil_v2")) {
        gcsUtil.remove(paths, MissingStrategy.SKIP_IF_MISSING);
        gcsUtil.removeBucket(BucketInfo.of(bucketName));
      } else {
        gcsUtil.remove(paths.stream().map(GcsPath::toString).collect(Collectors.toList()));
        gcsUtil.removeBucket(new Bucket().setName(bucketName));
      }
    } catch (IOException e) {
      System.err.println(
          "Error during tear down of test bucket " + bucketName + ": " + e.getMessage());
    }
  }

  @Test
  public void testCopy() throws IOException {
    final String existingBucket = "apache-beam-temp-bucket-12345";
    final String nonExistentBucket = "my-random-test-bucket-12345";

    try {
      final List<GcsPath> srcPaths = createTestBucketHelper(existingBucket);
      final List<GcsPath> dstPaths =
          srcPaths.stream()
              .map(o -> GcsPath.fromComponents(existingBucket, o.getObject() + ".bak"))
              .collect(Collectors.toList());
      final List<GcsPath> errPaths =
          srcPaths.stream()
              .map(o -> GcsPath.fromComponents(nonExistentBucket, o.getObject()))
              .collect(Collectors.toList());

      assertNotExists(dstPaths.get(0));
      assertNotExists(dstPaths.get(1));

      if (experiment.equals("use_gcsutil_v2")) {
        // (1) when the target files do not exist
        gcsUtil.copyV2(srcPaths, dstPaths);
        assertExists(dstPaths.get(0));
        assertExists(dstPaths.get(1));

        // (2) when the target files exist
        // (2a) no exception on SAFE_OVERWRITE, ALWAYS_OVERWRITE, SKIP_IF_EXISTS
        gcsUtil.copyV2(srcPaths, dstPaths);
        gcsUtil.copy(srcPaths, dstPaths, OverwriteStrategy.ALWAYS_OVERWRITE);
        gcsUtil.copy(srcPaths, dstPaths, OverwriteStrategy.SKIP_IF_EXISTS);

        // (2b) raise exception on FAIL_IF_EXISTS
        assertThrows(
            FileAlreadyExistsException.class,
            () -> gcsUtil.copy(srcPaths, dstPaths, OverwriteStrategy.FAIL_IF_EXISTS));

        // (3) raise exception when the target bucket is nonexistent.
        assertThrows(FileNotFoundException.class, () -> gcsUtil.copyV2(srcPaths, errPaths));

        // (4) raise exception when the source files are nonexistent.
        assertThrows(FileNotFoundException.class, () -> gcsUtil.copyV2(errPaths, dstPaths));
      } else {
        final List<String> srcList =
            srcPaths.stream().map(o -> o.toString()).collect(Collectors.toList());
        final List<String> dstList =
            dstPaths.stream().map(o -> o.toString()).collect(Collectors.toList());
        final List<String> errList =
            errPaths.stream().map(o -> o.toString()).collect(Collectors.toList());

        // (1) when the target files do not exist
        gcsUtil.copy(srcList, dstList);
        assertExists(dstPaths.get(0));
        assertExists(dstPaths.get(1));

        // (2) when the target files exist, no exception
        gcsUtil.copy(srcList, dstList);

        // (3) raise exception when the target bucket is nonexistent.
        assertThrows(FileNotFoundException.class, () -> gcsUtil.copy(srcList, errList));

        // (4) raise exception when the source files are nonexistent.
        assertThrows(FileNotFoundException.class, () -> gcsUtil.copy(errList, dstList));
      }
    } finally {
      tearDownTestBucketHelper(existingBucket);
    }
  }

  @Test
  public void testRemove() throws IOException {
    final String existingBucket = "apache-beam-temp-bucket-12345";
    final String nonExistentBucket = "my-random-test-bucket-12345";

    try {
      final List<GcsPath> srcPaths = createTestBucketHelper(existingBucket);
      final List<GcsPath> errPaths =
          srcPaths.stream()
              .map(o -> GcsPath.fromComponents(nonExistentBucket, o.getObject()))
              .collect(Collectors.toList());

      assertExists(srcPaths.get(0));
      assertExists(srcPaths.get(1));

      if (experiment.equals("use_gcsutil_v2")) {
        // (1) when the files to remove exist
        gcsUtil.removeV2(srcPaths);
        assertNotExists(srcPaths.get(0));
        assertNotExists(srcPaths.get(1));

        // (2) when the files to remove have been deleted
        // (2a) no exception on SKIP_IF_MISSING
        gcsUtil.removeV2(srcPaths);
        gcsUtil.remove(srcPaths, MissingStrategy.SKIP_IF_MISSING);

        // (2b) raise exception on FAIL_IF_MISSING
        assertThrows(
            FileNotFoundException.class,
            () -> gcsUtil.remove(srcPaths, MissingStrategy.FAIL_IF_MISSING));

        // (3) when the files are from an nonexistent bucket
        // (3a) no exception on SKIP_IF_MISSING
        gcsUtil.removeV2(errPaths);
        gcsUtil.remove(errPaths, MissingStrategy.SKIP_IF_MISSING);

        // (3b) raise exception on FAIL_IF_MISSING
        assertThrows(
            FileNotFoundException.class,
            () -> gcsUtil.remove(errPaths, MissingStrategy.FAIL_IF_MISSING));
      } else {
        final List<String> srcList =
            srcPaths.stream().map(o -> o.toString()).collect(Collectors.toList());
        final List<String> errList =
            errPaths.stream().map(o -> o.toString()).collect(Collectors.toList());

        // (1) when the files to remove exist
        gcsUtil.remove(srcList);
        assertNotExists(srcPaths.get(0));
        assertNotExists(srcPaths.get(1));

        // (2) when the files to remove have been deleted, no exception
        gcsUtil.remove(srcList);

        // (3) when the files are from an nonexistent bucket, no exception
        gcsUtil.remove(errList);
      }
    } finally {
      tearDownTestBucketHelper(existingBucket);
    }
  }

  @Test
  public void testRename() throws IOException {
    final String existingBucket = "apache-beam-temp-bucket-12345";
    final String nonExistentBucket = "my-random-test-bucket-12345";

    try {
      final List<GcsPath> srcPaths = createTestBucketHelper(existingBucket);
      final List<GcsPath> tmpPaths =
          srcPaths.stream()
              .map(o -> GcsPath.fromComponents(existingBucket, "tmp/" + o.getObject()))
              .collect(Collectors.toList());
      final List<GcsPath> dstPaths =
          srcPaths.stream()
              .map(o -> GcsPath.fromComponents(existingBucket, o.getObject() + ".bak"))
              .collect(Collectors.toList());
      final List<GcsPath> errPaths =
          srcPaths.stream()
              .map(o -> GcsPath.fromComponents(nonExistentBucket, o.getObject()))
              .collect(Collectors.toList());

      assertNotExists(dstPaths.get(0));
      assertNotExists(dstPaths.get(1));
      if (experiment.equals("use_gcsutil_v2")) {
        // Make a copy of sources
        gcsUtil.copyV2(srcPaths, tmpPaths);

        // (1) when the source files exist and target files do not
        gcsUtil.renameV2(tmpPaths, dstPaths);
        assertNotExists(tmpPaths.get(0));
        assertNotExists(tmpPaths.get(1));
        assertExists(dstPaths.get(0));
        assertExists(dstPaths.get(1));

        // (2) when the source files do not exist
        // (2a) no exception if IGNORE_MISSING_FILES is set
        gcsUtil.renameV2(errPaths, dstPaths, MoveOptions.StandardMoveOptions.IGNORE_MISSING_FILES);

        // (2b) raise exception if if IGNORE_MISSING_FILES is not set
        assertThrows(FileNotFoundException.class, () -> gcsUtil.renameV2(errPaths, dstPaths));

        // (3) when both source files and target files exist
        gcsUtil.renameV2(
            srcPaths, dstPaths, MoveOptions.StandardMoveOptions.SKIP_IF_DESTINATION_EXISTS);
        gcsUtil.renameV2(srcPaths, dstPaths);
      } else {
        final List<String> srcList =
            srcPaths.stream().map(o -> o.toString()).collect(Collectors.toList());
        final List<String> tmpList =
            tmpPaths.stream().map(o -> o.toString()).collect(Collectors.toList());
        final List<String> dstList =
            dstPaths.stream().map(o -> o.toString()).collect(Collectors.toList());
        final List<String> errList =
            errPaths.stream().map(o -> o.toString()).collect(Collectors.toList());

        // Make a copy of sources
        gcsUtil.copy(srcList, tmpList);

        // (1) when the source files exist and target files do not
        gcsUtil.rename(tmpList, dstList);
        assertNotExists(tmpPaths.get(0));
        assertNotExists(tmpPaths.get(1));
        assertExists(dstPaths.get(0));
        assertExists(dstPaths.get(1));

        // (2) when the source files do not exist
        // (2a) no exception if IGNORE_MISSING_FILES is set
        gcsUtil.rename(errList, dstList, MoveOptions.StandardMoveOptions.IGNORE_MISSING_FILES);

        // (2b) raise exception if if IGNORE_MISSING_FILES is not set
        assertThrows(FileNotFoundException.class, () -> gcsUtil.rename(errList, dstList));

        // (3) when both source files and target files exist
        assertExists(srcPaths.get(0));
        assertExists(srcPaths.get(1));
        assertExists(dstPaths.get(0));
        assertExists(dstPaths.get(1));

        // There is a bug in V1 where SKIP_IF_DESTINATION_EXISTS is not honored.
        gcsUtil.rename(
            srcList, dstList, MoveOptions.StandardMoveOptions.SKIP_IF_DESTINATION_EXISTS);

        assertNotExists(srcPaths.get(0)); // BUG! The renaming is supposed to be skipped
        assertNotExists(srcPaths.get(1)); // BUG! The renaming is supposed to be skipped
        // assertExists(srcPaths.get(0));
        // assertExists(srcPaths.get(1));
        assertExists(dstPaths.get(0));
        assertExists(dstPaths.get(1));
      }
    } finally {
      tearDownTestBucketHelper(existingBucket);
    }
  }

  private void assertExists(GcsPath path) throws IOException {
    if (experiment.equals("use_gcsutil_v2")) {
      gcsUtil.getBlob(path);
    } else {
      gcsUtil.getObject(path);
    }
  }

  private void assertNotExists(GcsPath path) throws IOException {
    if (experiment.equals("use_gcsutil_v2")) {
      assertThrows(FileNotFoundException.class, () -> gcsUtil.getBlob(path));
    } else {
      assertThrows(FileNotFoundException.class, () -> gcsUtil.getObject(path));
    }
  }
}
