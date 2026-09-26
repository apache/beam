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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import com.google.api.gax.paging.Page;
import com.google.api.services.storage.model.Bucket;
import com.google.api.services.storage.model.Objects;
import com.google.api.services.storage.model.StorageObject;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.StorageChannelUtils;
import com.google.protobuf.ByteString;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileAlreadyExistsException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.beam.runners.core.metrics.CounterCell;
import org.apache.beam.runners.core.metrics.GcpResourceIdentifiers;
import org.apache.beam.runners.core.metrics.MetricUpdates.MetricUpdate;
import org.apache.beam.runners.core.metrics.MetricsContainerImpl;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants;
import org.apache.beam.runners.core.metrics.MonitoringInfoMetricName;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.extensions.gcp.util.GcsUtil.CreateOptions;
import org.apache.beam.sdk.extensions.gcp.util.GcsUtilV2.MissingStrategy;
import org.apache.beam.sdk.extensions.gcp.util.GcsUtilV2.OverwriteStrategy;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MoveOptions;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.testing.UsesKms;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
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
 *
 * <p>Each test runs once with {@link GcsUtilV1} and once with {@link GcsUtilV2}:
 *
 * <ul>
 *   <li>Tests of the legacy API and the metrics run in both modes with the same assertions, which
 *       checks parity. Documented divergences are asserted per mode.
 *   <li>Tests of the V2-native API ({@code getBlob}, {@code copyV2}, ...) only run with V2.
 *   <li>Tests of V1-only features only run with V1.
 * </ul>
 */
@RunWith(Parameterized.class)
@Category(UsesKms.class)
public class GcsUtilIT {

  private static final String READ_COUNTER_PREFIX = "it_read_bytes";
  private static final String WRITE_COUNTER_PREFIX = "it_write_bytes";

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

  /** Returns a bucket name unique to this test run, so concurrent runs don't collide. */
  private static String randomBucketName() {
    return "apache-beam-temp-bucket-" + UUID.randomUUID();
  }

  @Test
  public void testFileSize() throws IOException {
    final GcsPath gcsPath = GcsPath.fromUri("gs://apache-beam-samples/shakespeare/kinglear.txt");
    final long expectedSize = 157283L;

    assertEquals(expectedSize, gcsUtil.fileSize(gcsPath));
  }

  @Test
  public void testGetBlob() throws IOException {
    assumeTrue(isV2());

    final GcsPath existingPath =
        GcsPath.fromUri("gs://apache-beam-samples/shakespeare/kinglear.txt");
    final String expectedCRC = "s0a3Tg==";

    Blob blob = gcsUtil.getBlob(existingPath);
    assertEquals(expectedCRC, blob.getCrc32c());

    final GcsPath nonExistentPath =
        GcsPath.fromUri("gs://my-random-test-bucket-12345/unknown-12345.txt");
    final GcsPath forbiddenPath = GcsPath.fromUri("gs://test-bucket/unknown-12345.txt");

    assertThrows(FileNotFoundException.class, () -> gcsUtil.getBlob(nonExistentPath));
    // For V2, we are returning AccessDeniedException (a subclass of IOException) for forbidden
    // paths.
    assertThrows(AccessDeniedException.class, () -> gcsUtil.getBlob(forbiddenPath));
  }

  @Test
  public void testGetBlobs() throws IOException {
    assumeTrue(isV2());

    final GcsPath existingPath =
        GcsPath.fromUri("gs://apache-beam-samples/shakespeare/kinglear.txt");
    final GcsPath nonExistentPath =
        GcsPath.fromUri("gs://my-random-test-bucket-12345/unknown-12345.txt");
    final List<GcsPath> paths = Arrays.asList(existingPath, nonExistentPath);

    List<GcsUtilV2.BlobResult> results = gcsUtil.getBlobs(paths);
    assertEquals(2, results.size());
    assertTrue(results.get(0).blob() != null);
    assertTrue(results.get(0).ioException() == null);
    assertTrue(results.get(1).blob() == null);
    assertTrue(results.get(1).ioException() != null);
  }

  @Test
  public void testListBlobs() throws IOException {
    assumeTrue(isV2());

    final String bucket = "apache-beam-samples";
    final String prefix = "shakespeare/kingrichard";

    Page<Blob> blobs = gcsUtil.listBlobs(bucket, prefix, null);
    List<String> names = blobs.streamAll().map(blob -> blob.getName()).collect(Collectors.toList());
    assertEquals(
        Arrays.asList("shakespeare/kingrichardii.txt", "shakespeare/kingrichardiii.txt"), names);

    final String randomPrefix = "my-random-prefix/random";
    Page<Blob> emptyBlobs = gcsUtil.listBlobs(bucket, randomPrefix, null);
    assertEquals(0, emptyBlobs.streamAll().count());
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
  public void testGetBucketWithOptions() throws IOException {
    assumeTrue(isV2());

    final GcsPath existingPath = GcsPath.fromUri("gs://apache-beam-samples");

    String bucket = gcsUtil.getBucketWithOptions(existingPath).getName();
    assertEquals("apache-beam-samples", bucket);

    final GcsPath nonExistentPath = GcsPath.fromUri("gs://my-random-test-bucket-12345");
    final GcsPath forbiddenPath = GcsPath.fromUri("gs://test-bucket");

    assertThrows(FileNotFoundException.class, () -> gcsUtil.getBucketWithOptions(nonExistentPath));
    assertThrows(AccessDeniedException.class, () -> gcsUtil.getBucketWithOptions(forbiddenPath));
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
  public void testCreateAndRemoveBucketWithBucketInfo() throws IOException {
    assumeTrue(isV2());

    final GcsPath gcsPath = GcsPath.fromUri("gs://" + randomBucketName());

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
  }

  private List<GcsPath> createTestBucketHelper(String bucketName, boolean copyData)
      throws IOException {
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

      if (copyData) {
        gcsUtil.copyV2(originPaths, testPaths);
      } else {
        return Collections.emptyList();
      }
    } else {
      GcsOptions gcsOptions = options.as(GcsOptions.class);
      gcsUtil.createBucket(gcsOptions.getProject(), new Bucket().setName(bucketName));

      if (copyData) {
        final List<String> originList =
            originPaths.stream().map(o -> o.toString()).collect(Collectors.toList());
        final List<String> testList =
            testPaths.stream().map(o -> o.toString()).collect(Collectors.toList());
        gcsUtil.copy(originList, testList);
      } else {
        return Collections.emptyList();
      }
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
  public void testCopyWithOverwriteStrategies() throws IOException {
    assumeTrue(isV2());

    final String existingBucket = randomBucketName();
    final String nonExistentBucket = "my-random-test-bucket-12345";

    try {
      final List<GcsPath> srcPaths = createTestBucketHelper(existingBucket, true);
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
    } finally {
      tearDownTestBucketHelper(existingBucket);
    }
  }

  @Test
  public void testRemoveWithMissingStrategies() throws IOException {
    assumeTrue(isV2());

    final String existingBucket = randomBucketName();
    final String nonExistentBucket = "my-random-test-bucket-12345";

    try {
      final List<GcsPath> srcPaths = createTestBucketHelper(existingBucket, true);
      final List<GcsPath> errPaths =
          srcPaths.stream()
              .map(o -> GcsPath.fromComponents(nonExistentBucket, o.getObject()))
              .collect(Collectors.toList());

      assertExists(srcPaths.get(0));
      assertExists(srcPaths.get(1));

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
    } finally {
      tearDownTestBucketHelper(existingBucket);
    }
  }

  @Test
  public void testRenameV2() throws IOException {
    assumeTrue(isV2());

    final String existingBucket = randomBucketName();
    final String nonExistentBucket = "my-random-test-bucket-12345";

    try {
      final List<GcsPath> srcPaths = createTestBucketHelper(existingBucket, true);
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

  String computeHash(ByteBuffer buffer) throws NoSuchAlgorithmException {
    MessageDigest digest = MessageDigest.getInstance("SHA-256");
    digest.update(buffer);
    byte[] hashBytes = digest.digest();

    // Convert bytes to Hex String
    StringBuilder sb = new StringBuilder();
    for (byte b : hashBytes) {
      sb.append(String.format("%02x", b));
    }
    return sb.toString();
  }

  @Test
  public void testRead() throws IOException, NoSuchAlgorithmException {
    final GcsPath gcsPath = GcsPath.fromUri("gs://apache-beam-samples/shakespeare/kinglear.txt");
    final String expectedHash = "674a2725884307c96398440497c889ad8cecccedf5689df85e6b0faabe4e0fe8";
    final long expectedSize = 157283L;

    try (SeekableByteChannel channel = gcsUtil.open(gcsPath)) {
      // Verify Size
      assertEquals(expectedSize, channel.size());
      assertEquals(0, channel.position());

      // Read content into ByteBuffer.
      // Allocate a larger buffer to ensure we receive the EOF at the expected place.
      ByteBuffer buffer = ByteBuffer.allocate((int) expectedSize + 1024);
      int bytesRead = StorageChannelUtils.blockingFillFrom(buffer, channel);

      // Verify total bytes read and position
      assertEquals(expectedSize, bytesRead);
      assertEquals(expectedSize, channel.position());

      // Flip the buffer to prepare it for reading (sets limit to current position, position to 0)
      buffer.flip();

      // Verify hash
      String actualHash = computeHash(buffer);
      assertEquals("Content hash should match", expectedHash, actualHash);
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Legacy API parity: the same legacy calls and assertions must hold for both GcsUtilV1 and
  // GcsUtilV2. Documented divergences are asserted per mode.
  // ---------------------------------------------------------------------------------------------

  private static final String KINGLEAR = "gs://apache-beam-samples/shakespeare/kinglear.txt";
  private static final String NONEXISTENT_BUCKET = "my-random-test-bucket-12345";
  private static final String FORBIDDEN_BUCKET = "test-bucket";

  private boolean isV2() {
    return experiment.equals("use_gcsutil_v2");
  }

  private static List<String> toStrings(List<GcsPath> paths) {
    return paths.stream().map(GcsPath::toString).collect(Collectors.toList());
  }

  private static List<GcsPath> objectPaths(String bucket, String... objects) {
    return Arrays.stream(objects)
        .map(o -> GcsPath.fromComponents(bucket, o))
        .collect(Collectors.toList());
  }

  private static List<String> itemNames(Objects objects) {
    if (objects.getItems() == null) {
      return Collections.emptyList();
    }
    return objects.getItems().stream().map(StorageObject::getName).collect(Collectors.toList());
  }

  private void writeObject(GcsPath path, byte[] content, CreateOptions createOptions)
      throws IOException {
    try (WritableByteChannel writer = gcsUtil.create(path, createOptions)) {
      ByteBuffer buffer = ByteBuffer.wrap(content);
      while (buffer.hasRemaining()) {
        writer.write(buffer);
      }
    }
  }

  private byte[] readObject(GcsPath path) throws IOException {
    ByteArrayOutputStream readContent = new ByteArrayOutputStream();
    try (ReadableByteChannel reader = gcsUtil.open(path)) {
      ByteBuffer buffer = ByteBuffer.allocate(64 * 1024);
      while (reader.read(buffer) != -1) {
        buffer.flip();
        readContent.write(buffer.array(), 0, buffer.limit());
        buffer.clear();
      }
    }
    return readContent.toByteArray();
  }

  @Test
  public void testLegacyGetObject() throws IOException {
    StorageObject obj = gcsUtil.getObject(GcsPath.fromUri(KINGLEAR));
    assertEquals("apache-beam-samples", obj.getBucket());
    assertEquals("shakespeare/kinglear.txt", obj.getName());
    assertEquals(BigInteger.valueOf(157283L), obj.getSize());
    assertEquals("s0a3Tg==", obj.getCrc32c());
    assertNotNull(obj.getMd5Hash());
    assertNotNull(obj.getGeneration());
    assertNotNull(obj.getUpdated());

    assertThrows(
        FileNotFoundException.class,
        () -> gcsUtil.getObject(GcsPath.fromComponents("apache-beam-samples", "unknown-12345")));
    assertThrows(
        FileNotFoundException.class,
        () -> gcsUtil.getObject(GcsPath.fromComponents(NONEXISTENT_BUCKET, "unknown-12345")));

    IOException forbidden =
        assertThrows(
            IOException.class,
            () -> gcsUtil.getObject(GcsPath.fromComponents(FORBIDDEN_BUCKET, "unknown-12345")));
    assertFalse(forbidden instanceof FileNotFoundException);
    if (isV2()) {
      // G4: V2 reports it as an AccessDeniedException, a subclass of IOException.
      assertTrue(forbidden instanceof AccessDeniedException);
    }
  }

  @Test
  public void testLegacyGetObjects() throws IOException {
    List<GcsPath> paths =
        Arrays.asList(
            GcsPath.fromUri(KINGLEAR),
            GcsPath.fromComponents("apache-beam-samples", "unknown-12345"),
            GcsPath.fromComponents(NONEXISTENT_BUCKET, "unknown-12345"),
            GcsPath.fromComponents(FORBIDDEN_BUCKET, "unknown-12345"));

    List<GcsUtil.StorageObjectOrIOException> results = gcsUtil.getObjects(paths);

    assertEquals(4, results.size());
    assertNull(results.get(0).ioException());
    assertEquals("s0a3Tg==", results.get(0).storageObject().getCrc32c());
    assertNull(results.get(1).storageObject());
    assertTrue(results.get(1).ioException() instanceof FileNotFoundException);
    assertNull(results.get(2).storageObject());
    assertTrue(results.get(2).ioException() instanceof FileNotFoundException);
    assertNull(results.get(3).storageObject());
    IOException forbidden = results.get(3).ioException();
    assertNotNull(forbidden);
    assertFalse(forbidden instanceof FileNotFoundException);
    if (isV2()) {
      // G4: V2 reports it as an AccessDeniedException, a subclass of IOException.
      assertTrue(forbidden instanceof AccessDeniedException);
    }
  }

  @Test
  public void testLegacyGetObjectsAboveBatchLimit() throws IOException {
    // Both versions send at most 100 requests per batch, so this spans three batches.
    final int count = 250;
    List<GcsPath> paths = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      paths.add(
          i % 2 == 0
              ? GcsPath.fromUri(KINGLEAR)
              : GcsPath.fromComponents("apache-beam-samples", "unknown-" + i));
    }

    List<GcsUtil.StorageObjectOrIOException> results = gcsUtil.getObjects(paths);

    assertEquals(count, results.size());
    for (int i = 0; i < count; i++) {
      if (i % 2 == 0) {
        assertEquals("result " + i, "s0a3Tg==", results.get(i).storageObject().getCrc32c());
      } else {
        assertTrue("result " + i, results.get(i).ioException() instanceof FileNotFoundException);
      }
    }
  }

  @Test
  public void testLegacyListObjectsAndExpandWithDirectories() throws IOException {
    final String bucket = randomBucketName();
    final GcsPath placeholder = GcsPath.fromComponents(bucket, "dir/");
    final byte[] content = "content".getBytes(StandardCharsets.UTF_8);

    try {
      createTestBucketHelper(bucket, false);
      writeObject(placeholder, new byte[0], CreateOptions.builder().build());
      for (GcsPath path : objectPaths(bucket, "dir/a.txt", "dir/b.csv", "dir/sub/c.txt")) {
        writeObject(path, content, CreateOptions.builder().build());
      }

      // A flat listing returns every object, including the directory placeholder.
      Objects flat = gcsUtil.listObjects(bucket, "dir/", null);
      assertEquals(
          Arrays.asList("dir/", "dir/a.txt", "dir/b.csv", "dir/sub/c.txt"), itemNames(flat));
      assertNull(flat.getPrefixes());
      assertNull(flat.getNextPageToken());

      // A delimited listing returns sub-directories as prefixes.
      Objects delimited = gcsUtil.listObjects(bucket, "dir/", null, "/");
      assertEquals(Arrays.asList("dir/", "dir/a.txt", "dir/b.csv"), itemNames(delimited));
      assertEquals(Collections.singletonList("dir/sub/"), delimited.getPrefixes());
      assertNull(delimited.getNextPageToken());

      // An empty listing has null items, which callers use to stop paging.
      Objects empty = gcsUtil.listObjects(bucket, "no-such-prefix/", null);
      assertNull(empty.getItems());
      assertNull(empty.getNextPageToken());

      // Globs skip the directory placeholder.
      assertEquals(
          objectPaths(bucket, "dir/a.txt", "dir/b.csv"),
          gcsUtil.expand(GcsPath.fromComponents(bucket, "dir/*")));
      assertEquals(
          objectPaths(bucket, "dir/a.txt"),
          gcsUtil.expand(GcsPath.fromComponents(bucket, "dir/*.txt")));
      assertEquals(
          objectPaths(bucket, "dir/a.txt", "dir/b.csv", "dir/sub/c.txt"),
          gcsUtil.expand(GcsPath.fromComponents(bucket, "dir/**")));
      assertEquals(
          objectPaths(bucket, "dir/sub/c.txt"),
          gcsUtil.expand(GcsPath.fromComponents(bucket, "dir/*/c.txt")));

      // A path without a wildcard expands to itself if it exists, and to nothing otherwise.
      assertEquals(
          objectPaths(bucket, "dir/a.txt"),
          gcsUtil.expand(GcsPath.fromComponents(bucket, "dir/a.txt")));
      assertTrue(gcsUtil.expand(GcsPath.fromComponents(bucket, "dir/missing.txt")).isEmpty());
    } finally {
      // tearDownTestBucketHelper expands a glob, which skips the placeholder, so remove it here.
      try {
        gcsUtil.remove(Collections.singletonList(placeholder.toString()));
      } catch (IOException e) {
      }
      tearDownTestBucketHelper(bucket);
    }
  }

  @Test
  public void testLegacyGetBucket() throws IOException {
    Bucket bucket = gcsUtil.getBucket(GcsPath.fromUri("gs://apache-beam-samples"));
    assertEquals("apache-beam-samples", bucket.getName());
    assertEquals(BigInteger.valueOf(844138762903L), bucket.getProjectNumber());
    assertNotNull(bucket.getLocation());

    final GcsPath nonExistentPath = GcsPath.fromUri("gs://" + NONEXISTENT_BUCKET);
    final GcsPath forbiddenPath = GcsPath.fromUri("gs://" + FORBIDDEN_BUCKET);
    assertThrows(FileNotFoundException.class, () -> gcsUtil.getBucket(nonExistentPath));
    assertThrows(AccessDeniedException.class, () -> gcsUtil.getBucket(forbiddenPath));
    assertThrows(
        FileNotFoundException.class, () -> gcsUtil.verifyBucketAccessible(nonExistentPath));
    assertThrows(IOException.class, () -> gcsUtil.verifyBucketAccessible(forbiddenPath));
  }

  @Test
  public void testLegacyCreateAndRemoveBucket() throws IOException {
    final String name = randomBucketName();
    final GcsPath path = GcsPath.fromUri("gs://" + name);
    final String projectId = options.as(GcsOptions.class).getProject();
    final Bucket bucket =
        new Bucket()
            .setName(name)
            .setLocation("US-CENTRAL1")
            .setStorageClass("NEARLINE")
            .setSoftDeletePolicy(new Bucket.SoftDeletePolicy().setRetentionDurationSeconds(0L));

    try {
      assertFalse(gcsUtil.bucketAccessible(path));
      gcsUtil.createBucket(projectId, bucket);
      assertTrue(gcsUtil.bucketAccessible(path));

      // The settings passed at creation are persisted.
      Bucket created = gcsUtil.getBucket(path);
      assertEquals(name, created.getName());
      assertEquals("US-CENTRAL1", created.getLocation());
      assertEquals("NEARLINE", created.getStorageClass());
      assertEquals(Long.valueOf(0L), created.getSoftDeletePolicy().getRetentionDurationSeconds());
      assertEquals(created.getProjectNumber().longValue(), gcsUtil.bucketOwner(path));

      // raise exception when the bucket already exists during creation
      assertThrows(FileAlreadyExistsException.class, () -> gcsUtil.createBucket(projectId, bucket));

      gcsUtil.removeBucket(bucket);
      assertFalse(gcsUtil.bucketAccessible(path));

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

  @Test
  public void testLegacyCopyAndRemove() throws IOException {
    final String bucket = randomBucketName();

    try {
      final List<GcsPath> srcPaths = createTestBucketHelper(bucket, true);
      final List<GcsPath> dstPaths =
          srcPaths.stream()
              .map(o -> GcsPath.fromComponents(bucket, o.getObject() + ".bak"))
              .collect(Collectors.toList());
      final List<GcsPath> errPaths =
          srcPaths.stream()
              .map(o -> GcsPath.fromComponents(NONEXISTENT_BUCKET, o.getObject()))
              .collect(Collectors.toList());
      final List<String> srcList = toStrings(srcPaths);
      final List<String> dstList = toStrings(dstPaths);
      final List<String> errList = toStrings(errPaths);
      final String srcMd5 = gcsUtil.getObject(srcPaths.get(0)).getMd5Hash();

      // (1) when the target files do not exist
      gcsUtil.copy(srcList, dstList);
      assertEquals(srcMd5, gcsUtil.getObject(dstPaths.get(0)).getMd5Hash());
      assertExists(dstPaths.get(1));

      // (2) when the target files exist, they are overwritten
      writeObject(
          dstPaths.get(0),
          "stale".getBytes(StandardCharsets.UTF_8),
          CreateOptions.builder().build());
      gcsUtil.copy(srcList, dstList);
      assertEquals(srcMd5, gcsUtil.getObject(dstPaths.get(0)).getMd5Hash());

      // (3) raise exception when the target bucket is nonexistent.
      assertThrows(FileNotFoundException.class, () -> gcsUtil.copy(srcList, errList));

      // (4) raise exception when the source files are nonexistent.
      assertThrows(FileNotFoundException.class, () -> gcsUtil.copy(errList, dstList));

      // (5) remove existing files
      gcsUtil.remove(dstList);
      assertNotExists(dstPaths.get(0));
      assertNotExists(dstPaths.get(1));
      assertExists(srcPaths.get(0));

      // (6) removing missing files, or files in a nonexistent bucket, raises no exception
      gcsUtil.remove(dstList);
      gcsUtil.remove(errList);
    } finally {
      tearDownTestBucketHelper(bucket);
    }
  }

  @Test
  public void testLegacyRename() throws IOException {
    final String bucket = randomBucketName();

    try {
      final List<GcsPath> srcPaths = createTestBucketHelper(bucket, true);
      final List<GcsPath> dstPaths =
          srcPaths.stream()
              .map(o -> GcsPath.fromComponents(bucket, o.getObject() + ".bak"))
              .collect(Collectors.toList());
      final List<GcsPath> missingPaths =
          srcPaths.stream()
              .map(o -> GcsPath.fromComponents(bucket, "missing/" + o.getObject()))
              .collect(Collectors.toList());
      final List<String> dstList = toStrings(dstPaths);
      final List<String> missingList = toStrings(missingPaths);
      final String srcMd5 = gcsUtil.getObject(srcPaths.get(0)).getMd5Hash();

      // (1) when the source files exist and target files do not
      gcsUtil.rename(toStrings(srcPaths), dstList);
      assertNotExists(srcPaths.get(0));
      assertNotExists(srcPaths.get(1));
      assertEquals(srcMd5, gcsUtil.getObject(dstPaths.get(0)).getMd5Hash());
      assertExists(dstPaths.get(1));

      // (2) when the source files do not exist
      // (2a) no exception if IGNORE_MISSING_FILES is set, and the targets are untouched
      gcsUtil.rename(missingList, dstList, MoveOptions.StandardMoveOptions.IGNORE_MISSING_FILES);
      assertExists(dstPaths.get(0));
      assertExists(dstPaths.get(1));

      // (2b) raise exception if IGNORE_MISSING_FILES is not set
      assertThrows(FileNotFoundException.class, () -> gcsUtil.rename(missingList, dstList));
    } finally {
      tearDownTestBucketHelper(bucket);
    }
  }

  @Test
  public void testLegacyRenameSkipDestinationExists() throws IOException {
    final String bucket = randomBucketName();
    final String otherBucket = randomBucketName();

    try {
      final List<GcsPath> srcPaths = createTestBucketHelper(bucket, true);
      createTestBucketHelper(otherBucket, false);
      final List<GcsPath> dstPaths =
          srcPaths.stream()
              .map(o -> GcsPath.fromComponents(bucket, o.getObject() + ".bak"))
              .collect(Collectors.toList());
      final List<GcsPath> otherPaths =
          dstPaths.stream()
              .map(o -> GcsPath.fromComponents(otherBucket, o.getObject()))
              .collect(Collectors.toList());
      final List<String> srcList = toStrings(srcPaths);
      final List<String> dstList = toStrings(dstPaths);
      final List<String> otherList = toStrings(otherPaths);
      gcsUtil.copy(srcList, dstList);

      // G3: within a bucket, when the targets exist.
      gcsUtil.rename(srcList, dstList, MoveOptions.StandardMoveOptions.SKIP_IF_DESTINATION_EXISTS);
      assertExists(dstPaths.get(0));
      assertExists(dstPaths.get(1));
      if (isV2()) {
        // V2 skips the rename and keeps the sources.
        assertExists(srcPaths.get(0));
        assertExists(srcPaths.get(1));
      } else {
        // There is a bug in V1 where SKIP_IF_DESTINATION_EXISTS is not honored.
        assertNotExists(srcPaths.get(0));
        assertNotExists(srcPaths.get(1));
      }

      // G2: across buckets, when the targets do not exist.
      if (isV2()) {
        gcsUtil.rename(
            dstList, otherList, MoveOptions.StandardMoveOptions.SKIP_IF_DESTINATION_EXISTS);
        assertNotExists(dstPaths.get(0));
        assertNotExists(dstPaths.get(1));
        assertExists(otherPaths.get(0));
        assertExists(otherPaths.get(1));
      } else {
        // V1 only supports SKIP_IF_DESTINATION_EXISTS within a bucket.
        assertThrows(
            UnsupportedOperationException.class,
            () ->
                gcsUtil.rename(
                    dstList,
                    otherList,
                    MoveOptions.StandardMoveOptions.SKIP_IF_DESTINATION_EXISTS));
        assertExists(dstPaths.get(0));
        assertExists(dstPaths.get(1));
      }
    } finally {
      tearDownTestBucketHelper(bucket);
      tearDownTestBucketHelper(otherBucket);
    }
  }

  @Test
  public void testLegacyCreateOverExistingObject() throws IOException {
    final String bucket = randomBucketName();
    final GcsPath path = GcsPath.fromComponents(bucket, "test-object.txt");
    final byte[] first = "first".getBytes(StandardCharsets.UTF_8);
    final byte[] second = "second".getBytes(StandardCharsets.UTF_8);

    try {
      createTestBucketHelper(bucket, false);
      writeObject(path, first, CreateOptions.builder().build());

      // Without expectFileToNotExist, an existing object is overwritten.
      writeObject(path, second, CreateOptions.builder().build());
      assertArrayEquals(second, readObject(path));

      // With expectFileToNotExist, the write fails and the object is left unchanged.
      assertThrows(
          IOException.class,
          () ->
              writeObject(
                  path, first, CreateOptions.builder().setExpectFileToNotExist(true).build()));
      assertArrayEquals(second, readObject(path));
    } finally {
      tearDownTestBucketHelper(bucket);
    }
  }

  @Test
  public void testLegacyWriteAndReadMultipleChunks() throws IOException {
    final String bucket = randomBucketName();
    final GcsPath path = GcsPath.fromComponents(bucket, "test-object.bin");
    final int chunkSize = 256 * 1024;
    // Four full chunks plus a partial one.
    final byte[] content = new byte[4 * chunkSize + 13];
    new Random(42).nextBytes(content);

    try {
      createTestBucketHelper(bucket, false);
      writeObject(
          path,
          content,
          CreateOptions.builder()
              .setContentType("application/octet-stream")
              .setUploadBufferSizeBytes(chunkSize)
              .setExpectFileToNotExist(true)
              .build());

      assertEquals(content.length, gcsUtil.fileSize(path));
      StorageObject obj = gcsUtil.getObject(path);
      assertEquals(BigInteger.valueOf(content.length), obj.getSize());
      assertEquals("application/octet-stream", obj.getContentType());
      assertArrayEquals(content, readObject(path));
    } finally {
      tearDownTestBucketHelper(bucket);
    }
  }

  @Test
  public void testReadChannelCloseTwice() throws IOException {
    SeekableByteChannel channel = gcsUtil.open(GcsPath.fromUri(KINGLEAR));
    assertTrue(channel.isOpen());
    channel.close();
    assertFalse(channel.isOpen());
    // Closing again is a no-op.
    channel.close();
    assertFalse(channel.isOpen());
  }

  // ---------------------------------------------------------------------------------------------
  // Metrics parity: the same assertions must hold for both GcsUtilV1 and GcsUtilV2.
  // ---------------------------------------------------------------------------------------------

  /** Returns a {@link GcsUtil} with every GCS metric flag turned on. */
  private GcsUtil gcsUtilWithAllMetrics() {
    GcsOptions gcsOptions = options.as(GcsOptions.class);
    gcsOptions.setGcsPerformanceMetrics(true);
    gcsOptions.setEnableBucketReadMetricCounter(true);
    gcsOptions.setEnableBucketWriteMetricCounter(true);
    gcsOptions.setGcsReadCounterPrefix(READ_COUNTER_PREFIX);
    gcsOptions.setGcsWriteCounterPrefix(WRITE_COUNTER_PREFIX);
    // Built directly, as getGcsUtil() returns the instance cached in setUp().
    return new GcsUtil(gcsOptions);
  }

  private static long counter(MetricsContainerImpl container, MetricName name) {
    CounterCell cell = container.tryGetCounter(name);
    assertNotNull("counter " + name + " was not reported", cell);
    return cell.getCumulative();
  }

  private static long gcsCounter(MetricsContainerImpl container, String name) {
    return counter(container, MetricName.named(GcsUtil.METRIC_NAMESPACE, name));
  }

  private static long bucketCounter(MetricsContainerImpl container, String prefix, String bucket) {
    return counter(container, MetricName.named(GcsUtil.class, prefix + "_" + bucket));
  }

  /**
   * Sums the API request counter for {@code method} and {@code status} on {@code bucket}.
   *
   * <p>The {@code GCS_PROJECT_ID} label is deliberately not matched: V1 reports the project of its
   * gcsio options (which is never set, so it reports {@code "null"}), while V2 reports the
   * pipeline's project.
   */
  private static long apiRequestCount(
      MetricsContainerImpl container, String method, String status, String bucket) {
    long total = 0;
    for (MetricUpdate<Long> update : container.getCumulative().counterUpdates()) {
      MetricName name = update.getKey().metricName();
      if (!(name instanceof MonitoringInfoMetricName)) {
        continue;
      }
      MonitoringInfoMetricName miName = (MonitoringInfoMetricName) name;
      Map<String, String> labels = miName.getLabels();
      if (MonitoringInfoConstants.Urns.API_REQUEST_COUNT.equals(miName.getUrn())
          && "Storage".equals(labels.get(MonitoringInfoConstants.Labels.SERVICE))
          && method.equals(labels.get(MonitoringInfoConstants.Labels.METHOD))
          && status.equals(labels.get(MonitoringInfoConstants.Labels.STATUS))
          && bucket.equals(labels.get(MonitoringInfoConstants.Labels.GCS_BUCKET))
          && GcpResourceIdentifiers.cloudStorageBucket(bucket)
              .equals(labels.get(MonitoringInfoConstants.Labels.RESOURCE))) {
        total += update.getUpdate();
      }
    }
    return total;
  }

  @Test
  public void testReadMetrics() throws IOException {
    final String bucket = "apache-beam-samples";
    final GcsPath gcsPath = GcsPath.fromComponents(bucket, "shakespeare/kinglear.txt");
    final long expectedSize = 157283L;
    GcsUtil metricsGcsUtil = gcsUtilWithAllMetrics();

    MetricsContainerImpl container = new MetricsContainerImpl("step");
    MetricsContainerImpl processWide = new MetricsContainerImpl(null);
    MetricsEnvironment.setCurrentContainer(container);
    MetricsEnvironment.setProcessWideContainer(processWide);
    try {
      try (SeekableByteChannel channel = metricsGcsUtil.open(gcsPath)) {
        ByteBuffer buffer = ByteBuffer.allocate((int) expectedSize + 1024);
        assertEquals(expectedSize, StorageChannelUtils.blockingFillFrom(buffer, channel));
      }
    } finally {
      MetricsEnvironment.setCurrentContainer(null);
      MetricsEnvironment.setProcessWideContainer(null);
    }

    // --enableBucketReadMetricCounter
    assertEquals(expectedSize, bucketCounter(container, READ_COUNTER_PREFIX, bucket));
    // --gcsPerformanceMetrics: wire bytes and HTTP counters
    assertEquals(expectedSize, gcsCounter(container, "gcs_http_read_wire_bytes_received"));
    assertTrue(gcsCounter(container, "gcs_http_read_request_count") >= 1);
    assertTrue(gcsCounter(container, "gcs_http_read_status_2xx") >= 1);
    assertEquals(
        gcsCounter(container, "gcs_http_read_request_count"),
        gcsCounter(container, "gcs_http_read_request_count_ranged")
            + gcsCounter(container, "gcs_http_read_request_count_unbounded")
            + gcsCounter(container, "gcs_http_read_request_count_other"));
    // A read must not produce write-side counters.
    assertNull(
        container.tryGetCounter(
            MetricName.named(GcsUtil.METRIC_NAMESPACE, "gcs_http_write_request_count")));
    // API request metric
    assertEquals(1, apiRequestCount(processWide, "GcsGet", "ok", bucket));
  }

  @Test
  public void testWriteMetrics() throws IOException {
    final String bucket =
        "apache-beam-temp-metrics-" + java.util.UUID.randomUUID().toString().substring(0, 8);
    final GcsPath targetPath = GcsPath.fromComponents(bucket, "test-object.txt");
    final byte[] content = "Hello, GCS metrics!".getBytes(StandardCharsets.UTF_8);
    GcsUtil metricsGcsUtil = gcsUtilWithAllMetrics();

    MetricsContainerImpl container = new MetricsContainerImpl("step");
    MetricsContainerImpl processWide = new MetricsContainerImpl(null);
    try {
      createTestBucketHelper(bucket, false);

      MetricsEnvironment.setCurrentContainer(container);
      MetricsEnvironment.setProcessWideContainer(processWide);
      try (WritableByteChannel writer =
          metricsGcsUtil.create(
              targetPath, CreateOptions.builder().setExpectFileToNotExist(true).build())) {
        writer.write(ByteBuffer.wrap(content));
      } finally {
        MetricsEnvironment.setCurrentContainer(null);
        MetricsEnvironment.setProcessWideContainer(null);
      }
    } finally {
      tearDownTestBucketHelper(bucket);
    }

    // --enableBucketWriteMetricCounter
    assertEquals(content.length, bucketCounter(container, WRITE_COUNTER_PREFIX, bucket));
    // --gcsPerformanceMetrics: wire bytes and HTTP counters
    assertEquals(content.length, gcsCounter(container, "gcs_http_write_wire_bytes_sent"));
    assertTrue(gcsCounter(container, "gcs_http_write_request_count") >= 1);
    assertTrue(gcsCounter(container, "gcs_http_write_status_2xx") >= 1);
    // A write must not produce read-side counters.
    assertNull(
        container.tryGetCounter(
            MetricName.named(GcsUtil.METRIC_NAMESPACE, "gcs_http_read_request_count")));
    // API request metric
    assertEquals(1, apiRequestCount(processWide, "GcsInsert", "ok", bucket));
  }

  // ---------------------------------------------------------------------------------------------
  // V1-only tests.
  // ---------------------------------------------------------------------------------------------

  /** Tests a rewrite operation that requires multiple API calls (using a continuation token). */
  @Test
  public void testRewriteMultiPart() throws IOException {
    // V2 copies each file with a single call, without rewrite tokens.
    assumeTrue(experiment.equals("use_gcsutil_v1"));

    TestPipelineOptions options =
        TestPipeline.testingPipelineOptions().as(TestPipelineOptions.class);
    // Using a KMS key is necessary to trigger multi-part rewrites (bucket is created
    // with a bucket default key).
    assertNotNull(options.getTempRoot());
    options.setTempLocation(
        FileSystems.matchNewDirectory(options.getTempRoot(), "testRewriteMultiPart").toString());

    GcsOptions gcsOptions = options.as(GcsOptions.class);
    GcsUtil gcsUtil = gcsOptions.getGcsUtil();
    String srcFilename = "gs://dataflow-samples/wikipedia_edits/wiki_data-000000000000.json";
    String dstFilename =
        gcsOptions.getGcpTempLocation()
            + String.format(
                "/GcsUtilIT-%tF-%<tH-%<tM-%<tS-%<tL.testRewriteMultiPart.copy",
                LocalDateTime.now(ZoneId.of("UTC")));
    gcsUtil.delegate.maxBytesRewrittenPerCall = 50L * 1024 * 1024;
    gcsUtil.delegate.numRewriteTokensUsed = new AtomicInteger();

    gcsUtil.copy(Lists.newArrayList(srcFilename), Lists.newArrayList(dstFilename));

    assertThat(gcsUtil.delegate.numRewriteTokensUsed.get(), equalTo(3));
    assertThat(
        gcsUtil.getObject(GcsPath.fromUri(srcFilename)).getMd5Hash(),
        equalTo(gcsUtil.getObject(GcsPath.fromUri(dstFilename)).getMd5Hash()));

    gcsUtil.remove(Lists.newArrayList(dstFilename));
  }

  // TODO: once the gRPC feature is in public GA, we will have to refactor this test.
  // As gRPC will be automatically enabled in each bucket by then, we will no longer need to check
  // the failed case. The interface of GcsGrpcOptions can also be removed.
  @Test
  public void testWriteAndReadGcsWithGrpc() throws IOException {
    // GcsUtilV2 does not support gRPC yet.
    assumeTrue(experiment.equals("use_gcsutil_v1"));

    final String outputPattern =
        "%s/GcsUtilIT-%tF-%<tH-%<tM-%<tS-%<tL.testWriteAndReadGcsWithGrpc.txt";
    final String testContent = "This is a test string.";

    TestPipelineOptions options =
        TestPipeline.testingPipelineOptions().as(TestPipelineOptions.class);

    // set the experimental flag to enable grpc
    ExperimentalOptions experimental = options.as(ExperimentalOptions.class);
    experimental.setExperiments(Collections.singletonList("use_grpc_for_gcs"));

    GcsOptions gcsOptions = options.as(GcsOptions.class);
    GcsUtil gcsUtil = gcsOptions.getGcsUtil();
    assertNotNull(gcsUtil);

    // Write a test file in a bucket with gRPC enabled.
    String tempLocationWithGrpc = options.getTempRoot() + "/temp";
    String filename =
        String.format(outputPattern, tempLocationWithGrpc, LocalDateTime.now(ZoneId.of("UTC")));
    writeGcsTextFile(gcsUtil, filename, testContent);

    // Read the test file back and verify
    assertEquals(testContent, readGcsTextFile(gcsUtil, filename));

    gcsUtil.remove(Collections.singletonList(filename));
  }

  void writeGcsTextFile(GcsUtil gcsUtil, String filename, String content) throws IOException {
    GcsPath gcsPath = GcsPath.fromUri(filename);
    try (WritableByteChannel channel =
        gcsUtil.create(
            gcsPath, CreateOptions.builder().setContentType("text/plain;charset=utf-8").build())) {
      channel.write(ByteString.copyFromUtf8(content).asReadOnlyByteBuffer());
    }
  }

  String readGcsTextFile(GcsUtil gcsUtil, String filename) throws IOException {
    GcsPath gcsPath = GcsPath.fromUri(filename);
    try (ByteStringOutputStream output = new ByteStringOutputStream()) {
      try (ReadableByteChannel channel = gcsUtil.open(gcsPath)) {
        ByteBuffer bb = ByteBuffer.allocate(16);
        while (channel.read(bb) != -1) {
          output.write(bb.array(), 0, bb.capacity() - bb.remaining());
          bb.clear();
        }
      }
      return output.toByteString().toStringUtf8();
    }
  }
}
