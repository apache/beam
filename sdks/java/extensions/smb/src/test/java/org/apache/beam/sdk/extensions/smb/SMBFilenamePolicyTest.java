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
package org.apache.beam.sdk.extensions.smb;

import org.apache.beam.sdk.extensions.smb.BucketMetadata.HashType;
import org.apache.beam.sdk.extensions.smb.SMBFilenamePolicy.FileAssignment;
import org.apache.beam.sdk.io.LocalResources;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Unit tests for SMB filename policy. */
public class SMBFilenamePolicyTest {
  @Rule public final TemporaryFolder destination = new TemporaryFolder();
  @Rule public final TemporaryFolder tmpDestination = new TemporaryFolder();

  private static final String SUFFIX = ".foo";

  @Test
  public void testInvalidConstructor() {
    Assert.assertThrows(
        IllegalArgumentException.class,
        () ->
            new SMBFilenamePolicy(
                LocalResources.fromFile(destination.newFile("foo.txt"), false), SUFFIX));
  }

  @Test
  public void testDestinationFileAssignment() throws Exception {
    final SMBFilenamePolicy policy = testFilenamePolicy(destination);
    final FileAssignment fileAssignment = policy.forDestination();

    Assert.assertEquals(
        fileAssignment.forMetadata(), resolveFile(destination, "metadata", ".json"));

    final BucketMetadata metadata = new TestBucketMetadata(8, 3, HashType.MURMUR3_32);

    // Test valid shard-bucket combination
    Assert.assertEquals(
        fileAssignment.forBucket(BucketShardId.of(5, 1), metadata),
        resolveFile(destination, "bucket-00005-of-00008-shard-00001-of-00003", SUFFIX));

    Assert.assertEquals(
        fileAssignment.forBucket(BucketShardId.ofNullKey(1), metadata),
        resolveFile(destination, "bucket-null-keys-shard-00001-of-00003", SUFFIX));

    // Test invalid shard-bucket combinations
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> fileAssignment.forBucket(BucketShardId.of(100, 1), metadata));

    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> fileAssignment.forBucket(BucketShardId.of(2, 100), metadata));
  }

  @Test
  public void testTempFileAssignment() throws Exception {
    final ResourceId tmpDstResource = LocalResources.fromFile(tmpDestination.getRoot(), true);

    SMBFilenamePolicy policy = testFilenamePolicy(destination);
    final Long oldTempId = policy.getTempId();

    Assert.assertTrue(
        policy
            .forTempFiles(tmpDstResource)
            .forMetadata()
            .toString()
            .matches(tmpFileRegex(tmpDstResource, "metadata", ".json")));

    final BucketMetadata metadata = new TestBucketMetadata(8, 3, HashType.MURMUR3_32);

    // Recreate the policy to test tempId is incremented
    policy = testFilenamePolicy(destination);
    final Long newTempId = policy.getTempId();

    Assert.assertEquals(1, newTempId - oldTempId);

    // Test valid shard-bucket combination
    Assert.assertTrue(
        policy
            .forTempFiles(tmpDstResource)
            .forBucket(BucketShardId.of(5, 1), metadata)
            .toString()
            .matches(
                tmpFileRegex(
                    tmpDstResource, "bucket-00005-of-00008-shard-00001-of-00003", SUFFIX)));

    // Test invalid shard-bucket combinations
    Assert.assertThrows(
        IllegalArgumentException.class,
        () ->
            testFilenamePolicy(destination)
                .forTempFiles(tmpDstResource)
                .forBucket(BucketShardId.of(100, 1), metadata));

    Assert.assertThrows(
        IllegalArgumentException.class,
        () ->
            testFilenamePolicy(destination)
                .forTempFiles(tmpDstResource)
                .forBucket(BucketShardId.of(2, 100), metadata));
  }

  private static SMBFilenamePolicy testFilenamePolicy(TemporaryFolder folder) {
    return new SMBFilenamePolicy(LocalResources.fromFile(folder.getRoot(), true), SUFFIX);
  }

  private static ResourceId resolveFile(TemporaryFolder parent, String filename, String suffix) {
    return LocalResources.fromFile(parent.getRoot(), true)
        .resolve(filename + suffix, StandardResolveOptions.RESOLVE_FILE);
  }

  private static String tmpFileRegex(ResourceId directory, String filename, String suffix) {
    final String timestampMinutePrefix =
        Instant.now().toString(DateTimeFormat.forPattern("yyyy-MM-dd_HH-mm-"));

    return String.format(
        "%s.temp-beam-%s\\d{2}-\\d{2}/%s\\d{2}-%s%s",
        directory.toString(), timestampMinutePrefix, timestampMinutePrefix, filename, suffix);
  }
}
