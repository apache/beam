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
package org.apache.beam.sdk.extensions.smb.avro;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.extensions.smb.BucketMetadata.HashType;
import org.apache.beam.sdk.extensions.smb.SMBFilenamePolicy;
import org.apache.beam.sdk.extensions.smb.SortedBucketFile.Writer;
import org.apache.beam.sdk.extensions.smb.SortedBucketSource;
import org.apache.beam.sdk.io.AvroGeneratedUser;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.LocalResources;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Tests Avro SMB source. */
public class AvroSortedBucketSourceTest {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  @Rule public final TemporaryFolder source1Folder = new TemporaryFolder();
  @Rule public final TemporaryFolder source2Folder = new TemporaryFolder();

  private SMBFilenamePolicy filenamePolicySource1;
  private SMBFilenamePolicy filenamePolicySource2;

  @Before
  public void setup() {
    filenamePolicySource1 =
        new SMBFilenamePolicy(LocalResources.fromFile(source1Folder.getRoot(), true), "avro");
    filenamePolicySource2 =
        new SMBFilenamePolicy(LocalResources.fromFile(source2Folder.getRoot(), true), "avro");
  }

  @Test
  public void testGenericRecordSources() throws Exception {

    // Setup: write GenericRecords to sink
    final AvroSortedBucketFile<GenericRecord> file =
        new AvroSortedBucketFile<>(null, TestUtils.USER_SCHEMA);

    final GenericRecord userA = TestUtils.createUserRecord("a", 50);
    final GenericRecord userB = TestUtils.createUserRecord("b", 50);
    final GenericRecord userC = TestUtils.createUserRecord("c", 25);
    final GenericRecord userD = TestUtils.createUserRecord("d", 25);
    final GenericRecord userE = TestUtils.createUserRecord("e", 75);

    final Writer<GenericRecord> writer = file.createWriter();

    writer.prepareWrite(
        FileSystems.create(
            filenamePolicySource1.forDestination().forBucket(0, 1),
            file.createWriter().getMimeType()));

    writer.write(userC);
    writer.write(userA);
    writer.write(userA);
    writer.write(userE);
    writer.finishWrite();

    writer.prepareWrite(
        FileSystems.create(
            filenamePolicySource2.forDestination().forBucket(0, 1),
            file.createWriter().getMimeType()));
    writer.write(userD);
    writer.write(userB);
    writer.finishWrite();

    final AvroBucketMetadata<Integer, GenericRecord> metadata =
        new AvroBucketMetadata<>(1, Integer.class, HashType.MURMUR3_32, "age");

    final ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.writeValue(
        new File(source1Folder.getRoot().getAbsolutePath(), "metadata.json"), metadata);

    objectMapper.writeValue(
        new File(source2Folder.getRoot().getAbsolutePath(), "metadata.json"), metadata);

    // Test execution

    final SortedBucketSource<Integer, KV<Iterable<GenericRecord>, Iterable<GenericRecord>>>
        sourceTransform =
            AvroSortedBucketIO.SortedBucketSourceJoinBuilder.forKeyType(Integer.class)
                .of(LocalResources.fromFile(source1Folder.getRoot(), true), TestUtils.USER_SCHEMA)
                .and(LocalResources.fromFile(source2Folder.getRoot(), true), TestUtils.USER_SCHEMA)
                .build();

    final PCollection<KV<Integer, KV<Iterable<GenericRecord>, Iterable<GenericRecord>>>>
        joinedSources = pipeline.apply(sourceTransform);

    PAssert.that(joinedSources)
        .containsInAnyOrder(
            KV.of(50, KV.of(ImmutableList.of(userA, userA), ImmutableList.of(userB))),
            KV.of(25, KV.of(ImmutableList.of(userC), ImmutableList.of(userD))),
            KV.of(75, KV.of(ImmutableList.of(userE), null)));

    pipeline.run();
  }

  @Test
  public void testSpecificRecordSources() throws Exception {
    // Setup: write SpecificRecords to sink

    final AvroSortedBucketFile<AvroGeneratedUser> file =
        new AvroSortedBucketFile<>(AvroGeneratedUser.class, AvroGeneratedUser.SCHEMA$);

    final AvroGeneratedUser userA = new AvroGeneratedUser("a", 50, "red");
    final AvroGeneratedUser userB = new AvroGeneratedUser("b", 50, "green");
    final AvroGeneratedUser userC = new AvroGeneratedUser("c", 25, "red");
    final AvroGeneratedUser userD = new AvroGeneratedUser("d", 25, "blue");
    final AvroGeneratedUser userE = new AvroGeneratedUser("e", 75, "yellow");

    final Writer<AvroGeneratedUser> writer = file.createWriter();

    writer.prepareWrite(
        FileSystems.create(
            filenamePolicySource1.forDestination().forBucket(0, 1),
            file.createWriter().getMimeType()));

    writer.write(userC);
    writer.write(userA);
    writer.write(userA);
    writer.write(userE);
    writer.finishWrite();

    writer.prepareWrite(
        FileSystems.create(
            filenamePolicySource2.forDestination().forBucket(0, 1),
            file.createWriter().getMimeType()));
    writer.write(userD);
    writer.write(userB);
    writer.finishWrite();

    final AvroBucketMetadata<Integer, GenericRecord> metadata =
        new AvroBucketMetadata<>(1, Integer.class, HashType.MURMUR3_32, "favorite_number");

    final ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.writeValue(
        new File(source1Folder.getRoot().getAbsolutePath(), "metadata.json"), metadata);

    objectMapper.writeValue(
        new File(source2Folder.getRoot().getAbsolutePath(), "metadata.json"), metadata);

    // Test execution

    final SortedBucketSource<Integer, KV<Iterable<AvroGeneratedUser>, Iterable<AvroGeneratedUser>>>
        sourceTransform =
            AvroSortedBucketIO.SortedBucketSourceJoinBuilder.forKeyType(Integer.class)
                .of(
                    LocalResources.fromFile(source1Folder.getRoot(), true),
                    AvroGeneratedUser.SCHEMA$,
                    AvroGeneratedUser.class)
                .and(
                    LocalResources.fromFile(source2Folder.getRoot(), true),
                    AvroGeneratedUser.SCHEMA$,
                    AvroGeneratedUser.class)
                .build();

    final PCollection<KV<Integer, KV<Iterable<AvroGeneratedUser>, Iterable<AvroGeneratedUser>>>>
        joinedSources = pipeline.apply(sourceTransform);

    PAssert.that(joinedSources)
        .containsInAnyOrder(
            KV.of(50, KV.of(ImmutableList.of(userA, userA), ImmutableList.of(userB))),
            KV.of(25, KV.of(ImmutableList.of(userC), ImmutableList.of(userD))),
            KV.of(75, KV.of(ImmutableList.of(userE), null)));

    pipeline.run();
  }
}
