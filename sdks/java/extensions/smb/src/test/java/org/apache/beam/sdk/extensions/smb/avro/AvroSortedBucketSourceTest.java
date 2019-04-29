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
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.extensions.smb.BucketMetadata;
import org.apache.beam.sdk.extensions.smb.BucketMetadata.HashType;
import org.apache.beam.sdk.extensions.smb.SMBFilenamePolicy;
import org.apache.beam.sdk.extensions.smb.SortedBucketFile.Writer;
import org.apache.beam.sdk.extensions.smb.SortedBucketSource;
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

  private static final GenericRecord USER_A = TestUtils.createUserRecord("a", 50);
  private static final GenericRecord USER_B = TestUtils.createUserRecord("b", 50);
  private static final GenericRecord USER_C = TestUtils.createUserRecord("c", 25);
  private static final GenericRecord USER_D = TestUtils.createUserRecord("d", 25);
  private static final GenericRecord USER_E = TestUtils.createUserRecord("e", 75);

  private final AvroSortedBucketFile<GenericRecord> file =
      new AvroSortedBucketFile<>(GenericRecord.class, TestUtils.SCHEMA);

  // Write two sources with GenericRecords + metadata
  @Before
  public void setUp() throws Exception {
    final SMBFilenamePolicy filenamePolicySource1 =
        new SMBFilenamePolicy(LocalResources.fromFile(source1Folder.getRoot(), true), "avro");

    final SMBFilenamePolicy filenamePolicySource2 =
        new SMBFilenamePolicy(LocalResources.fromFile(source2Folder.getRoot(), true), "avro");

    final Writer<GenericRecord> writer = file.createWriter();

    writer.prepareWrite(
        FileSystems.create(
            filenamePolicySource1.forDestination().forBucketShard(0, 1, 1, 1),
            file.createWriter().getMimeType()));

    writer.write(USER_C);
    writer.write(USER_A);
    writer.write(USER_A);
    writer.write(USER_E);
    writer.finishWrite();

    writer.prepareWrite(
        FileSystems.create(
            filenamePolicySource2.forDestination().forBucketShard(0, 1, 1, 1),
            file.createWriter().getMimeType()));
    writer.write(USER_D);
    writer.write(USER_B);
    writer.finishWrite();

    final BucketMetadata<Integer, GenericRecord> metadata =
        TestUtils.tryCreateMetadata(1, HashType.MURMUR3_32);

    final ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.writeValue(
        new File(source1Folder.getRoot().getAbsolutePath(), "metadata.json"), metadata);

    objectMapper.writeValue(
        new File(source2Folder.getRoot().getAbsolutePath(), "metadata.json"), metadata);
  }

  @Test
  public void testSources() {
    final SortedBucketSource<Integer, KV<Iterable<GenericRecord>, Iterable<GenericRecord>>>
        sourceTransform =
            AvroSortedBucketIO.SortedBucketSourceJoinBuilder.of(
                    VarIntCoder.of(),
                    AvroCoder.of(TestUtils.SCHEMA),
                    AvroCoder.of(TestUtils.SCHEMA))
                .of(LocalResources.fromFile(source1Folder.getRoot(), true), file.createReader())
                .and(LocalResources.fromFile(source2Folder.getRoot(), true), file.createReader())
                .build();

    final PCollection<KV<Integer, KV<Iterable<GenericRecord>, Iterable<GenericRecord>>>>
        joinedSources = pipeline.apply(sourceTransform);

    PAssert.that(joinedSources)
        .containsInAnyOrder(
            KV.of(50, KV.of(ImmutableList.of(USER_A, USER_A), ImmutableList.of(USER_B))),
            KV.of(25, KV.of(ImmutableList.of(USER_C), ImmutableList.of(USER_D))),
            KV.of(75, KV.of(ImmutableList.of(USER_E), null)));

    pipeline.run();
  }
}
