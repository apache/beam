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

import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.extensions.smb.BucketMetadata;
import org.apache.beam.sdk.extensions.smb.BucketMetadata.HashType;
import org.apache.beam.sdk.extensions.smb.SortedBucketSink;
import org.apache.beam.sdk.extensions.smb.SortedBucketSink.WriteResult;
import org.apache.beam.sdk.io.AvroGeneratedUser;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.LocalResources;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.fs.ResourceIdCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Tests Avro SMB sink. */
public class AvroSortedBucketSinkTest {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  @Rule public final TemporaryFolder outputFolder1 = new TemporaryFolder();
  @Rule public final TemporaryFolder outputFolder2 = new TemporaryFolder();
  @Rule public final TemporaryFolder tmpFolder = new TemporaryFolder();

  @Test
  public void testGenericRecordSink() throws Exception {
    final AvroBucketMetadata<Integer, GenericRecord> metadata =
        new AvroBucketMetadata<>(1, Integer.class, HashType.MURMUR3_32, "age");

    final SortedBucketSink<Integer, GenericRecord> sink =
        AvroSortedBucketIO.sink(
            metadata,
            LocalResources.fromFile(outputFolder1.getRoot(), true),
            LocalResources.fromFile(tmpFolder.getRoot(), true),
            TestUtils.USER_SCHEMA);

    final PCollection<GenericRecord> users =
        pipeline.apply(
            Create.of(
                    Lists.newArrayList(
                        TestUtils.createUserRecord("a", 50),
                        TestUtils.createUserRecord("b", 75),
                        TestUtils.createUserRecord("c", 25)))
                .withCoder(TestUtils.USER_CODER));

    final WriteResult writeResult = users.apply("test sink", sink);

    final PCollection<ResourceId> writtenMetadata =
        (PCollection<ResourceId>) writeResult.expand().get(new TupleTag<>("SMBMetadataWritten"));

    PAssert.that(writtenMetadata)
        .satisfies(
            m -> {
              final ResourceId metadataFile = m.iterator().next();
              try {
                final BucketMetadata<Integer, Object> readMetadata =
                    BucketMetadata.from(Channels.newInputStream(FileSystems.open(metadataFile)));

                Assert.assertTrue(readMetadata.compatibleWith(metadata));
              } catch (IOException e) {
                Assert.fail(String.format("Failed to read written metadata file: %s", e));
              }

              return null;
            });

    final PCollection<KV<Integer, ResourceId>> writtenBuckets =
        (PCollection<KV<Integer, ResourceId>>)
            writeResult.expand().get(new TupleTag<>("SortedBucketsWritten"));

    PAssert.that(writtenBuckets.setCoder(KvCoder.of(VarIntCoder.of(), ResourceIdCoder.of())))
        .satisfies(
            b -> {
              final KV<Integer, ResourceId> bucketFile = b.iterator().next();
              Assert.assertTrue(0 == bucketFile.getKey());

              try {
                final ReadableByteChannel channel = FileSystems.open(bucketFile.getValue());
                final DataFileStream<GenericRecord> reader =
                    new DataFileStream<>(
                        Channels.newInputStream(channel),
                        new GenericDatumReader<>(TestUtils.USER_SCHEMA));

                Assert.assertEquals("c", reader.next().get("name").toString());
                Assert.assertEquals("a", reader.next().get("name").toString());
                Assert.assertEquals("b", reader.next().get("name").toString());

                Assert.assertFalse(reader.hasNext());
                reader.close();
              } catch (IOException e) {
                Assert.fail(String.format("Failed to read written bucket file: %s", e));
              }
              return null;
            });

    pipeline.run();
  }

  @Test
  public void testSpecificRecordSink() throws Exception {
    final AvroBucketMetadata<Integer, AvroGeneratedUser> metadata =
        new AvroBucketMetadata<>(1, Integer.class, HashType.MURMUR3_32, "favorite_number");

    final SortedBucketSink<Integer, AvroGeneratedUser> sink =
        AvroSortedBucketIO.sink(
            metadata,
            LocalResources.fromFile(outputFolder2.getRoot(), true),
            LocalResources.fromFile(tmpFolder.getRoot(), true),
            AvroGeneratedUser.class);

    final PCollection<AvroGeneratedUser> users =
        pipeline.apply(
            Create.of(
                    Lists.newArrayList(
                        new AvroGeneratedUser("a", 50, "red"),
                        new AvroGeneratedUser("b", 30, "green"),
                        new AvroGeneratedUser("c", 25, "red")))
                .withCoder(AvroCoder.of(AvroGeneratedUser.class)));

    final WriteResult writeResult = users.apply("test sink", sink);

    final PCollection<ResourceId> writtenMetadata =
        (PCollection<ResourceId>) writeResult.expand().get(new TupleTag<>("SMBMetadataWritten"));

    PAssert.that(writtenMetadata)
        .satisfies(
            m -> {
              final ResourceId metadataFile = m.iterator().next();
              try {
                final BucketMetadata<Integer, Object> readMetadata =
                    BucketMetadata.from(Channels.newInputStream(FileSystems.open(metadataFile)));

                Assert.assertTrue(readMetadata.compatibleWith(metadata));
              } catch (IOException e) {
                Assert.fail(String.format("Failed to read written metadata file: %s", e));
              }

              return null;
            });

    final PCollection<KV<Integer, ResourceId>> writtenBuckets =
        (PCollection<KV<Integer, ResourceId>>)
            writeResult.expand().get(new TupleTag<>("SortedBucketsWritten"));

    PAssert.that(writtenBuckets.setCoder(KvCoder.of(VarIntCoder.of(), ResourceIdCoder.of())))
        .satisfies(
            b -> {
              final KV<Integer, ResourceId> bucketFile = b.iterator().next();
              Assert.assertTrue(0 == bucketFile.getKey());

              try {
                final ReadableByteChannel channel = FileSystems.open(bucketFile.getValue());
                final DataFileStream<AvroGeneratedUser> reader =
                    new DataFileStream<>(
                        Channels.newInputStream(channel),
                        new ReflectDatumReader<>(AvroGeneratedUser.class));

                Assert.assertEquals("c", reader.next().getName());
                Assert.assertEquals("b", reader.next().getName());
                Assert.assertEquals("a", reader.next().getName());

                Assert.assertFalse(reader.hasNext());
                reader.close();
              } catch (IOException e) {
                Assert.fail(String.format("Failed to read written bucket file: %s", e));
              }
              return null;
            });

    pipeline.run();
  }
}
