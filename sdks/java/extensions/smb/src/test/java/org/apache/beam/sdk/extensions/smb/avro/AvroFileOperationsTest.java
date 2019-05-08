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

import static org.apache.beam.sdk.extensions.smb.TestUtils.fromFolder;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.extensions.smb.FileOperations;
import org.apache.beam.sdk.io.AvroGeneratedUser;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Unit tests for {@link AvroFileOperations}. */
public class AvroFileOperationsTest {
  @Rule public final TemporaryFolder output = new TemporaryFolder();

  private static final Schema USER_SCHEMA =
      Schema.createRecord(
          "User",
          "",
          "org.apache.beam.sdk.extensions.smb.avro",
          false,
          Lists.newArrayList(
              new Schema.Field("name", Schema.create(Schema.Type.STRING), "", ""),
              new Schema.Field("age", Schema.create(Schema.Type.INT), "", 0)));

  @Test
  public void testGenericRecord() throws Exception {
    final AvroFileOperations<GenericRecord> fileOperations = AvroFileOperations.of(USER_SCHEMA);
    final ResourceId file =
        fromFolder(output).resolve("file.avro", StandardResolveOptions.RESOLVE_FILE);

    final List<GenericRecord> records =
        IntStream.range(0, 10)
            .mapToObj(
                i ->
                    new GenericRecordBuilder(USER_SCHEMA)
                        .set("name", String.format("user%02d", i))
                        .set("age", i)
                        .build())
            .collect(Collectors.toList());
    final FileOperations.Writer<GenericRecord> writer = fileOperations.createWriter();
    writer.prepareWrite(FileSystems.create(file, writer.getMimeType()));
    for (GenericRecord record : records) {
      writer.write(record);
    }
    writer.finishWrite();

    final List<GenericRecord> actual = new ArrayList<>();
    fileOperations.iterator(file).forEachRemaining(actual::add);

    Assert.assertEquals(records, actual);
  }

  @Test
  public void testSpecificRecord() throws Exception {
    final AvroFileOperations<AvroGeneratedUser> fileOperations =
        AvroFileOperations.of(AvroGeneratedUser.class);
    final ResourceId file =
        fromFolder(output).resolve("file.avro", StandardResolveOptions.RESOLVE_FILE);

    final List<AvroGeneratedUser> records =
        IntStream.range(0, 10)
            .mapToObj(
                i ->
                    AvroGeneratedUser.newBuilder()
                        .setName(String.format("user%02d", i))
                        .setFavoriteColor(String.format("color%02d", i))
                        .setFavoriteNumber(i)
                        .build())
            .collect(Collectors.toList());
    final FileOperations.Writer<AvroGeneratedUser> writer = fileOperations.createWriter();
    writer.prepareWrite(FileSystems.create(file, writer.getMimeType()));
    for (AvroGeneratedUser record : records) {
      writer.write(record);
    }
    writer.finishWrite();

    final List<AvroGeneratedUser> actual = new ArrayList<>();
    fileOperations.iterator(file).forEachRemaining(actual::add);

    Assert.assertEquals(records, actual);
  }
}
