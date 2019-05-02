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

import com.google.protobuf.ByteString;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.smb.BucketMetadata.HashType;
import org.apache.beam.sdk.extensions.smb.avro.AvroBucketMetadata;
import org.apache.beam.sdk.extensions.smb.avro.AvroFileOperations;
import org.apache.beam.sdk.extensions.smb.json.JsonBucketMetadata;
import org.apache.beam.sdk.extensions.smb.json.JsonFileOperations;
import org.apache.beam.sdk.io.LocalResources;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Lists;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class MixedSourcesEndToEndTest implements Serializable {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  @Rule public final transient TestPipeline pipeline2 = TestPipeline.create();
  @Rule public final transient TestPipeline pipeline3 = TestPipeline.create();
  @Rule public final TemporaryFolder source1Folder = new TemporaryFolder();
  @Rule public final TemporaryFolder source2Folder = new TemporaryFolder();

  @Rule public final TemporaryFolder tmpFolder = new TemporaryFolder();
  @Rule public final TemporaryFolder tmpFolder2 = new TemporaryFolder();

  private static final Schema GR_USER_SCHEMA =
      Schema.createRecord(
          "user",
          "",
          "org.apache.beam.sdk.extensions.smb",
          false,
          Lists.newArrayList(
              new Field("name", Schema.create(Type.BYTES), "", null),
              new Field("age", Schema.create(Type.INT), "", null)));

  private static GenericRecord createUserGR(String name, int age) {
    GenericData.Record result = new GenericData.Record(GR_USER_SCHEMA);
    result.put("name", ByteBuffer.wrap(ByteString.copyFromUtf8(name).toByteArray()));
    result.put("age", age);

    return result;
  }

  private static Map<String, Object> createUserJson(String name, String country) {
    Map<String, Object> result = new HashMap<>();
    result.put("name", name);
    result.put("country", country);
    return result;
  }

  @Test
  public void testE2E() throws Exception {
    AvroBucketMetadata<ByteBuffer, GenericRecord> avroMetadata =
        new AvroBucketMetadata<>(2, ByteBuffer.class, HashType.MURMUR3_32, "name");

    pipeline
        .apply(
            Create.of(
                    createUserGR("a", 1),
                    createUserGR("b", 2),
                    createUserGR("c", 3),
                    createUserGR("d", 4),
                    createUserGR("e", 5),
                    createUserGR("f", 6))
                .withCoder(AvroCoder.of(GR_USER_SCHEMA)))
        .apply(
            SortedBucketIO.sink(
                avroMetadata,
                LocalResources.fromFile(source1Folder.getRoot(), true),
                "avro",
                LocalResources.fromFile(tmpFolder.getRoot(), true),
                new AvroFileOperations<>(null, GR_USER_SCHEMA)));

    pipeline.run().waitUntilFinish();

    JsonBucketMetadata<String> jsonMetadata =
        new JsonBucketMetadata<>(2, String.class, HashType.MURMUR3_32, "name");

    Coder<Map<String, Object>> jsonCoder = null; // @TODO needs a handcrafted coder to work

    pipeline2
        .apply(
            Create.of(
                    createUserJson("a", "US"),
                    createUserJson("b", "CN"),
                    createUserJson("c", "MX"),
                    createUserJson("d", "DE"),
                    createUserJson("e", "AU"),
                    createUserJson("g", "SE"))
                .withCoder(jsonCoder))
        .apply(
            SortedBucketIO.sink(
                jsonMetadata,
                LocalResources.fromFile(source2Folder.getRoot(), true),
                "json",
                LocalResources.fromFile(tmpFolder2.getRoot(), true),
                new JsonFileOperations()));

    pipeline2.run().waitUntilFinish();

    final SortedBucketSource<String, KV<Iterable<GenericRecord>, Iterable<Map<String, Object>>>>
        sourceTransform =
            SortedBucketIO.SortedBucketSourceJoinBuilder.withFinalKeyType(String.class)
                .of(
                    LocalResources.fromFile(source1Folder.getRoot(), true),
                    "avro",
                    new AvroFileOperations<>(null, GR_USER_SCHEMA),
                    AvroCoder.of(GR_USER_SCHEMA))
                .and(
                    LocalResources.fromFile(source2Folder.getRoot(), true),
                    "json",
                    new JsonFileOperations(),
                    jsonCoder)
                .build();

    final PCollection<KV<String, KV<Iterable<GenericRecord>, Iterable<Map<String, Object>>>>>
        joinedSources = pipeline3.apply(sourceTransform);

    PAssert.that(joinedSources)
        .satisfies(
            s -> {
              s.forEach(System.out::println);
              return null;
            });
    pipeline3.run();
  }
}
