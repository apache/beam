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

import com.google.api.services.bigquery.model.TableRow;
import com.google.protobuf.ByteString;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.extensions.smb.BucketMetadata.HashType;
import org.apache.beam.sdk.extensions.smb.avro.AvroBucketMetadata;
import org.apache.beam.sdk.extensions.smb.avro.AvroSortedBucketIO;
import org.apache.beam.sdk.extensions.smb.json.JsonBucketMetadata;
import org.apache.beam.sdk.extensions.smb.json.JsonSortedBucketIO;
import org.apache.beam.sdk.io.LocalResources;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** E2E test for heterogeneously-typed SMB join. */
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
          Arrays.asList(
              new Field("name", Schema.create(Type.BYTES), "", ""),
              new Field("age", Schema.create(Type.INT), "", -1)));

  private static GenericRecord createUserGR(String name, int age) {
    GenericData.Record result = new GenericData.Record(GR_USER_SCHEMA);
    result.put("name", ByteBuffer.wrap(ByteString.copyFromUtf8(name).toByteArray()));
    result.put("age", age);

    return result;
  }

  private static TableRow createUserJson(String name, String country) {
    return new TableRow().set("name", name).set("country", country);
  }

  @Test
  public void testE2E() throws Exception {
    final AvroBucketMetadata<ByteBuffer, GenericRecord> avroMetadata =
        new AvroBucketMetadata<>(4, 3, ByteBuffer.class, HashType.MURMUR3_32, "name");

    pipeline
        .apply(
            Create.of(
                    createUserGR("a", 1),
                    createUserGR("b", 2),
                    createUserGR("c", 3),
                    createUserGR("d", 4),
                    createUserGR("e", 5),
                    createUserGR("f", 6),
                    createUserGR("g", 7),
                    createUserGR("h", 8))
                .withCoder(AvroCoder.of(GR_USER_SCHEMA)))
        .apply(
            AvroSortedBucketIO.sink(
                avroMetadata,
                LocalResources.fromFile(source1Folder.getRoot(), true),
                LocalResources.fromFile(tmpFolder.getRoot(), true),
                GR_USER_SCHEMA));

    pipeline.run().waitUntilFinish();

    final JsonBucketMetadata<String> jsonMetadata =
        new JsonBucketMetadata<>(8, 4, String.class, HashType.MURMUR3_32, "name");

    pipeline2
        .apply(
            Create.of(
                    createUserJson("a", "US"),
                    createUserJson("c", "DE"),
                    createUserJson("d", "MX"),
                    createUserJson("e", "AU"),
                    createUserJson("f", "US"),
                    createUserJson("g", "SE"),
                    createUserJson("h", "DE"),
                    createUserJson("i", "MX"))
                .withCoder(TableRowJsonCoder.of()))
        .apply(
            JsonSortedBucketIO.sink(
                jsonMetadata,
                LocalResources.fromFile(source2Folder.getRoot(), true),
                LocalResources.fromFile(tmpFolder2.getRoot(), true)));

    pipeline2.run().waitUntilFinish();

    final SortedBucketSource<String, KV<Iterable<GenericRecord>, Iterable<TableRow>>>
        sourceTransform =
            SortedBucketIO.read(String.class)
                .of(
                    AvroSortedBucketIO.avroSource(
                        new TupleTag<>(),
                        GR_USER_SCHEMA,
                        LocalResources.fromFile(source1Folder.getRoot(), true)))
                .and(
                    JsonSortedBucketIO.jsonSource(
                        new TupleTag<>(), LocalResources.fromFile(source2Folder.getRoot(), true)))
                .build();

    final PCollection<KV<String, KV<Iterable<GenericRecord>, Iterable<TableRow>>>> joinedSources =
        pipeline3.apply(sourceTransform);

    PAssert.that(joinedSources)
        .containsInAnyOrder(
            Arrays.asList(
                KV.of(
                    "a",
                    KV.of(
                        Collections.singletonList(createUserGR("a", 1)),
                        Collections.singletonList(createUserJson("a", "US")))),
                KV.of(
                    "b",
                    KV.of(
                        Collections.singletonList(createUserGR("b", 2)), Collections.emptyList())),
                KV.of(
                    "c",
                    KV.of(
                        Collections.singletonList(createUserGR("c", 3)),
                        Collections.singletonList(createUserJson("c", "DE")))),
                KV.of(
                    "d",
                    KV.of(
                        Collections.singletonList(createUserGR("d", 4)),
                        Collections.singletonList(createUserJson("d", "MX")))),
                KV.of(
                    "e",
                    KV.of(
                        Collections.singletonList(createUserGR("e", 5)),
                        Collections.singletonList(createUserJson("e", "AU")))),
                KV.of(
                    "f",
                    KV.of(
                        Collections.singletonList(createUserGR("f", 6)),
                        Collections.singletonList(createUserJson("f", "US")))),
                KV.of(
                    "g",
                    KV.of(
                        Collections.singletonList(createUserGR("g", 7)),
                        Collections.singletonList(createUserJson("g", "SE")))),
                KV.of(
                    "h",
                    KV.of(
                        Collections.singletonList(createUserGR("h", 8)),
                        Collections.singletonList(createUserJson("h", "DE")))),
                KV.of(
                    "i",
                    KV.of(
                        Collections.emptyList(),
                        Collections.singletonList(createUserJson("i", "MX"))))));

    pipeline3.run();
  }
}
