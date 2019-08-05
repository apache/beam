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
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.smb.BucketMetadata.HashType;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

/** E2E test for heterogeneously-typed SMB join. */
public class MixedSourcesEndToEndTest {
  @Rule public final TestPipeline pipeline1 = TestPipeline.create();
  @Rule public final TestPipeline pipeline2 = TestPipeline.create();
  @Rule public final TestPipeline pipeline3 = TestPipeline.create();
  @Rule public final TemporaryFolder sourceFolder1 = new TemporaryFolder();
  @Rule public final TemporaryFolder sourceFolder2 = new TemporaryFolder();

  @Rule public final TemporaryFolder tmpFolder1 = new TemporaryFolder();
  @Rule public final TemporaryFolder tmpFolder2 = new TemporaryFolder();

  private static final Schema GR_USER_SCHEMA =
      Schema.createRecord(
          "user",
          "",
          "org.apache.beam.sdk.extensions.smb",
          false,
          Arrays.asList(
              new Field(
                  "name",
                  Schema.createUnion(
                      Arrays.asList(Schema.create(Type.NULL), Schema.create(Type.BYTES))),
                  "",
                  ""),
              new Field("age", Schema.create(Type.INT), "", -1)));

  private static GenericRecord createUserGR(String name, int age) {
    GenericData.Record result = new GenericData.Record(GR_USER_SCHEMA);
    result.put(
        "name",
        Optional.ofNullable(name)
            .map(n -> ByteBuffer.wrap(ByteString.copyFromUtf8(n).toByteArray()))
            .orElse(null));
    result.put("age", age);

    return result;
  }

  private static TableRow createUserJson(String name, String country) {
    return new TableRow().set("name", name).set("country", country);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testE2E() throws Exception {
    pipeline1
        .apply(
            Create.of(
                    createUserGR("a", 1),
                    createUserGR("b", 2),
                    createUserGR("c", 3),
                    createUserGR("d", 4),
                    createUserGR("e", 5),
                    createUserGR("f", 6),
                    createUserGR("g", 7),
                    createUserGR(null, 7),
                    createUserGR("h", 8))
                .withCoder(AvroCoder.of(GR_USER_SCHEMA)))
        .apply(
            AvroSortedBucketIO.write(ByteBuffer.class, "name", GR_USER_SCHEMA)
                .to(sourceFolder1.getRoot().getPath())
                .withTempDirectory(tmpFolder1.getRoot().getPath())
                .withNumBuckets(8)
                .withNumShards(4)
                .withHashType(HashType.MURMUR3_32)
                .withSuffix(".avro")
                .withCodec(CodecFactory.snappyCodec()));

    pipeline1.run().waitUntilFinish();

    pipeline2
        .apply(
            Create.of(
                    createUserJson("a", "US"),
                    createUserJson("c", "DE"),
                    createUserJson("d", "MX"),
                    createUserJson("e", "AU"),
                    createUserJson("f", "US"),
                    createUserJson("g", "SE"),
                    createUserJson(null, "SE"),
                    createUserJson("h", "DE"),
                    createUserJson("i", "MX"))
                .withCoder(TableRowJsonCoder.of()))
        .apply(
            JsonSortedBucketIO.write(String.class, "name")
                .to(sourceFolder2.getRoot().getPath())
                .withTempDirectory(tmpFolder2.getRoot().getPath())
                .withNumBuckets(8)
                .withNumShards(4)
                .withHashType(HashType.MURMUR3_32)
                .withSuffix(".json")
                .withCompression(Compression.UNCOMPRESSED));

    pipeline2.run().waitUntilFinish();

    TupleTag<GenericRecord> lhsTag = new TupleTag<>();
    TupleTag<TableRow> rhsTag = new TupleTag<>();

    final SortedBucketIO.CoGbk<String> sourceTransform =
        SortedBucketIO.read(String.class)
            .of(
                AvroSortedBucketIO.read(lhsTag, GR_USER_SCHEMA)
                    .from(sourceFolder1.getRoot().getPath()))
            .and(JsonSortedBucketIO.read(rhsTag).from(sourceFolder2.getRoot().getPath()));

    final PCollection<KV<String, KV<Iterable<GenericRecord>, Iterable<TableRow>>>> joinedSources =
        pipeline3
            .apply(sourceTransform)
            .apply(ParDo.of(new ExpandResult(lhsTag, rhsTag)))
            .setCoder(
                KvCoder.of(
                    StringUtf8Coder.of(),
                    KvCoder.of(
                        IterableCoder.of(AvroCoder.of(GR_USER_SCHEMA)),
                        IterableCoder.of(TableRowJsonCoder.of()))));

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

  private static class ExpandResult
      extends DoFn<
          KV<String, CoGbkResult>, KV<String, KV<Iterable<GenericRecord>, Iterable<TableRow>>>> {
    private final TupleTag<GenericRecord> lhsTag;
    private final TupleTag<TableRow> rhsTag;

    private ExpandResult(TupleTag<GenericRecord> lhsTag, TupleTag<TableRow> rhsTag) {
      this.lhsTag = lhsTag;
      this.rhsTag = rhsTag;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      final KV<String, CoGbkResult> kv = c.element();
      final CoGbkResult result = kv.getValue();
      c.output(KV.of(kv.getKey(), KV.of(result.getAll(lhsTag), result.getAll(rhsTag))));
    }
  }
}
