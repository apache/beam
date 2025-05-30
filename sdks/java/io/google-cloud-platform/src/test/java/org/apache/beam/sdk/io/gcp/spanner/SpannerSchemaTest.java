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
package org.apache.beam.sdk.io.gcp.spanner;

import static org.junit.Assert.assertEquals;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Type;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** A test of {@link SpannerSchema}. */
@RunWith(JUnit4.class)
public class SpannerSchemaTest {

  @Test
  public void testSingleTable() throws Exception {
    SpannerSchema schema =
        SpannerSchema.builder()
            .addColumn("test", "pk_0", "STRING(48)")
            .addKeyPart("test", "pk_0", false)
            .addColumn("test", "maxKey_1", "STRING(MAX)")
            .addColumn("test", "numericVal_2", "NUMERIC")
            .addColumn("test", "jsonVal_3", "JSON")
            .addColumn("test", "protoVal_4", "PROTO<customer.app.TestMessage>")
            .addColumn("test", "enumVal_5", "ENUM<customer.app.TestEnum>")
            .addColumn("test", "tokens_6", "TOKENLIST")
            .addColumn("test", "uuidCol_7", "UUID")
            .addColumn("test", "arrayVal_8", "ARRAY<FLOAT32>(vector_length=>256)")
            .addColumn("test", "sizedArrayVal_9", "ARRAY<STRING(MAX)>")
            .addColumn("test", "sizedByteVal_10", "ARRAY<BYTES(1024)>")
            .addColumn("test", "hexSizedByteVal_11", "ARRAY<BYTES(0x400)>")
            .addColumn("test", "arrayValue_12", "ARRAY<FLOAT32>")
            .build();

    assertEquals(1, schema.getTables().size());
    assertEquals(13, schema.getColumns("test").size());
    assertEquals(1, schema.getKeyParts("test").size());
    assertEquals(Type.numeric(), schema.getColumns("test").get(2).getType());
    assertEquals(Type.json(), schema.getColumns("test").get(3).getType());
    assertEquals(
        Type.proto("customer.app.TestMessage"), schema.getColumns("test").get(4).getType());
    assertEquals(
        Type.protoEnum("customer.app.TestEnum"), schema.getColumns("test").get(5).getType());
    assertEquals(Type.bytes(), schema.getColumns("test").get(6).getType());
    assertEquals(Type.string(), schema.getColumns("test").get(7).getType());
    assertEquals(Type.array(Type.float32()), schema.getColumns("test").get(8).getType());
    assertEquals(Type.array(Type.string()), schema.getColumns("test").get(9).getType());
    assertEquals(Type.array(Type.bytes()), schema.getColumns("test").get(10).getType());
    assertEquals(Type.array(Type.bytes()), schema.getColumns("test").get(11).getType());
    assertEquals(Type.array(Type.float32()), schema.getColumns("test").get(12).getType());
  }

  @Test
  public void testTwoTables() throws Exception {
    SpannerSchema schema =
        SpannerSchema.builder()
            .addColumn("test", "pk", "STRING(48)")
            .addKeyPart("test", "pk", false)
            .addColumn("test", "maxKey", "STRING(MAX)")
            .addColumn("other", "pk", "INT64")
            .addKeyPart("other", "pk", true)
            .addColumn("other", "maxKey", "STRING(MAX)")
            .build();

    assertEquals(2, schema.getTables().size());
    assertEquals(2, schema.getColumns("test").size());
    assertEquals(1, schema.getKeyParts("test").size());

    assertEquals(2, schema.getColumns("other").size());
    assertEquals(1, schema.getKeyParts("other").size());
  }

  @Test
  public void testSinglePgTable() throws Exception {
    SpannerSchema schema =
        SpannerSchema.builder(Dialect.POSTGRESQL)
            .addColumn("test", "pk", "character varying(48)")
            .addKeyPart("test", "pk", false)
            .addColumn("test", "maxKey", "character varying")
            .addColumn("test", "numericVal", "numeric")
            .addColumn("test", "commitTime", "spanner.commit_timestamp")
            .addColumn("test", "jsonbCol", "jsonb")
            .addColumn("test", "tokens", "spanner.tokenlist")
            .addColumn("test", "uuidCol", "uuid")
            .addColumn("test", "arrayCol", "DOUBLE PRECISION[]")
            .addColumn("test", "embeddingVectorCol", "DOUBLE PRECISION[] VECTOR LENGTH 16")
            .build();

    assertEquals(1, schema.getTables().size());
    assertEquals(9, schema.getColumns("test").size());
    assertEquals(1, schema.getKeyParts("test").size());
    assertEquals(Type.timestamp(), schema.getColumns("test").get(3).getType());
    assertEquals(Type.bytes(), schema.getColumns("test").get(5).getType());
    assertEquals(Type.string(), schema.getColumns("test").get(6).getType());
    assertEquals(Type.array(Type.float64()), schema.getColumns("test").get(7).getType());
    assertEquals(Type.array(Type.float64()), schema.getColumns("test").get(8).getType());
  }

  @Test
  public void testTwoPgTables() throws Exception {
    SpannerSchema schema =
        SpannerSchema.builder(Dialect.POSTGRESQL)
            .addColumn("test", "pk", "character varying(48)")
            .addKeyPart("test", "pk", false)
            .addColumn("test", "maxKey", "character varying")
            .addColumn("test", "jsonbCol", "jsonb")
            .addColumn("other", "pk", "bigint")
            .addKeyPart("other", "pk", true)
            .addColumn("other", "maxKey", "character varying")
            .addColumn("other", "commitTime", "spanner.commit_timestamp")
            .build();

    assertEquals(2, schema.getTables().size());
    assertEquals(3, schema.getColumns("test").size());
    assertEquals(1, schema.getKeyParts("test").size());

    assertEquals(3, schema.getColumns("other").size());
    assertEquals(1, schema.getKeyParts("other").size());
    assertEquals(Type.timestamp(), schema.getColumns("other").get(2).getType());
  }
}
