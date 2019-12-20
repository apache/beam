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
package org.apache.beam.sdk.extensions.sql.meta.provider.datastore;

import static com.google.datastore.v1.client.DatastoreHelper.makeKey;
import static com.google.datastore.v1.client.DatastoreHelper.makeValue;
import static org.apache.beam.sdk.schemas.Schema.FieldType.BOOLEAN;
import static org.apache.beam.sdk.schemas.Schema.FieldType.DOUBLE;
import static org.apache.beam.sdk.schemas.Schema.FieldType.INT64;

import com.google.datastore.v1.Entity;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.extensions.sql.meta.provider.datastore.DataStoreV1Table.EntityToRowConverter;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DataStoreTableTest {

  private static final Schema SCHEMA =
      Schema.builder()
          .addNullableField("long", INT64)
          .addNullableField("bool", BOOLEAN)
          .addNullableField("double", DOUBLE)
          .addNullableField("string", CalciteUtils.CHAR)
          .build();
  private static final Entity ENTITY =
      Entity.newBuilder()
          .setKey(makeKey("key1"))
          .putProperties("long", makeValue(Long.MAX_VALUE).build())
          .putProperties("bool", makeValue(true).build())
          .putProperties("double", makeValue(Double.MAX_VALUE).build())
          .putProperties("string", makeValue("string").build())
          .build();
  @Rule public transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testEntityToRowConverter() {
    Row expected = row(SCHEMA, Long.MAX_VALUE, true, Double.MAX_VALUE, "string");

    PCollection<Row> result =
        pipeline.apply(Create.of(ENTITY)).apply(ParDo.of(EntityToRowConverter.create(SCHEMA)));
    PAssert.that(result).containsInAnyOrder(expected);

    pipeline.run().waitUntilFinish();
  }

  private Row row(Schema schema, Object... values) {
    return Row.withSchema(schema).addValues(values).build();
  }
}
