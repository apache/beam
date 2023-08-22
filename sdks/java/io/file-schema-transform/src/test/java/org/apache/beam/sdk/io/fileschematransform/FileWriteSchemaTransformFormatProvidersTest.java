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
package org.apache.beam.sdk.io.fileschematransform;

import static org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformFormatProviders.AVRO;
import static org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformFormatProviders.CSV;
import static org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformFormatProviders.JSON;
import static org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformFormatProviders.PARQUET;
import static org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformFormatProviders.XML;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformFormatProviders.BeamRowMapperWithDlq;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link FileWriteSchemaTransformFormatProviders}. */
@RunWith(JUnit4.class)
public class FileWriteSchemaTransformFormatProvidersTest {
  @Rule public TestPipeline p = TestPipeline.create();

  @Test
  public void loadProviders() {
    Map<String, FileWriteSchemaTransformFormatProvider> formatProviderMap =
        FileWriteSchemaTransformFormatProviders.loadProviders();
    Set<String> keys = formatProviderMap.keySet();
    assertEquals(ImmutableSet.of(AVRO, CSV, JSON, PARQUET, XML), keys);
  }

  @Test
  public void testErrorCounterSuccess() {
    SerializableFunction<Row, String> mapFn = new SimpleMapFn(false);
    List<String> records = Arrays.asList(NO_DLQ_MSG, NO_DLQ_MSG, NO_DLQ_MSG);

    PCollection<Row> input = p.apply(Create.of(ROWS));
    PCollectionTuple output =
        input.apply(
            ParDo.of(
                    new BeamRowMapperWithDlq<String>(
                        "Generic-write-error-counter", mapFn, OUTPUT_TAG))
                .withOutputTags(OUTPUT_TAG, TupleTagList.of(ERROR_TAG)));
    output.get(ERROR_TAG).setRowSchema(ERROR_SCHEMA);
    PAssert.that(output.get(OUTPUT_TAG)).containsInAnyOrder(records);
    p.run().waitUntilFinish();
  }

  @Test
  public void testErrorCounterDlqSuccess() {
    SerializableFunction<Row, String> mapFn = new SimpleMapFn(true);

    PCollection<Row> input = p.apply(Create.of(ROWS));
    PCollectionTuple output =
        input.apply(
            ParDo.of(
                    new BeamRowMapperWithDlq<String>(
                        "Generic-write-error-counter", mapFn, OUTPUT_TAG))
                .withOutputTags(OUTPUT_TAG, TupleTagList.of(ERROR_TAG)));
    output.get(ERROR_TAG).setRowSchema(ERROR_SCHEMA);
    PCollection<Long> count = output.get(ERROR_TAG).apply(Count.globally());
    PAssert.that(count).containsInAnyOrder(Collections.singletonList(3L));
    p.run().waitUntilFinish();
  }

  private static class SimpleMapFn implements SerializableFunction<Row, String> {
    private boolean useDlq;

    SimpleMapFn(boolean useDlq) {
      this.useDlq = useDlq;
    }

    @Override
    public String apply(Row row) {
      if (useDlq) {
        throw new IllegalArgumentException(DLQ_MSG);
      } else {
        return NO_DLQ_MSG;
      }
    }
  }

  private static final TupleTag<String> OUTPUT_TAG = new TupleTag<String>() {};
  private static final TupleTag<Row> ERROR_TAG = FileWriteSchemaTransformProvider.ERROR_TAG;

  private static final Schema BEAM_SCHEMA =
      Schema.of(Schema.Field.of("name", Schema.FieldType.STRING));
  private static final Schema ERROR_SCHEMA = FileWriteSchemaTransformProvider.ERROR_SCHEMA;

  private static final List<Row> ROWS =
      Arrays.asList(
          Row.withSchema(BEAM_SCHEMA).withFieldValue("name", "a").build(),
          Row.withSchema(BEAM_SCHEMA).withFieldValue("name", "b").build(),
          Row.withSchema(BEAM_SCHEMA).withFieldValue("name", "c").build());

  private static final String DLQ_MSG = "Testing DLQ behavior";
  private static final String NO_DLQ_MSG = "Testing without DLQ";
}
