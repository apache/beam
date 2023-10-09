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
package org.apache.beam.testinfra.pipelines.conversions;

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.beam.sdk.values.TypeDescriptors.longs;
import static org.apache.beam.sdk.values.TypeDescriptors.strings;
import static org.apache.beam.testinfra.pipelines.conversions.WithAppendedDetailsToRow.JOB_CREATE_TIME;
import static org.apache.beam.testinfra.pipelines.conversions.WithAppendedDetailsToRow.JOB_ID_FIELD;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.GeneratedMessageV3;
import java.io.Serializable;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.testinfra.pipelines.schemas.DescriptorSchemaRegistry;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.joda.time.Instant;
import org.junit.jupiter.api.Test;

/** Base class for testing {@link WithAppendedDetailsToRow} transforms. */
abstract class WithAppendedDetailsToRowTest<AppendedDetailsT, EmbeddedT extends GeneratedMessageV3>
    implements Serializable {

  private static final DescriptorSchemaRegistry SCHEMA_REGISTRY = DescriptorSchemaRegistry.INSTANCE;

  abstract WithAppendedDetailsToRow<AppendedDetailsT, EmbeddedT> transform();

  abstract Descriptor embeddedTypeDescriptor();

  abstract String embeddedTypeFieldName();

  abstract Function<AppendedDetailsT, String> jobIdGetter();

  abstract Function<AppendedDetailsT, Instant> createTimeGetter();

  abstract @NonNull List<@NonNull AppendedDetailsT> input();

  @Test
  void emittedRowMatchesExpectedSchema() {
    Pipeline pipeline = Pipeline.create();

    PCollection<AppendedDetailsT> input = pipeline.apply(Create.of(input()));

    RowConversionResult<AppendedDetailsT, ConversionError> result = input.apply(transform());

    PAssert.thatSingleton(result.getFailure().apply(Count.globally())).isEqualTo(0L);

    Schema actualSchema = result.getSuccess().getSchema();

    assertEquals(expectedSchema(), actualSchema);

    pipeline.run();
  }

  @Test
  void emittedRowsMatchesJobIds() {
    Pipeline pipeline = Pipeline.create();

    PCollection<AppendedDetailsT> input = pipeline.apply(Create.of(input()));

    RowConversionResult<AppendedDetailsT, ConversionError> result = input.apply(transform());

    PAssert.thatSingleton(result.getFailure().apply(Count.globally())).isEqualTo(0L);

    List<String> expectedJobIds = input().stream().map(jobIdGetter()).collect(Collectors.toList());

    PAssert.that(jobIdsFrom(result)).containsInAnyOrder(expectedJobIds);

    pipeline.run();
  }

  @Test
  void emittedRowsMatchesCreateTimes() {
    Pipeline pipeline = Pipeline.create();

    PCollection<AppendedDetailsT> input = pipeline.apply(Create.of(input()));

    RowConversionResult<AppendedDetailsT, ConversionError> result = input.apply(transform());

    PAssert.thatSingleton(result.getFailure().apply(Count.globally())).isEqualTo(0L);

    List<Long> expectedCreateTimes =
        input().stream()
            .map(createTimeGetter())
            .map(Instant::getMillis)
            .collect(Collectors.toList());

    PAssert.that(createTimesFrom(result)).containsInAnyOrder(expectedCreateTimes);

    pipeline.run();
  }

  protected @NonNull Schema expectedEmbeddedSchema() {
    return SCHEMA_REGISTRY.getOrBuild(embeddedTypeDescriptor());
  }

  private @NonNull Schema expectedSchema() {
    Schema embeddedSchema = expectedEmbeddedSchema();
    return Schema.of(
        JOB_ID_FIELD,
        JOB_CREATE_TIME,
        Schema.Field.of(embeddedTypeFieldName(), Schema.FieldType.row(embeddedSchema)));
  }

  private PCollection<String> jobIdsFrom(
      RowConversionResult<AppendedDetailsT, ConversionError> result) {
    return result
        .getSuccess()
        .apply(
            "get job ids",
            MapElements.into(strings())
                .via(row -> checkStateNotNull(row).getString(JOB_ID_FIELD.getName())));
  }

  private PCollection<Long> createTimesFrom(
      RowConversionResult<AppendedDetailsT, ConversionError> result) {
    return result
        .getSuccess()
        .apply(
            "get create times",
            MapElements.into(longs())
                .via(
                    row ->
                        checkStateNotNull(row)
                            .getLogicalTypeValue(JOB_CREATE_TIME.getName(), Instant.class)
                            .getMillis()));
  }
}
