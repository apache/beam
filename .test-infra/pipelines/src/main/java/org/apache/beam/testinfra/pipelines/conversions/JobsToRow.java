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

import static org.apache.beam.sdk.values.TypeDescriptors.rows;

import com.google.dataflow.v1beta3.Job;
import java.util.Optional;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.WithFailures.Result;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.testinfra.pipelines.schemas.DescriptorSchemaRegistry;
import org.apache.beam.testinfra.pipelines.schemas.GeneratedMessageV3RowBuilder;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Throwables;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.joda.time.Instant;

/** {@link PTransform} that converts {@link Job}s to {@link Row}s. */
@Internal
public class JobsToRow
    extends PTransform<
        @NonNull PCollection<Job>, @NonNull RowConversionResult<Job, ConversionError>> {

  public static JobsToRow create() {
    return new JobsToRow();
  }

  private static final DescriptorSchemaRegistry SCHEMA_REGISTRY = DescriptorSchemaRegistry.INSTANCE;

  static {
    SCHEMA_REGISTRY.build(Job.getDescriptor());
  }

  @Override
  public @NonNull RowConversionResult<Job, ConversionError> expand(
      @NonNull PCollection<Job> input) {
    TupleTag<Row> success = new TupleTag<Row>() {};
    TupleTag<ConversionError> failure = new TupleTag<ConversionError>() {};
    Schema successSchema = SCHEMA_REGISTRY.getOrBuild(Job.getDescriptor());
    Result<@NonNull PCollection<Row>, ConversionError> result =
        input.apply(
            "Jobs To Row",
            MapElements.into(rows())
                .via(jobsToRowFn())
                .exceptionsInto(new TypeDescriptor<ConversionError>() {})
                .exceptionsVia(
                    error ->
                        ConversionError.builder()
                            .setObservedTime(Instant.now())
                            .setMessage(
                                Optional.ofNullable(error.exception().getMessage()).orElse(""))
                            .setStackTrace(Throwables.getStackTraceAsString(error.exception()))
                            .build()));

    PCollectionTuple pct =
        PCollectionTuple.of(success, result.output()).and(failure, result.failures());
    return new RowConversionResult<>(successSchema, success, failure, pct);
  }

  private static SerializableFunction<Job, Row> jobsToRowFn() {
    return job -> {
      GeneratedMessageV3RowBuilder<Job> builder = GeneratedMessageV3RowBuilder.of(job);
      return builder.build();
    };
  }
}
