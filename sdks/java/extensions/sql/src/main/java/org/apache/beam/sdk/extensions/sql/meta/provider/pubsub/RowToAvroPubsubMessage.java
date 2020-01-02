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
package org.apache.beam.sdk.extensions.sql.meta.provider.pubsub;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import java.util.stream.Collectors;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.extensions.sql.meta.provider.avro.GenericRecordWriteConverter;
import org.apache.beam.sdk.extensions.sql.meta.provider.pubsub.PubsubTableProvider.PubsubIOTableConfiguration;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.DropFields;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_20_0.com.google.common.collect.ImmutableMap;

/**
 * A {@link PTransform} to convert {@link Row} to {@link PubsubMessage}.
 *
 * <p>Currently only supports writing a flat schema into a AVRO payload.
 */
@Experimental
@Internal
public class RowToAvroPubsubMessage
    extends PTransform<PCollection<Row>, PCollection<PubsubMessage>> {
  private final PubsubIOTableConfiguration config;

  private static final String EVENT_TIMESTAMP = "event_timestamp";

  private RowToAvroPubsubMessage(PubsubIOTableConfiguration config) {
    checkArgument(
        config.getUseFlatSchema(), "RowToAvroPubsubMessage only supports flattened schemas.");

    this.config = config;
  }

  public static RowToAvroPubsubMessage fromTableConfig(PubsubIOTableConfiguration config) {
    return new RowToAvroPubsubMessage(config);
  }

  @Override
  public PCollection<PubsubMessage> expand(PCollection<Row> input) {
    PCollection<Row> withTimestamp =
        (config.useTimestampAttribute())
            ? input.apply(WithTimestamps.of((row) -> row.getDateTime(EVENT_TIMESTAMP).toInstant()))
            : input;

    Schema payloadSchema =
        new Schema(
            config.getSchema().getFields().stream()
                .filter(field -> !EVENT_TIMESTAMP.equals(field.getName()))
                .collect(Collectors.toList()));

    return withTimestamp
        .apply(DropFields.fields(EVENT_TIMESTAMP))
        .apply(GenericRecordWriteConverter.builder().beamSchema(payloadSchema).build())
        .apply(
            ParDo.of(
                new DoFn<GenericRecord, PubsubMessage>() {
                  @ProcessElement
                  public void processElement(ProcessContext context) {
                    context.output(
                        new PubsubMessage(
                            AvroUtils.toBytes(context.element()),
                            ImmutableMap.of(),
                            config.useTimestampAttribute()
                                ? context.element().get(EVENT_TIMESTAMP).toString()
                                : null));
                  }
                }));
  }
}
