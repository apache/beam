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
package org.apache.beam.examples;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.logicaltypes.SqlTypes;
import org.apache.beam.sdk.schemas.variant.Variant;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.JsonToRow;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ToJson;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads the same semi-structured JSON events twice: once with a strict schema and once with a
 * {@link Variant} payload.
 *
 * <p>The input has two common problems of evolving data:
 *
 * <ul>
 *   <li>event 2 has a field the schema doesn't know about ({@code coupon}), and
 *   <li>event 3 has a value of an unexpected type ({@code "amount": "n/a"}).
 * </ul>
 *
 * <p>With the strict schema, {@code coupon} is silently dropped and event 3 fails to parse. With a
 * Variant payload, all three events are kept with their full payload, and a typed value is
 * extracted only where it is needed: {@code tryGet} returns {@code null} for the bad amount instead
 * of losing the whole record.
 *
 * <pre>
 * Strict schema
 *   parsed: {"id":1,"payload":{"amount":19.99}}
 *   parsed: {"id":2,"payload":{"amount":5.01}}                    &lt;- coupon dropped
 *   failed: {"id": 3, "payload": {"amount": "n/a"}}               &lt;- record lost
 * Variant payload
 *   parsed: {"id":1,"payload":{"amount":19.99}}  amount=19.99
 *   parsed: {"id":2,"payload":{"amount":5.01,"coupon":"SAVE5"}}  amount=5.01
 *   parsed: {"id":3,"payload":{"amount":"n/a"}}  amount=null
 * </pre>
 */
public class VariantExample {
  private static final Logger LOG = LoggerFactory.getLogger(VariantExample.class);

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
    Pipeline pipeline = Pipeline.create(options);

    PCollection<String> events =
        pipeline.apply(
            Create.of(
                "{\"id\": 1, \"payload\": {\"amount\": 19.99}}",
                "{\"id\": 2, \"payload\": {\"amount\": 5.01, \"coupon\": \"SAVE5\"}}",
                "{\"id\": 3, \"payload\": {\"amount\": \"n/a\"}}"));

    // [START strict_schema]
    // Every payload field must be declared up front.
    Schema strictSchema =
        Schema.builder()
            .addInt64Field("id")
            .addRowField("payload", Schema.builder().addDecimalField("amount").build())
            .build();

    JsonToRow.ParseResult strict =
        events.apply("StrictParse", JsonToRow.withExceptionReporting(strictSchema));
    strict
        .getResults()
        .apply("StrictToJson", ToJson.of())
        .apply("LogStrictParsed", ParDo.of(new Log("strict  parsed: ")));
    strict
        .getFailedToParseLines()
        .apply(
            "FailedLine",
            MapElements.into(TypeDescriptors.strings())
                .via((Row failure) -> String.valueOf(failure.getString("line"))))
        .apply("LogStrictFailed", ParDo.of(new Log("strict  failed: ")));
    // [END strict_schema]

    // [START variant_schema]
    // The payload is one Variant column: any shape is accepted and kept as-is.
    Schema variantSchema =
        Schema.builder()
            .addInt64Field("id")
            .addLogicalTypeField("payload", SqlTypes.VARIANT)
            .build();

    events
        .apply("VariantParse", JsonToRow.withSchema(variantSchema))
        .apply(
            "ExtractAmount",
            ParDo.of(
                new DoFn<Row, String>() {
                  @ProcessElement
                  public void process(@Element Row event, OutputReceiver<String> out) {
                    Variant payload = event.getLogicalTypeValue("payload", Variant.class);
                    // Typed extraction: null (not a failure) when the value isn't a number.
                    Object amount = payload.tryGet("$.amount", FieldType.DECIMAL);
                    out.output(
                        "{\"id\":"
                            + event.getInt64("id")
                            + ",\"payload\":"
                            + payload.toJson()
                            + "}  amount="
                            + amount);
                  }
                }))
        .apply("LogVariant", ParDo.of(new Log("variant parsed: ")));
    // [END variant_schema]

    pipeline.run().waitUntilFinish();
  }

  /** Logs each element with a prefix. */
  static class Log extends DoFn<String, Void> {
    private final String prefix;

    Log(String prefix) {
      this.prefix = prefix;
    }

    @ProcessElement
    public void process(@Element String element) {
      LOG.info("{}{}", prefix, element);
    }
  }
}
