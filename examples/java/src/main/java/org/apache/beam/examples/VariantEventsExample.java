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

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.logicaltypes.SqlTypes;
import org.apache.beam.sdk.schemas.variant.Variant;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.JsonToRow;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Processes a semi-structured event stream with the Variant type.
 *
 * <p>Every event has a stable core ({@code id}, {@code user}, {@code event}) and a {@code payload}
 * whose shape depends on the event type and changes over time. The payload is kept as one {@link
 * Variant} column, so new payload fields need no schema change, and typed values are extracted only
 * where they are needed.
 *
 * <p>This example computes total purchase revenue per user. The purchase amount is read from the
 * payload as an exact {@code DECIMAL}; payloads without a numeric amount are skipped.
 *
 * <pre>{@code
 * {"id": 1, "user": "amy",  "event": "login",    "payload": {"device": {"os": "ios"}}}
 * {"id": 2, "user": "ali",  "event": "purchase", "payload": {"amount": 19.99, "items": ["sku-1"]}}
 * {"id": 5, "user": "ali",  "event": "purchase", "payload": {"amount": 5.01, "coupon": "SAVE5"}}
 *
 * => ali,25.00
 * }</pre>
 *
 * <p>To run with the built-in sample events:
 *
 * <pre>{@code
 * ./gradlew :examples:java:execute -PmainClass=org.apache.beam.examples.VariantEventsExample
 * }</pre>
 *
 * <p>Pass {@code --inputFile} (JSON lines) and {@code --output} to read and write files instead.
 */
public class VariantEventsExample {

  /** Schema of each event: a typed core plus a Variant payload. */
  public static final Schema EVENT_SCHEMA =
      Schema.builder()
          .addInt64Field("id")
          .addStringField("user")
          .addStringField("event")
          .addLogicalTypeField("payload", SqlTypes.VARIANT)
          .build();

  static final List<String> SAMPLE_EVENTS =
      Arrays.asList(
          "{\"id\": 1, \"user\": \"amy\", \"event\": \"login\","
              + " \"payload\": {\"device\": {\"os\": \"ios\"}}}",
          "{\"id\": 2, \"user\": \"ali\", \"event\": \"purchase\","
              + " \"payload\": {\"amount\": 19.99, \"items\": [\"sku-1\", \"sku-2\"]}}",
          "{\"id\": 3, \"user\": \"john\", \"event\": \"error\","
              + " \"payload\": {\"code\": 503, \"trace\": {\"svc\": \"cart\"}}}",
          "{\"id\": 4, \"user\": \"amy\", \"event\": \"purchase\","
              + " \"payload\": {\"amount\": 7.50, \"items\": [\"sku-3\"]}}",
          // A field added by a newer producer flows through without a schema change.
          "{\"id\": 5, \"user\": \"ali\", \"event\": \"purchase\","
              + " \"payload\": {\"amount\": 5.01, \"coupon\": \"SAVE5\"}}",
          // A malformed amount is skipped instead of failing the pipeline.
          "{\"id\": 6, \"user\": \"john\", \"event\": \"purchase\","
              + " \"payload\": {\"amount\": \"n/a\"}}");

  /** Parses JSON events and sums the purchase {@code amount} per user. */
  public static class RevenuePerUser
      extends PTransform<PCollection<String>, PCollection<KV<String, BigDecimal>>> {
    @Override
    public PCollection<KV<String, BigDecimal>> expand(PCollection<String> jsonLines) {
      return jsonLines
          .apply("ParseEvents", JsonToRow.withSchema(EVENT_SCHEMA))
          .apply("Purchases", Filter.by((Row event) -> "purchase".equals(event.getString("event"))))
          .apply(
              "ExtractAmount",
              FlatMapElements.into(
                      TypeDescriptors.kvs(
                          TypeDescriptors.strings(), TypeDescriptor.of(BigDecimal.class)))
                  .via(VariantEventsExample::amountOf))
          .apply("SumPerUser", Combine.perKey(VariantEventsExample::sum));
    }
  }

  /** Reads {@code payload.amount} as an exact decimal; skips events without a numeric amount. */
  static List<KV<String, BigDecimal>> amountOf(Row event) {
    Variant payload = event.getLogicalTypeValue("payload", Variant.class);
    @Nullable BigDecimal amount =
        payload == null ? null : payload.tryGet("$.amount", FieldType.DECIMAL);
    String user = event.getString("user");
    if (amount == null || user == null) {
      return Arrays.asList();
    }
    return Arrays.asList(KV.of(user, amount));
  }

  static BigDecimal sum(Iterable<BigDecimal> amounts) {
    BigDecimal total = BigDecimal.ZERO;
    for (BigDecimal amount : amounts) {
      total = total.add(amount);
    }
    return total;
  }

  /** Options for {@link VariantEventsExample}. */
  public interface Options extends PipelineOptions {
    @Description("JSON-lines file of events. Uses built-in sample events if not set.")
    @Default.String("")
    String getInputFile();

    void setInputFile(String value);

    @Description("Output file prefix. Logs results to stdout if not set.")
    @Default.String("")
    String getOutput();

    void setOutput(String value);
  }

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    Pipeline pipeline = Pipeline.create(options);

    PCollection<String> events =
        options.getInputFile().isEmpty()
            ? pipeline.apply("SampleEvents", Create.of(SAMPLE_EVENTS))
            : pipeline.apply("ReadEvents", TextIO.read().from(options.getInputFile()));

    PCollection<String> lines =
        events
            .apply("RevenuePerUser", new RevenuePerUser())
            .apply(
                "Format",
                MapElements.into(TypeDescriptors.strings())
                    .via(kv -> kv.getKey() + "," + kv.getValue().toPlainString()));

    if (options.getOutput().isEmpty()) {
      lines.apply(
          "Print",
          MapElements.into(TypeDescriptors.strings())
              .via(
                  line -> {
                    System.out.println(line);
                    return line;
                  }));
    } else {
      lines.apply("WriteResults", TextIO.write().to(options.getOutput()));
    }
    pipeline.run().waitUntilFinish();
  }
}
