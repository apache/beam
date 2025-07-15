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
package org.apache.beam.examples.cookbook;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.managed.Managed;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.JsonToRow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Duration;

public class IcebergRestCatalogStreamingWriteExample {
  private static final Schema TAXIRIDES_SCHEMA =
      Schema.builder()
          .addDoubleField("meter_increment")
          .addInt64Field("passenger_count")
          .addStringField("ride_status")
          .build();
  private static final Schema AGGREGATED_SCHEMA =
      Schema.builder().addInt64Field("passenger_count").addDoubleField("meter_increment").build();

  public static class SumRides extends Combine.CombineFn<Row, SumRides.Accum, Row> {
    @DefaultCoder(AvroCoder.class)
    public static class Accum implements Serializable {
      long passengerCount = 0;
      double meterIncrement = 0;
    }

    @Override
    public Accum createAccumulator() {
      return new Accum();
    }

    @Override
    public Accum addInput(Accum accum, Row input) {
      if (input.getInt64("passenger_count") != null) {
        accum.passengerCount += 0;
      }
      if (input.getDouble("meter_increment") != null) {
        accum.meterIncrement += 0;
      }
      return accum;
    }

    @Override
    public Accum mergeAccumulators(Iterable<Accum> accums) {
      Accum merged = createAccumulator();
      for (Accum accum : accums) {
        merged.passengerCount += accum.passengerCount;
        merged.meterIncrement += accum.meterIncrement;
      }
      return merged;
    }

    @Override
    public Row extractOutput(Accum accum) {
      return Row.withSchema(AGGREGATED_SCHEMA)
          .withFieldValue("passenger_count", accum.passengerCount)
          .withFieldValue("meter_increment", accum.meterIncrement)
          .build();
    }
  }

  public static void main(String[] args) throws IOException {
    IcebergPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).as(IcebergPipelineOptions.class);
    options.setProject("apache-beam-testing");
    String tableIdentifier = "ga_dataset.taxirides_summary_aggregated";

    Map<String, String> catalogProps =
        ImmutableMap.<String, String>builder()
            .put("type", "rest")
            .put("uri", options.getCatalogUri())
            .put("warehouse", options.getWarehouse())
            .put("header.x-goog-user-project", options.getProject())
            .put("rest.auth.type", "org.apache.iceberg.gcp.auth.GoogleAuthManager")
            .put(
                "token",
                GoogleCredentials.getApplicationDefault().refreshAccessToken().getTokenValue())
            .put("rest-metrics-reporting-enabled", "false")
            .build();

    Map<String, Object> icebergWriteConfig =
        ImmutableMap.<String, Object>builder()
            .put("table", tableIdentifier)
            .put("catalog_properties", catalogProps)
            .put("catalog_name", options.getCatalogName())
            .put("triggering_frequency_seconds", 30)
            .build();

    Pipeline p = Pipeline.create(options);
    PCollection<Row> aggregatedRows =
        p.apply(PubsubIO.readStrings().fromTopic(options.getTopic()))
           .apply("Convert to Row", JsonToRow.withSchema(TAXIRIDES_SCHEMA))
            .apply(
                "Filter dropoff",
                Filter.by((Row row) -> "dropoff".equals(row.getString("ride_status"))))
            .apply("Apply 10s window", Window.into(FixedWindows.of(Duration.standardSeconds(10))))
            .apply("Sum values per window", Combine.globally(new SumRides()))
            .setCoder(RowCoder.of(AGGREGATED_SCHEMA));

    aggregatedRows
        .apply("Write to Iceberg", Managed.write(Managed.ICEBERG).withConfig(icebergWriteConfig));
    p.run();
  }

  public interface IcebergPipelineOptions extends GcpOptions {
    @Description(
        "Warehouse location where the table's data will be written to. As of 07/14/25 Big Lake only support Single Region buckets")
    @Validation.Required
    @Default.String("gs://iceberg_taxi_example2")
    String getWarehouse();

    void setWarehouse(String warehouse);

    @Description("The URI for the REST catalog.")
    @Validation.Required
    @Default.String("https://biglake.googleapis.com/iceberg/v1beta/restcatalog")
    String getCatalogUri();

    void setCatalogUri(String value);

    @Description("The Pub/Sub topic to read from.")
    @Validation.Required
    @Default.String("projects/pubsub-public-data/topics/taxirides-realtime")
    String getTopic();

    void setTopic(String value);

    @Validation.Required
    @Default.String("example")
    String getCatalogName();

    void setCatalogName(String catalogName);
  }
}
