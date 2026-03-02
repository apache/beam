package org.apache.beam.examples.snippets.transforms.io.iceberg;

import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.managed.Managed;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.Map;

public class Quickstart {
  public static void main(String[] args) {
    // [START hadoop_catalog_props]
    Map<String, String> catalogProps = ImmutableMap.of(
      "type", "hadoop",
      "warehouse", "file://tmp/beam-iceberg-local-quickstart"
    );
    // [END hadoop_catalog_props]
  }
  public static void other() {
    // [START biglake_catalog_props]
    Map<String, String> catalogProps = ImmutableMap.of(
      "type", "rest",
      "uri", "https://biglake.googleapis.com/iceberg/v1/restcatalog",
      "warehouse", "gs://biglake-public-nyc-taxi-iceberg",
      "header.x-goog-user-project", "$PROJECT_ID",
      "rest.auth.type", "google",
      "io-impl", "org.apache.iceberg.gcp.gcs.GCSFileIO",
      "header.X-Iceberg-Access-Delegation", "vended-credentials"
    );
    // [END biglake_catalog_props]

    // [START managed_iceberg_config]
    Map<String, Object> managedConfig = ImmutableMap.of(
      "table", "my_db.my_table",
      "catalog_properties", catalogProps
    );
    // Note: The table will get created when inserting data (see below)
    // [END managed_iceberg_config]

    // [START managed_iceberg_insert]
    Schema inputSchema = Schema.builder()
      .addInt64Field("id")
      .addStringField("name")
      .addInt32Field("age")
      .build();

    Pipeline p = Pipeline.create();
    p
      .apply(Create.of(
          Row.withSchema(inputSchema).addValues(1, "Mark", 34).build(),
          Row.withSchema(inputSchema).addValues(2, "Omar", 24).build(),
          Row.withSchema(inputSchema).addValues(3, "Rachel", 27).build()))
      .apply(Managed.write("iceberg").withConfig(managedConfig));

    p.run();
    // [END managed_iceberg_insert]

    // [START managed_iceberg_read]
    Pipeline q = Pipeline.create();
    PCollection<Row> rows = q
      .apply(Managed.read("iceberg").withConfig(managedConfig))
      .getSinglePCollection();

    rows
      .apply(MapElements.into(TypeDescriptors.voids()).via(row -> {
        System.out.println(row);
        return null;
      }));

    q.run();
    // [END managed_iceberg_read]
  }
}
