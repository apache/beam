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
package org.apache.beam.examples.iceberg;

import static java.lang.String.format;

import com.google.auth.oauth2.GoogleCredentials;
import java.io.IOException;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;

/**
 * This is a streaming pipeline that demonstrates how to write data to an Iceberg table using Beam
 * SQL. It reads real-time NYC taxi ride information from Pub/Sub, processes the data using a SQL
 * query, and writes the results to a single Iceberg table.
 *
 * <p>This example is a demonstration of the Iceberg REST Catalog. For more information, see the
 * documentation at {@link https://cloud.google.com/bigquery/docs/blms-rest-catalog}.
 */
public class IcebergRestCatalogStreamingWriteSqlExample {

  public static final String TAXI_RIDES_TOPIC =
      "projects/pubsub-public-data/topics/taxirides-realtime";

  /**
   * The main entry point for the pipeline.
   *
   * @param args Command-line arguments
   * @throws IOException If there is an issue with Google Credentials
   */
  public static void main(String[] args) throws IOException {
    IcebergPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(IcebergPipelineOptions.class);

    final String table = options.getIcebergTable();
    final String database = options.getIcebergDatabase();
    final String tableIdentifier = database + "." + table;
    final String catalogUri = options.getCatalogUri();
    final String warehouseLocation = options.getWarehouse();
    final String projectName = options.getProject();
    final String catalogName = options.getCatalogName();
    final int triggeringFrequencySeconds = 30;
    final String token =
        GoogleCredentials.getApplicationDefault().refreshAccessToken().getTokenValue();

    // Note: The token expires in 1 hour. Users may need to re-run the pipeline.
    // Future updates to Iceberg and the BigLake Metastore will support token refreshing.
    String createCatalogDdl =
        format(
            "CREATE CATALOG %s "
                + "TYPE iceberg "
                + "PROPERTIES ("
                + "'type'='rest', "
                + "'uri'='%s', "
                + "'warehouse'='%s', "
                + "'header.x-goog-user-project'='%s', "
                + "'oauth2-server-uri'='https://oauth2.googleapis.com/token', "
                + "'token'='%s', "
                + "'rest-metrics-reporting-enabled'='false'"
                + ")",
            catalogName, catalogUri, warehouseLocation, projectName, token);

    String setCatalogDdl = format("USE CATALOG %s", catalogName);

    String createDatabaseDdl = format("CREATE DATABASE IF NOT EXISTS %s", database);

    String pubsubTableDdl =
        format(
            "CREATE EXTERNAL TABLE taxirides (\n"
                + "  ride_id VARCHAR,\n"
                + "  ride_status VARCHAR,\n"
                + "  `event_timestamp` TIMESTAMP,\n"
                + "  passenger_count BIGINT,\n"
                + "  meter_reading DOUBLE\n"
                + ") \n"
                + "TYPE 'pubsub' \n"
                + "LOCATION '%s'",
            TAXI_RIDES_TOPIC);

    String icebergTableDdl =
        format(
            "CREATE EXTERNAL TABLE %s (\n"
                + "  ride_minute TIMESTAMP,\n"
                + "  total_rides BIGINT,\n"
                + "  revenue DOUBLE,\n"
                + "  passenger_count BIGINT\n"
                + ") \n"
                + "TYPE 'iceberg' \n"
                + "LOCATION '%s' \n"
                + "TBLPROPERTIES '{ \"triggering_frequency_seconds\" : %d }'",
            table, tableIdentifier, triggeringFrequencySeconds);

    String insertStatement =
        format(
            "INSERT INTO %s \n"
                + "SELECT \n"
                + "  TUMBLE_START(`event_timestamp`, INTERVAL '60' SECOND) AS ride_minute, \n"
                + "  COUNT(*) AS total_rides, \n"
                + "  SUM(meter_reading) AS revenue, \n"
                + "  SUM(passenger_count) AS passenger_count \n"
                + "FROM taxirides \n"
                + "WHERE ride_status = 'dropoff' \n"
                + "GROUP BY TUMBLE(`event_timestamp`, INTERVAL '60' SECOND)",
            table);

    Pipeline p = Pipeline.create(options);

    p.apply(
        "ExecuteSQL",
        SqlTransform.query(insertStatement)
            .withDdlString(createCatalogDdl)
            .withDdlString(setCatalogDdl)
            .withDdlString(createDatabaseDdl)
            .withDdlString(pubsubTableDdl)
            .withDdlString(icebergTableDdl));

    p.run().waitUntilFinish();
  }

  /** Pipeline options for the IcebergRestCatalogStreamingWriteSqlExample. */
  public interface IcebergPipelineOptions extends GcpOptions {
    @Description(
        "Warehouse location where the table's data will be written to. "
            + "As of 07/14/25 BigLake only supports Single Region buckets")
    @Validation.Required
    @Default.String("gs://biglake_taxi_ride_metrics")
    String getWarehouse();

    void setWarehouse(String warehouse);

    @Description("The URI for the REST catalog.")
    @Validation.Required
    @Default.String("https://biglake.googleapis.com/iceberg/v1beta/restcatalog")
    String getCatalogUri();

    void setCatalogUri(String value);

    @Description("The iceberg table to write to.")
    @Validation.Required
    @Default.String("ride_metrics_by_minute")
    String getIcebergTable();

    void setIcebergTable(String value);

    @Description("The iceberg database to write to.")
    @Validation.Required
    @Default.String("taxi_dataset")
    String getIcebergDatabase();

    void setIcebergDatabase(String value);

    @Validation.Required
    @Default.String("taxi_rides")
    String getCatalogName();

    void setCatalogName(String catalogName);
  }
}
