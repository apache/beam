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
package org.apache.beam.sdk.io.iceberg;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Splitter;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

/**
 * Test-side description of the BigLake (Lakehouse) Iceberg REST catalog the ITs run against.
 *
 * <p>The catalog is a multiple-bucket catalog addressed by a {@code
 * bl://projects/PROJECT/catalogs/CATALOG} warehouse and allowed to store resources under a fixed
 * set of Cloud Storage locations: the catalog's default location plus any restricted locations.
 * Configured (defaults and overrides) by the integrationTest task in build.gradle through the
 * system properties
 *
 * <ul>
 *   <li>{@code beam.iceberg.biglake.warehouse}: the {@code bl://} warehouse URI
 *   <li>{@code beam.iceberg.biglake.locations}: comma-separated {@code gs://} prefixes the catalog
 *       may write to; the first is the catalog's default location, the rest are additional
 *       restricted locations
 * </ul>
 */
public final class BigLakeTestCatalog {
  private static final Pattern WAREHOUSE_PATTERN =
      Pattern.compile("bl://projects/([^/]+)/catalogs/([^/]+)");

  public static final String WAREHOUSE = requiredProperty("beam.iceberg.biglake.warehouse");

  public static final List<String> LOCATIONS =
      ImmutableList.copyOf(
          Splitter.on(',')
              .trimResults()
              .omitEmptyStrings()
              .split(requiredProperty("beam.iceberg.biglake.locations")));

  /** Catalog id, which is also the second segment of BigQuery's 4-part table reference. */
  public static final String CATALOG_ID = parseCatalogId(WAREHOUSE);

  private static final String PROJECT =
      TestPipeline.testingPipelineOptions().as(GcpOptions.class).getProject();

  private BigLakeTestCatalog() {}

  private static String requiredProperty(String name) {
    String value = System.getProperty(name);
    checkArgument(
        value != null && !value.isEmpty(),
        "System property %s is not set; run through the integrationTest gradle task, which"
            + " sets it, or pass -D%s=...",
        name,
        name);
    return value;
  }

  private static String parseCatalogId(String warehouse) {
    // Legacy single-bucket catalogs are addressed by their bucket and named after it.
    if (warehouse.startsWith("gs://")) {
      return bucketOf(warehouse);
    }
    Matcher matcher = WAREHOUSE_PATTERN.matcher(warehouse);
    checkArgument(
        matcher.matches(),
        "Expected a bl://projects/PROJECT/catalogs/CATALOG (or legacy gs://BUCKET) warehouse, got '%s'",
        warehouse);
    return matcher.group(2);
  }

  /** The catalog's default location; tables land here unless created with an explicit location. */
  public static String defaultLocation() {
    return LOCATIONS.get(0);
  }

  /** A restricted location outside the default one, i.e. in a second bucket. */
  public static String additionalLocation() {
    checkArgument(
        LOCATIONS.size() >= 2,
        "beam.iceberg.biglake.locations must list at least two locations, got %s",
        LOCATIONS);
    return LOCATIONS.get(1);
  }

  public static String bucketOf(String gcsLocation) {
    checkArgument(gcsLocation.startsWith("gs://"), "Not a gs:// location: %s", gcsLocation);
    String withoutScheme = gcsLocation.substring("gs://".length());
    int slash = withoutScheme.indexOf('/');
    return slash < 0 ? withoutScheme : withoutScheme.substring(0, slash);
  }

  /** Object-name prefix (no bucket, no leading slash) of a {@code gs://bucket/path} location. */
  public static String prefixOf(String gcsLocation) {
    String withoutScheme = gcsLocation.substring("gs://".length());
    int slash = withoutScheme.indexOf('/');
    return slash < 0 ? "" : withoutScheme.substring(slash + 1);
  }

  public static Map<String, String> catalogProperties() {
    return ImmutableMap.<String, String>builder()
        .put("type", "rest")
        .put("uri", "https://biglake.googleapis.com/iceberg/v1/restcatalog")
        .put("warehouse", WAREHOUSE)
        .put("header.x-goog-user-project", PROJECT)
        .put("io-impl", "org.apache.iceberg.gcp.gcs.GCSFileIO")
        .put("rest.auth.type", "org.apache.iceberg.gcp.auth.GoogleAuthManager")
        .build();
  }

  /** BigQuery's 4-part {@code project.catalog.namespace.table} reference. */
  public static String bigQueryTableSpec(String namespace, String table) {
    return String.format("%s.%s.%s.%s", PROJECT, CATALOG_ID, namespace, table);
  }
}
