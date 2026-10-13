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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import org.apache.beam.examples.iceberg.DeltaLakeToLakehouseCdcExample.Options;
import org.apache.beam.examples.iceberg.DeltaLakeToLakehouseCdcUtils.LakehouseTableSpec;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link DeltaLakeToLakehouseCdcExample}. */
@RunWith(JUnit4.class)
public class DeltaLakeToLakehouseCdcExampleTest {

  @Test
  @SuppressWarnings("unchecked")
  public void testBuildManagedConfigs() {
    Options options = PipelineOptionsFactory.as(Options.class);
    options.setDeltaTable("gs://my-delta-lake-bucket/delta_lake/employee_data/");
    options.setLakehouseTable(
        "my-gcp-project.my-lakehouse-warehouse-bucket.my_namespace.my_iceberg_table");
    options.setEqualityColumns("employee_id");
    options.setStartVersion(2L);
    options.setEndVersion(4L);

    LakehouseTableSpec spec = DeltaLakeToLakehouseCdcUtils.resolveLakehouseTableSpec(options);
    Map<String, Object> readConfig =
        DeltaLakeToLakehouseCdcExample.buildDeltaCdcReadConfig(options, spec.project);
    assertEquals("gs://my-delta-lake-bucket/delta_lake/employee_data/", readConfig.get("table"));
    assertEquals(2L, readConfig.get("start_version"));
    assertEquals(4L, readConfig.get("end_version"));
    assertEquals(
        Arrays.asList("_change_type", "_commit_version"),
        readConfig.get("include_metadata_columns"));
    Map<String, String> hadoopConfig = (Map<String, String>) readConfig.get("hadoop_config");
    assertEquals("my-gcp-project", hadoopConfig.get("fs.gs.project.id"));

    Map<String, Object> writeConfig =
        DeltaLakeToLakehouseCdcExample.buildLakehouseCdcWriteConfig(options, spec);
    assertEquals("my_namespace.my_iceberg_table", writeConfig.get("table"));
    assertEquals("merge-on-read", writeConfig.get("mode"));
    assertEquals("_commit_version", writeConfig.get("sequence_number_column"));
    assertEquals("_change_type", writeConfig.get("change_type_column"));
    assertEquals(Arrays.asList("employee_id"), writeConfig.get("equality_columns"));
    Map<String, String> catalogProps = (Map<String, String>) writeConfig.get("catalog_properties");
    assertEquals("rest", catalogProps.get("type"));
    assertEquals("gs://my-lakehouse-warehouse-bucket", catalogProps.get("warehouse"));
    assertEquals("my-gcp-project", catalogProps.get("header.x-goog-user-project"));
  }

  @Test
  public void testBuildManagedConfigsStreamingJavaRunnerSkipsChangeTypeColumn() {
    Options options = PipelineOptionsFactory.as(Options.class);
    options.setUsePortableRunner(false);
    options.setDeltaTable("gs://my-delta-lake-bucket/delta_lake/employee_data/");
    options.setLakehouseTable(
        "my-gcp-project.my-lakehouse-warehouse-bucket.my_namespace.my_iceberg_table");
    options.setEqualityColumns("employee_id");
    options.setStartVersion(2L);
    options.setEndVersion(4L);

    LakehouseTableSpec spec = DeltaLakeToLakehouseCdcUtils.resolveLakehouseTableSpec(options);
    Map<String, Object> readConfig =
        DeltaLakeToLakehouseCdcExample.buildDeltaCdcReadConfig(options, spec.project);
    assertEquals(
        Collections.singletonList("_commit_version"), readConfig.get("include_metadata_columns"));

    Map<String, Object> writeConfig =
        DeltaLakeToLakehouseCdcExample.buildLakehouseCdcWriteConfig(options, spec);
    assertEquals("_commit_version", writeConfig.get("sequence_number_column"));
    assertFalse(writeConfig.containsKey("change_type_column"));
    assertFalse(writeConfig.containsKey("change_type_map"));
  }
}
