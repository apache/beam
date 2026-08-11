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
package org.apache.beam.sdk.io.gcp.bigquery;

import java.io.IOException;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class StorageApiSinkSchemaUpdateWithoutInputSchemaIT
    extends StorageApiSinkSchemaUpdateITBase {
  private static final String BIG_QUERY_DATASET_ID =
      "storage_api_sink_schema_change_without_input_" + System.nanoTime();

  @Parameterized.Parameters(name = "changeTableSchema={0}")
  public static Iterable<Object[]> data() {
    return ImmutableList.of(new Object[] {false}, new Object[] {true});
  }

  public StorageApiSinkSchemaUpdateWithoutInputSchemaIT(boolean changeTableSchema) {
    super(false, changeTableSchema, BIG_QUERY_DATASET_ID);
  }

  @BeforeClass
  public static void setUpTestEnvironment() throws IOException, InterruptedException {
    StorageApiSinkSchemaUpdateITBase.setUpTestEnvironment(BIG_QUERY_DATASET_ID);
  }

  @AfterClass
  public static void cleanUp() {
    StorageApiSinkSchemaUpdateITBase.cleanUp(BIG_QUERY_DATASET_ID);
  }
}
