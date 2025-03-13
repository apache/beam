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
package org.apache.beam.sdk.io.gcp.spanner.changestreams.it;

import org.apache.beam.sdk.io.common.IOTestPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.StreamingOptions;
import org.checkerframework.checker.nullness.qual.Nullable;

public interface ChangeStreamTestPipelineOptions extends IOTestPipelineOptions, StreamingOptions {
  @Description("Project that hosts Spanner instance")
  @Nullable
  String getProjectId();

  void setProjectId(String value);

  @Description("Instance ID to write to in Spanner")
  @Default.String("beam-test")
  String getInstanceId();

  void setInstanceId(String value);

  @Description("Database ID prefix to write to in Spanner")
  @Default.String("cstest_primary")
  String getDatabaseId();

  void setDatabaseId(String value);

  @Description("Metadata database ID prefix to write to in Spanner")
  @Default.String("cstest_metadata")
  String getMetadataDatabaseId();

  void setMetadataDatabaseId(String value);
}
