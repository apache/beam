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
package org.apache.beam.sdk.extensions.sql.impl;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

/** Options used to configure BeamSQL. */
public interface BeamSqlPipelineOptions extends PipelineOptions {

  @Description("QueryPlanner class name.")
  @Default.String("org.apache.beam.sdk.extensions.sql.impl.CalciteQueryPlanner")
  String getPlannerName();

  void setPlannerName(String className);

  @Description(
      "Default timezone for ZetaSQL analyzer; has no effect on CalciteSQL. Allow tz database "
          + "timezone names in https://en.wikipedia.org/wiki/List_of_tz_database_time_zones")
  @Default.String("UTC")
  String getZetaSqlDefaultTimezone();

  void setZetaSqlDefaultTimezone(String timezone);

  @Description("Enables extra verification of row values for debugging.")
  @Default.Boolean(false)
  Boolean getVerifyRowValues();

  void setVerifyRowValues(Boolean verifyRowValues);
}
