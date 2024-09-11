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
package org.apache.beam.runners.dataflow.worker.windmill;

import org.apache.beam.sdk.annotations.Internal;

/** Client for WindmillService via Streaming Appliance. */
@Internal
public interface ApplianceWindmillClient {
  /** Get a batch of work to process. */
  Windmill.GetWorkResponse getWork(Windmill.GetWorkRequest request);

  /** Get additional data such as state needed to process work. */
  Windmill.GetDataResponse getData(Windmill.GetDataRequest request);

  /** Commit the work, issuing any output productions, state modifications etc. */
  Windmill.CommitWorkResponse commitWork(Windmill.CommitWorkRequest request);

  /** Get configuration data from the server. */
  Windmill.GetConfigResponse getConfig(Windmill.GetConfigRequest request);

  /** Report execution information to the server. */
  Windmill.ReportStatsResponse reportStats(Windmill.ReportStatsRequest request);
}
