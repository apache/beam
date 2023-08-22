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
package org.apache.beam.runners.dataflow.worker.windmill.connectionscache;

import java.util.Optional;
import org.apache.beam.runners.dataflow.worker.windmill.CloudWindmillServiceV1Alpha1Grpc;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillApplianceGrpc;

/** Read only interface for {@link WindmillConnectionsCache}. */
public interface ReadOnlyWindmillConnectionsCache {

  /**
   * Used by GetDataStream and CommitWorkStream calls inorder to make sure Windmill API calls get
   * routed to the same workers that GetWork was initiated on. If no {@link WindmillConnection} is
   * found, it means that the windmill worker working on the key range has moved on (ranges have
   * moved to other workers, worker crash, etc.).
   *
   * @see <a
   *     href=https://medium.com/google-cloud/streaming-engine-execution-model-1eb2eef69a8e>Windmill
   *     API</a>
   */
  Optional<WindmillConnection> getWindmillWorkerConnection(WindmillConnectionCacheToken token);

  /**
   * Used by GetWorkStream to initiate the Windmill API calls and fetch Work items.
   *
   * @see <a
   *     href=https://medium.com/google-cloud/streaming-engine-execution-model-1eb2eef69a8e>Windmill
   *     API</a>
   */
  WindmillConnection getNewWindmillWorkerConnection();

  CloudWindmillServiceV1Alpha1Grpc.CloudWindmillServiceV1Alpha1Stub getDispatcherStub();

  Optional<CloudWindmillServiceV1Alpha1Grpc.CloudWindmillServiceV1Alpha1Stub> getGlobalDataStub(
      Windmill.GlobalDataId globalDataId);

  Optional<WindmillApplianceGrpc.WindmillApplianceBlockingStub> getWindmillApplianceStub();
}
