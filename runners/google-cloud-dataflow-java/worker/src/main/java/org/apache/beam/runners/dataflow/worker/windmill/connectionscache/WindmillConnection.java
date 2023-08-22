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

import com.google.auto.value.AutoValue;
import java.util.Optional;
import java.util.function.Function;
import org.apache.beam.runners.dataflow.worker.windmill.CloudWindmillServiceV1Alpha1Grpc;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillEndpoints;

@AutoValue
public abstract class WindmillConnection {
  static WindmillConnection.Builder from(
      WindmillEndpoints.Endpoint windmillEndpoint,
      Function<
              WindmillEndpoints.Endpoint,
              CloudWindmillServiceV1Alpha1Grpc.CloudWindmillServiceV1Alpha1Stub>
          endpointToStubFn) {
    WindmillConnection.Builder windmillWorkerConnection = WindmillConnection.builder();

    windmillEndpoint.workerToken().ifPresent(windmillWorkerConnection::setBackendWorkerToken);
    windmillWorkerConnection.setDirectStub(endpointToStubFn.apply(windmillEndpoint));

    return windmillWorkerConnection;
  }

  public static Builder builder() {
    return new AutoValue_WindmillConnection.Builder();
  }

  public abstract WindmillConnectionCacheToken cacheToken();

  public abstract Optional<String> backendWorkerToken();

  public abstract Optional<CloudWindmillServiceV1Alpha1Grpc.CloudWindmillServiceV1Alpha1Stub>
      directStub();

  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder setCacheToken(WindmillConnectionCacheToken cacheToken);

    abstract Builder setBackendWorkerToken(String backendWorkerToken);

    abstract Builder setDirectStub(
        CloudWindmillServiceV1Alpha1Grpc.CloudWindmillServiceV1Alpha1Stub directStub);

    abstract WindmillConnection build();
  }
}
