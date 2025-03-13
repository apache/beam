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

import com.google.auto.value.AutoValue;
import java.util.Optional;
import java.util.function.Function;
import org.apache.beam.runners.dataflow.worker.windmill.CloudWindmillServiceV1Alpha1Grpc.CloudWindmillServiceV1Alpha1Stub;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillEndpoints.Endpoint;
import org.apache.beam.sdk.annotations.Internal;

@AutoValue
@Internal
public abstract class WindmillConnection {
  private static final String NO_BACKEND_WORKER_TOKEN = "";

  public static WindmillConnection from(
      Endpoint windmillEndpoint,
      Function<Endpoint, CloudWindmillServiceV1Alpha1Stub> endpointToStubFn) {
    WindmillConnection.Builder windmillWorkerConnection = WindmillConnection.builder();

    windmillEndpoint.workerToken().ifPresent(windmillWorkerConnection::setBackendWorkerToken);
    windmillEndpoint.directEndpoint().ifPresent(windmillWorkerConnection::setDirectEndpoint);
    windmillWorkerConnection.setStub(endpointToStubFn.apply(windmillEndpoint));

    return windmillWorkerConnection.build();
  }

  public static Builder builder() {
    return new AutoValue_WindmillConnection.Builder()
        .setBackendWorkerToken(NO_BACKEND_WORKER_TOKEN);
  }

  public abstract String backendWorkerToken();

  public abstract Optional<WindmillServiceAddress> directEndpoint();

  public abstract CloudWindmillServiceV1Alpha1Stub stub();

  @AutoValue.Builder
  public abstract static class Builder {
    abstract Builder setBackendWorkerToken(String backendWorkerToken);

    public abstract Builder setDirectEndpoint(WindmillServiceAddress value);

    public abstract Builder setStub(CloudWindmillServiceV1Alpha1Stub stub);

    public abstract WindmillConnection build();
  }
}
