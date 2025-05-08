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
package org.apache.beam.sdk.io.solace.it;

import com.google.api.client.http.HttpRequestFactory;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.solace.broker.BasicAuthSempClient;
import org.apache.beam.sdk.io.solace.broker.SempBasicAuthClientExecutor;
import org.apache.beam.sdk.util.SerializableSupplier;

/**
 * Example class showing how the {@link BasicAuthSempClient} can be extended or have functionalities
 * overridden. In this case, the modified method is {@link
 * BasicAuthSempClient#getBacklogBytes(String)}, which queries multiple SEMP endpoints to collect
 * accurate backlog metrics. For usage, see {@link SolaceIOMultipleSempIT}.
 */
public class BasicAuthMultipleSempClient extends BasicAuthSempClient {
  private final List<SempBasicAuthClientExecutor> sempBacklogBasicAuthClientExecutors;

  public BasicAuthMultipleSempClient(
      String mainHost,
      List<String> backlogHosts,
      String username,
      String password,
      String vpnName,
      SerializableSupplier<HttpRequestFactory> httpRequestFactorySupplier) {
    super(mainHost, username, password, vpnName, httpRequestFactorySupplier);
    sempBacklogBasicAuthClientExecutors =
        backlogHosts.stream()
            .map(
                host ->
                    new SempBasicAuthClientExecutor(
                        host, username, password, vpnName, httpRequestFactorySupplier.get()))
            .collect(Collectors.toList());
  }

  @Override
  public long getBacklogBytes(String queueName) throws IOException {
    long backlog = 0;
    for (SempBasicAuthClientExecutor client : sempBacklogBasicAuthClientExecutors) {
      backlog += client.getBacklogBytes(queueName);
    }
    return backlog;
  }
}
