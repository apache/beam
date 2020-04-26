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
package org.apache.beam.runners.flink;

import akka.actor.ActorSystem;
import com.typesafe.config.Config;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.akka.AkkaRpcService;
import org.apache.flink.runtime.rpc.akka.AkkaRpcServiceConfiguration;

/** A {@link MiniCluster} which allows remote connections for the end-to-end test. */
public class RemoteMiniClusterImpl extends RemoteMiniCluster {

  private int port;

  public RemoteMiniClusterImpl(MiniClusterConfiguration miniClusterConfiguration) {
    super(miniClusterConfiguration);
  }

  @Override
  protected RpcService createRpcService(
      AkkaRpcServiceConfiguration akkaRpcServiceConfig, boolean remoteEnabled, String bindAddress) {

    // Enable remote connections to the mini cluster which are disabled by default
    final Config akkaConfig =
        AkkaUtils.getAkkaConfig(akkaRpcServiceConfig.getConfiguration(), bindAddress, 0);

    final Config effectiveAkkaConfig = AkkaUtils.testDispatcherConfig().withFallback(akkaConfig);

    final ActorSystem actorSystem = AkkaUtils.createActorSystem(effectiveAkkaConfig);

    final AkkaRpcService akkaRpcService = new AkkaRpcService(actorSystem, akkaRpcServiceConfig);
    this.port = akkaRpcService.getPort();

    return akkaRpcService;
  }

  @Override
  public int getClusterPort() {
    Preconditions.checkState(port > 0, "Port not yet initialized. Start the cluster first.");
    return port;
  }

  @Override
  public int getRestPort() {
    try {
      return getRestAddress().get().getPort();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
