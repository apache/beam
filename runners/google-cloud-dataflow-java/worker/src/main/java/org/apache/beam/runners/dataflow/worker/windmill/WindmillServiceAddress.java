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

import com.google.auto.value.AutoOneOf;
import com.google.auto.value.AutoValue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.net.HostAndPort;

/** Used to create channels to communicate with Streaming Engine via gRpc. */
@AutoOneOf(WindmillServiceAddress.Kind.class)
public abstract class WindmillServiceAddress {

  public static WindmillServiceAddress create(HostAndPort gcpServiceAddress) {
    return AutoOneOf_WindmillServiceAddress.gcpServiceAddress(gcpServiceAddress);
  }

  public static WindmillServiceAddress create(
      AuthenticatedGcpServiceAddress authenticatedGcpServiceAddress) {
    return AutoOneOf_WindmillServiceAddress.authenticatedGcpServiceAddress(
        authenticatedGcpServiceAddress);
  }

  public abstract Kind getKind();

  public abstract HostAndPort gcpServiceAddress();

  public abstract AuthenticatedGcpServiceAddress authenticatedGcpServiceAddress();

  public final HostAndPort getServiceAddress() {
    return getKind() == WindmillServiceAddress.Kind.GCP_SERVICE_ADDRESS
        ? gcpServiceAddress()
        : authenticatedGcpServiceAddress().gcpServiceAddress();
  }

  public enum Kind {
    GCP_SERVICE_ADDRESS,
    AUTHENTICATED_GCP_SERVICE_ADDRESS
  }

  @AutoValue
  public abstract static class AuthenticatedGcpServiceAddress {

    public static AuthenticatedGcpServiceAddress create(
        String authenticatingService, HostAndPort gcpServiceAddress) {
      // HostAndPort supports IpV6.
      return new AutoValue_WindmillServiceAddress_AuthenticatedGcpServiceAddress(
          authenticatingService, gcpServiceAddress);
    }

    public abstract String authenticatingService();

    public abstract HostAndPort gcpServiceAddress();
  }
}
