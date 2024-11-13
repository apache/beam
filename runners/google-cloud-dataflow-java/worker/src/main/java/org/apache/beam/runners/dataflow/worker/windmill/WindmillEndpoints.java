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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap.toImmutableMap;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet.toImmutableSet;

import com.google.auto.value.AutoValue;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Optional;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillServiceAddress.AuthenticatedGcpServiceAddress;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.net.HostAndPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Value class for holding endpoints used for communicating with Windmill service. Corresponds
 * directly with {@link Windmill.WorkerMetadataResponse}.
 */
@AutoValue
public abstract class WindmillEndpoints {
  private static final Logger LOG = LoggerFactory.getLogger(WindmillEndpoints.class);

  public static WindmillEndpoints none() {
    return WindmillEndpoints.builder()
        .setVersion(Long.MAX_VALUE)
        .setWindmillEndpoints(ImmutableSet.of())
        .setGlobalDataEndpoints(ImmutableMap.of())
        .build();
  }

  public static WindmillEndpoints from(
      Windmill.WorkerMetadataResponse workerMetadataResponseProto) {
    ImmutableMap<String, WindmillEndpoints.Endpoint> globalDataServers =
        workerMetadataResponseProto.getGlobalDataEndpointsMap().entrySet().stream()
            .collect(
                toImmutableMap(
                    Map.Entry::getKey, // global data key
                    endpoint ->
                        WindmillEndpoints.Endpoint.from(
                            endpoint.getValue(),
                            workerMetadataResponseProto.getExternalEndpoint())));

    ImmutableSet<WindmillEndpoints.Endpoint> windmillServers =
        workerMetadataResponseProto.getWorkEndpointsList().stream()
            .map(
                endpointProto ->
                    Endpoint.from(endpointProto, workerMetadataResponseProto.getExternalEndpoint()))
            .collect(toImmutableSet());

    return WindmillEndpoints.builder()
        .setVersion(workerMetadataResponseProto.getMetadataVersion())
        .setGlobalDataEndpoints(globalDataServers)
        .setWindmillEndpoints(windmillServers)
        .build();
  }

  public static WindmillEndpoints.Builder builder() {
    return new AutoValue_WindmillEndpoints.Builder();
  }

  private static Optional<WindmillServiceAddress> parseDirectEndpoint(
      Windmill.WorkerMetadataResponse.Endpoint endpointProto, String authenticatingService) {
    Optional<WindmillServiceAddress> directEndpointIpV6Address =
        tryParseDirectEndpointIntoIpV6Address(endpointProto)
            .map(address -> AuthenticatedGcpServiceAddress.create(authenticatingService, address))
            .map(WindmillServiceAddress::create);

    return directEndpointIpV6Address.isPresent()
        ? directEndpointIpV6Address
        : tryParseEndpointIntoHostAndPort(endpointProto.getDirectEndpoint())
            .map(WindmillServiceAddress::create);
  }

  private static Optional<HostAndPort> tryParseEndpointIntoHostAndPort(String directEndpoint) {
    try {
      return Optional.of(HostAndPort.fromString(directEndpoint));
    } catch (IllegalArgumentException e) {
      LOG.warn("{} cannot be parsed into a gcpServiceAddress", directEndpoint);
      return Optional.empty();
    }
  }

  private static Optional<HostAndPort> tryParseDirectEndpointIntoIpV6Address(
      Windmill.WorkerMetadataResponse.Endpoint endpointProto) {
    if (!endpointProto.hasDirectEndpoint()) {
      return Optional.empty();
    }

    InetAddress directEndpointAddress;
    try {
      directEndpointAddress = Inet6Address.getByName(endpointProto.getDirectEndpoint());
    } catch (UnknownHostException e) {
      LOG.warn(
          "Error occurred trying to parse direct_endpoint={} into IPv6 address. Exception={}",
          endpointProto.getDirectEndpoint(),
          e.toString());
      return Optional.empty();
    }

    // Inet6Address.getByAddress returns either an IPv4 or an IPv6 address depending on the format
    // of the direct_endpoint string.
    if (!(directEndpointAddress instanceof Inet6Address)) {
      LOG.warn(
          "{} is not an IPv6 address. Direct endpoints are expected to be in IPv6 format.",
          endpointProto.getDirectEndpoint());
      return Optional.empty();
    }

    return Optional.of(
        HostAndPort.fromParts(
            directEndpointAddress.getHostAddress(), (int) endpointProto.getPort()));
  }

  public final boolean isEmpty() {
    return equals(none());
  }

  /** Version of the endpoints which increases with every modification. */
  public abstract long version();

  /**
   * Used by GetData GlobalDataRequest(s) to support Beam side inputs. Returns a map where the key
   * is a global data tag and the value is the endpoint where the data associated with the global
   * data tag resides.
   *
   * @see <a href="https://beam.apache.org/documentation/programming-guide/#side-inputs">Beam Side
   *     Inputs</a>
   */
  public abstract ImmutableMap<String, Endpoint> globalDataEndpoints();

  /**
   * Used by GetWork/GetData/CommitWork calls to send, receive, and commit work directly to/from
   * Windmill servers. Returns a list of endpoints used to communicate with the corresponding
   * Windmill servers.
   */
  public abstract ImmutableSet<Endpoint> windmillEndpoints();

  /**
   * Representation of an endpoint in {@link Windmill.WorkerMetadataResponse.Endpoint} proto with
   * the worker_token field, and direct_endpoint field parsed into a {@link WindmillServiceAddress}
   * which holds either a {@link Inet6Address} or {@link HostAndPort} used to connect to Streaming
   * Engine. {@link Inet6Address}(s) represent direct Windmill worker connections, and {@link
   * HostAndPort}(s) represent connections to the Windmill Dispatcher.
   */
  @AutoValue
  public abstract static class Endpoint {
    public static Endpoint.Builder builder() {
      return new AutoValue_WindmillEndpoints_Endpoint.Builder();
    }

    public static Endpoint from(
        Windmill.WorkerMetadataResponse.Endpoint endpointProto, String authenticatingService) {
      Endpoint.Builder endpointBuilder = Endpoint.builder();

      if (!endpointProto.getDirectEndpoint().isEmpty()) {
        parseDirectEndpoint(endpointProto, authenticatingService)
            .ifPresent(endpointBuilder::setDirectEndpoint);
      }

      if (!endpointProto.getBackendWorkerToken().isEmpty()) {
        endpointBuilder.setWorkerToken(endpointProto.getBackendWorkerToken());
      }

      Endpoint endpoint = endpointBuilder.build();

      if (!endpoint.directEndpoint().isPresent() && !endpoint.workerToken().isPresent()) {
        throw new IllegalArgumentException(
            String.format(
                "direct_endpoint=[%s] not present or could not be parsed, and worker_token"
                    + " not present. At least one of these fields is required.",
                endpointProto.getDirectEndpoint()));
      }

      return endpoint;
    }

    /**
     * {@link WindmillServiceAddress} representation of {@link
     * Windmill.WorkerMetadataResponse.Endpoint#getDirectEndpoint()}. The proto's direct_endpoint
     * string can be converted to either {@link Inet6Address} or {@link HostAndPort}.
     */
    public abstract Optional<WindmillServiceAddress> directEndpoint();

    /**
     * Corresponds to {@link Windmill.WorkerMetadataResponse.Endpoint#getBackendWorkerToken()} ()}
     * in the windmill.proto file.
     */
    public abstract Optional<String> workerToken();

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setDirectEndpoint(WindmillServiceAddress directEndpoint);

      public abstract Builder setWorkerToken(String workerToken);

      public abstract Endpoint build();
    }
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setVersion(long version);

    public abstract Builder setGlobalDataEndpoints(
        ImmutableMap<String, WindmillEndpoints.Endpoint> globalDataServers);

    public abstract Builder setWindmillEndpoints(
        ImmutableSet<WindmillEndpoints.Endpoint> windmillServers);

    abstract ImmutableSet.Builder<WindmillEndpoints.Endpoint> windmillEndpointsBuilder();

    public final Builder addWindmillEndpoint(WindmillEndpoints.Endpoint endpoint) {
      windmillEndpointsBuilder().add(endpoint);
      return this;
    }

    public final Builder addAllWindmillEndpoints(Iterable<WindmillEndpoints.Endpoint> endpoints) {
      windmillEndpointsBuilder().addAll(endpoints);
      return this;
    }

    abstract ImmutableMap.Builder<String, WindmillEndpoints.Endpoint> globalDataEndpointsBuilder();

    public final Builder addGlobalDataEndpoint(
        String globalDataKey, WindmillEndpoints.Endpoint endpoint) {
      globalDataEndpointsBuilder().put(globalDataKey, endpoint);
      return this;
    }

    public final Builder addAllGlobalDataEndpoints(
        Map<String, WindmillEndpoints.Endpoint> globalDataEndpoints) {
      globalDataEndpointsBuilder().putAll(globalDataEndpoints);
      return this;
    }

    public abstract WindmillEndpoints build();
  }
}
