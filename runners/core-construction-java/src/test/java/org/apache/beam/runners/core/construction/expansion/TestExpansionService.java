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
package org.apache.beam.runners.core.construction.expansion;

import com.google.auto.service.AutoService;
import java.util.Map;
import org.apache.beam.model.pipeline.v1.ExternalTransforms;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.vendor.grpc.v1p13p1.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.beam.vendor.grpc.v1p13p1.io.grpc.Server;
import org.apache.beam.vendor.grpc.v1p13p1.io.grpc.ServerBuilder;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableMap;

/**
 * An {@link org.apache.beam.runners.core.construction.expansion.ExpansionService} useful for tests.
 */
public class TestExpansionService {

  private static final String TEST_COUNT_URN = "pytest:beam:transforms:count";
  private static final String TEST_FILTER_URN = "pytest:beam:transforms:filter_less_than";

  /** Registers a single test transformation. */
  @AutoService(ExpansionService.ExpansionServiceRegistrar.class)
  public static class TestTransforms implements ExpansionService.ExpansionServiceRegistrar {
    @Override
    public Map<String, ExpansionService.TransformProvider> knownTransforms() {
      return ImmutableMap.of(
          TEST_COUNT_URN, spec -> Count.perElement(),
          TEST_FILTER_URN,
              spec ->
                  Filter.lessThanEq(
                      // TODO(BEAM-6587): Use strings directly rather than longs.
                      (long) spec.getPayload().toStringUtf8().charAt(0)));
    }
  }

  /** Exposes Java's GenerateSequence transform */
  @AutoService(ExpansionService.ExpansionServiceRegistrar.class)
  public static class RegisteredTransforms implements ExpansionService.ExpansionServiceRegistrar {

    private static final String generateSequenceUrn =
        BeamUrns.getUrn(ExternalTransforms.GenerateSequencePayload.Enum.PROPERTIES);

    @Override
    public Map<String, ExpansionService.TransformProvider> knownTransforms() {
      ImmutableMap.Builder<String, ExpansionService.TransformProvider> builder =
          ImmutableMap.builder();
      builder.put(
          generateSequenceUrn,
          spec -> {
            ExternalTransforms.GenerateSequencePayload payload;
            try {
              payload = ExternalTransforms.GenerateSequencePayload.parseFrom(spec.getPayload());
            } catch (InvalidProtocolBufferException e) {
              throw new RuntimeException("Failed to parse GenerateSequencePayload", e);
            }
            return GenerateSequence.fromExternal(payload);
          });
      return builder.build();
    }
  }

  public static void main(String[] args) throws Exception {
    int port = Integer.parseInt(args[0]);
    System.out.println("Starting expansion service at localhost:" + port);
    Server server = ServerBuilder.forPort(port).addService(new ExpansionService()).build();
    server.start();
    server.awaitTermination();
  }
}
