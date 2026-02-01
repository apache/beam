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
package org.apache.beam.examples;

import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.components.ratelimiter.EnvoyRateLimiterContext;
import org.apache.beam.sdk.io.components.ratelimiter.EnvoyRateLimiterFactory;
import org.apache.beam.sdk.io.components.ratelimiter.RateLimiter;
import org.apache.beam.sdk.io.components.ratelimiter.RateLimiterFactory;
import org.apache.beam.sdk.io.components.ratelimiter.RateLimiterOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A simple example demonstrating how to use the {@link RateLimiter} in a custom {@link DoFn}.
 *
 * <p>This pipeline creates a small set of elements and processes them using a DoFn that calls an
 * external service (simulated). The processing is rate-limited using an Envoy Rate Limit Service.
 *
 * <p>To run this example, you need a running Envoy Rate Limit Service.
 */
public class RateLimiterSimple {

  public interface Options extends PipelineOptions {
    @Description("Address of the Envoy Rate Limit Service (e.g., localhost:8081)")
    String getRateLimiterAddress();

    void setRateLimiterAddress(String value);

    @Description("Domain for the Rate Limit Service")
    String getRateLimiterDomain();

    void setRateLimiterDomain(String value);
  }

  static class CallExternalServiceFn extends DoFn<String, String> {
    private final String rlsAddress;
    private final String rlsDomain;
    private transient @Nullable RateLimiter rateLimiter = null;

    public CallExternalServiceFn(String rlsAddress, String rlsDomain) {
      this.rlsAddress = rlsAddress;
      this.rlsDomain = rlsDomain;
    }

    @Setup
    public void setup() {
      // Create the RateLimiterOptions.
      RateLimiterOptions options = RateLimiterOptions.builder().setAddress(rlsAddress).build();

      // Static RateLimtier with pre-configured domain and descriptors
      RateLimiterFactory factory = new EnvoyRateLimiterFactory(options);
      EnvoyRateLimiterContext context =
          EnvoyRateLimiterContext.create(rlsDomain, ImmutableMap.of("database", "users"));
      this.rateLimiter = factory.getLimiter(context);
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      String element = c.element();
      try {
        Preconditions.checkNotNull(rateLimiter).allow(1);
      } catch (Exception e) {
        throw new RuntimeException("Failed to acquire rate limit token", e);
      }

      // Simulate external API call
      Thread.sleep(100);
      System.out.println("Processing: " + element);
      c.output("Processed: " + element);
    }
  }

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    Pipeline p = Pipeline.create(options);

    p.apply(
            "CreateItems",
            Create.of(
                IntStream.range(0, 1000000).mapToObj(i -> "item" + i).collect(Collectors.toList())))
        .apply(
            "CallExternalService",
            ParDo.of(
                new CallExternalServiceFn(
                    options.getRateLimiterAddress(), options.getRateLimiterDomain())));

    p.run().waitUntilFinish();
  }
}
