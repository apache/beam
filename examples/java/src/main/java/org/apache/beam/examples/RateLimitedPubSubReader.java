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

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.components.ratelimiter.EnvoyRateLimiterContext;
import org.apache.beam.sdk.io.components.ratelimiter.EnvoyRateLimiterFactory;
import org.apache.beam.sdk.io.components.ratelimiter.RateLimiter;
import org.apache.beam.sdk.io.components.ratelimiter.RateLimiterContext;
import org.apache.beam.sdk.io.components.ratelimiter.RateLimiterFactory;
import org.apache.beam.sdk.io.components.ratelimiter.RateLimiterOptions;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple example demonstrating how to read from a Pub/Sub topic, rate limit the stream using an
 * Envoy Rate Limit Service, and simply log the raw records.
 *
 * <p>To run this example, you need a running Envoy Rate Limit Service and access to the GCP Pub/Sub
 * topic.
 */
public class RateLimitedPubSubReader {

  public interface Options extends PipelineOptions {
    @Description("Address of the Envoy Rate Limit Service (eg: localhost:8081)")
    @Validation.Required
    String getRateLimiterAddress();

    void setRateLimiterAddress(String value);

    @Description("Domain for the Rate Limit Service (eg: mydomain)")
    @Validation.Required
    String getRateLimiterDomain();

    void setRateLimiterDomain(String value);

    @Description(
        "The Pub/Sub topic to read from (eg: projects/pubsub-public-data/topics/taxirides-realtime)")
    @Validation.Required
    String getTopic();

    void setTopic(String value);
  }

  static class RateLimitAndLogFn extends DoFn<String, String> {
    private final String rlsAddress;
    private final String rlsDomain;
    private transient @Nullable RateLimiter rateLimiter;
    private static final Logger LOG = LoggerFactory.getLogger(RateLimitAndLogFn.class);

    public RateLimitAndLogFn(String rlsAddress, String rlsDomain) {
      this.rlsAddress = rlsAddress;
      this.rlsDomain = rlsDomain;
    }

    @Setup
    public void setup() {
      // Create the RateLimiterOptions.
      RateLimiterOptions options = RateLimiterOptions.builder().setAddress(rlsAddress).build();

      // Static RateLimiter with pre-configured domain and descriptors
      RateLimiterFactory factory = new EnvoyRateLimiterFactory(options);
      RateLimiterContext context =
          EnvoyRateLimiterContext.builder()
              .setDomain(rlsDomain)
              .addDescriptor("database", "users") // generic descriptors
              .build();
      this.rateLimiter = factory.getLimiter(context);
    }

    @Teardown
    public void teardown() {
      if (rateLimiter != null) {
        try {
          rateLimiter.close();
        } catch (Exception e) {
          LOG.warn("Failed to close RateLimiter", e);
        }
      }
    }

    @ProcessElement
    public void processElement(@Element String element, OutputReceiver<String> receiver)
        throws Exception {
      try {
        Preconditions.checkNotNull(rateLimiter).allow(1);
      } catch (Exception e) {
        throw new RuntimeException("Failed to acquire rate limit token", e);
      }

      // Simulate external API call or simply log the read entry
      Thread.sleep(100);
      LOG.info("Received and rate-limited record: {}", element);
      receiver.output(element);
    }
  }

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    Pipeline p = Pipeline.create(options);

    p.apply("ReadFromPubSub", PubsubIO.readStrings().fromTopic(options.getTopic()))
        .apply(
            "RateLimitAndLog",
            ParDo.of(
                new RateLimitAndLogFn(
                    options.getRateLimiterAddress(), options.getRateLimiterDomain())));

    p.run().waitUntilFinish();
  }
}
