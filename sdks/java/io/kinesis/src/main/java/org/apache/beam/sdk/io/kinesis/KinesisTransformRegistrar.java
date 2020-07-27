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
package org.apache.beam.sdk.io.kinesis;

import com.amazonaws.regions.Regions;
import com.google.auto.service.AutoService;
import java.util.Map;
import java.util.Properties;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.expansion.ExternalTransformRegistrar;
import org.apache.beam.sdk.transforms.ExternalTransformBuilder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

/** Exposes {@link KinesisIO.Write} as an external transform for cross-language usage. */
@Experimental(Kind.PORTABILITY)
@AutoService(ExternalTransformRegistrar.class)
public class KinesisTransformRegistrar implements ExternalTransformRegistrar {
  public static final String WRITE_URN = "beam:external:java:kinesis:write:v1";

  @Override
  public Map<String, Class<? extends ExternalTransformBuilder>> knownBuilders() {
    return ImmutableMap.of(WRITE_URN, WriteBuilder.class);
  }

  private abstract static class CrossLanguageConfiguration {
    String streamName;
    String awsAccessKey;
    String awsSecretKey;
    Regions region;
    @Nullable String serviceEndpoint;

    public void setStreamName(String streamName) {
      this.streamName = streamName;
    }

    public void setAwsAccessKey(String awsAccessKey) {
      this.awsAccessKey = awsAccessKey;
    }

    public void setAwsSecretKey(String awsSecretKey) {
      this.awsSecretKey = awsSecretKey;
    }

    public void setRegion(String region) {
      this.region = Regions.fromName(region);
    }

    public void setServiceEndpoint(@Nullable String serviceEndpoint) {
      this.serviceEndpoint = serviceEndpoint;
    }
  }

  @Experimental(Kind.PORTABILITY)
  public static class WriteBuilder
      implements ExternalTransformBuilder<WriteBuilder.Configuration, PCollection<byte[]>, PDone> {

    public static class Configuration extends CrossLanguageConfiguration {
      private Properties producerProperties;
      private String partitionKey;

      public void setProducerProperties(Iterable<KV<String, String>> producerProperties) {
        if (producerProperties != null) {
          Properties properties = new Properties();
          producerProperties.forEach(kv -> properties.setProperty(kv.getKey(), kv.getValue()));
          this.producerProperties = properties;
        }
      }

      public void setPartitionKey(String partitionKey) {
        this.partitionKey = partitionKey;
      }
    }

    @Override
    public PTransform<PCollection<byte[]>, PDone> buildExternal(Configuration configuration) {
      KinesisIO.Write writeTransform =
          KinesisIO.write()
              .withStreamName(configuration.streamName)
              .withAWSClientsProvider(
                  configuration.awsAccessKey,
                  configuration.awsSecretKey,
                  configuration.region,
                  configuration.serviceEndpoint)
              .withPartitionKey(configuration.partitionKey);

      if (configuration.producerProperties != null) {
        writeTransform = writeTransform.withProducerProperties(configuration.producerProperties);
      }

      return writeTransform;
    }
  }
}
