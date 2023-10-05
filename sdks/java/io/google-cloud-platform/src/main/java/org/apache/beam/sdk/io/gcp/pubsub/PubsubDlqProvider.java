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
package org.apache.beam.sdk.io.gcp.pubsub;

import com.google.auto.service.AutoService;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.schemas.io.Failure;
import org.apache.beam.sdk.schemas.io.GenericDlqProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

@Internal
@AutoService(GenericDlqProvider.class)
public class PubsubDlqProvider implements GenericDlqProvider {
  @Override
  public String identifier() {
    return "pubsub";
  }

  @Override
  public PTransform<PCollection<Failure>, PDone> newDlqTransform(String config) {
    return new DlqTransform(config);
  }

  private static class DlqTransform extends PTransform<PCollection<Failure>, PDone> {
    private final String topic;

    DlqTransform(String topic) {
      this.topic = topic;
    }

    @Override
    public PDone expand(PCollection<Failure> input) {
      return input
          .apply(
              "Failure to PubsubMessage",
              MapElements.into(TypeDescriptor.of(PubsubMessage.class))
                  .via(DlqTransform::getMessage))
          .apply("Write Failures to Pub/Sub", PubsubIO.writeMessages().to(topic));
    }

    private static PubsubMessage getMessage(Failure failure) {
      return new PubsubMessage(
          failure.getPayload(), ImmutableMap.of("beam-dlq-error", failure.getError()));
    }
  }
}
