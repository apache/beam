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
package org.apache.beam.sdk.io.aws2.kinesis;

import static org.apache.beam.sdk.io.aws2.kinesis.TestHelpers.createIOOptions;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.apache.beam.sdk.io.aws2.options.SerializationTestUtil;
import org.apache.beam.sdk.options.PipelineOptions;
import org.junit.Test;

public class KinesisIOOptionsTest {
  private KinesisIOOptions serializeDeserialize(KinesisIOOptions opts) {
    return SerializationTestUtil.serializeDeserialize(PipelineOptions.class, opts)
        .as(KinesisIOOptions.class);
  }

  @Test
  public void testSerializeDeserializeDefaults() {
    KinesisIOOptions options = createIOOptions();
    KinesisIOOptions copy = serializeDeserialize(options);

    Map<String, String> consumerMapping = options.getKinesisIOConsumerArns();
    Map<String, String> copyMapping = copy.getKinesisIOConsumerArns();

    assertThat(copyMapping.size()).isEqualTo(0);
    assertThat(copyMapping).isEqualTo(consumerMapping);
  }

  @Test
  public void testConsumerMapping() {
    KinesisIOOptions options =
        createIOOptions(
            "--kinesisIOConsumerArns={\"stream-01\": \"arn-01\", \"stream-02\": \"arn-02\"}");
    KinesisIOOptions copy = serializeDeserialize(options);
    Map<String, String> consumerMapping = copy.getKinesisIOConsumerArns();

    assertThat(consumerMapping.get("stream-01")).isEqualTo("arn-01");
    assertThat(consumerMapping.get("stream-02")).isEqualTo("arn-02");
    assertThat(consumerMapping.get("other")).isNull();
  }

  @Test
  public void testNullConsumerArn() {
    KinesisIOOptions options = createIOOptions("--kinesisIOConsumerArns={\"stream-01\": null}");
    KinesisIOOptions copy = serializeDeserialize(options);
    Map<String, String> consumerMapping = copy.getKinesisIOConsumerArns();

    assertThat(consumerMapping.containsKey("stream-01")).isTrue();
    assertThat(consumerMapping.get("stream-01")).isNull();
    assertThat(consumerMapping.containsKey("other")).isFalse();
    assertThat(consumerMapping.get("other")).isNull();
  }
}
