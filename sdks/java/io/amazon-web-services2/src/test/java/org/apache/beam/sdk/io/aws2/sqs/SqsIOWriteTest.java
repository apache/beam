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
package org.apache.beam.sdk.io.aws2.sqs;

import static java.util.stream.IntStream.range;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.aws2.MockClientBuilderFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.SqsClientBuilder;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

/** Tests for {@link SqsIO.Write}. */
@RunWith(MockitoJUnitRunner.class)
public class SqsIOWriteTest {
  @Rule public TestPipeline p = TestPipeline.create();
  @Mock public SqsClient sqs;

  @Before
  public void configureClientBuilderFactory() {
    MockClientBuilderFactory.set(p, SqsClientBuilder.class, sqs);
  }

  @Test
  public void testWrite() {
    when(sqs.sendMessage(any(SendMessageRequest.class)))
        .thenReturn(SendMessageResponse.builder().build());

    SendMessageRequest.Builder builder = SendMessageRequest.builder().queueUrl("url");
    List<SendMessageRequest> messages =
        range(0, 100)
            .mapToObj(i -> builder.messageBody("test" + i).build())
            .collect(Collectors.toList());

    p.apply(Create.of(messages)).apply(SqsIO.write());
    p.run().waitUntilFinish();

    messages.forEach(msg -> verify(sqs).sendMessage(msg));
  }
}
