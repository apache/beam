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
package org.apache.beam.sdk.io.aws.sns;

import com.amazonaws.http.SdkHttpMetadata;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;
import java.util.HashMap;
import java.util.UUID;
import org.mockito.Mockito;

/** Mock class to test a successful publish of a msg. */
public class AmazonSNSMockSuccess extends AmazonSNSMock {
  @Override
  public PublishResult publish(PublishRequest publishRequest) {
    PublishResult result = Mockito.mock(PublishResult.class);
    SdkHttpMetadata metadata = Mockito.mock(SdkHttpMetadata.class);
    Mockito.when(metadata.getHttpHeaders()).thenReturn(new HashMap<>());
    Mockito.when(metadata.getHttpStatusCode()).thenReturn(200);
    Mockito.when(result.getSdkHttpMetadata()).thenReturn(metadata);
    Mockito.when(result.getMessageId()).thenReturn(UUID.randomUUID().toString());
    return result;
  }
}
