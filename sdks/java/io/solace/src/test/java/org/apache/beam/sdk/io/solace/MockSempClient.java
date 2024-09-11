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
package org.apache.beam.sdk.io.solace;

import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.Queue;
import java.io.IOException;
import org.apache.beam.sdk.io.solace.broker.SempClient;
import org.apache.beam.sdk.transforms.SerializableFunction;

public class MockSempClient implements SempClient {

  private final SerializableFunction<String, Boolean> isQueueNonExclusiveFn;
  private final SerializableFunction<String, Long> getBacklogBytesFn;
  private final SerializableFunction<String, Integer> createQueueForTopicFn;

  private MockSempClient(
      SerializableFunction<String, Boolean> isQueueNonExclusiveFn,
      SerializableFunction<String, Long> getBacklogBytesFn,
      SerializableFunction<String, Integer> createQueueForTopicFn) {
    this.isQueueNonExclusiveFn = isQueueNonExclusiveFn;
    this.getBacklogBytesFn = getBacklogBytesFn;
    this.createQueueForTopicFn = createQueueForTopicFn;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private SerializableFunction<String, Boolean> isQueueNonExclusiveFn = (queueName) -> true;
    private SerializableFunction<String, Long> getBacklogBytesFn = (queueName) -> 0L;
    private SerializableFunction<String, Integer> createQueueForTopicFn = (queueName) -> 0;

    public Builder setIsQueueNonExclusiveFn(
        SerializableFunction<String, Boolean> isQueueNonExclusiveFn) {
      this.isQueueNonExclusiveFn = isQueueNonExclusiveFn;
      return this;
    }

    public Builder setGetBacklogBytesFn(SerializableFunction<String, Long> getBacklogBytesFn) {
      this.getBacklogBytesFn = getBacklogBytesFn;
      return this;
    }

    public Builder setCreateQueueForTopicFn(
        SerializableFunction<String, Integer> createQueueForTopicFn) {
      this.createQueueForTopicFn = createQueueForTopicFn;
      return this;
    }

    public MockSempClient build() {
      return new MockSempClient(isQueueNonExclusiveFn, getBacklogBytesFn, createQueueForTopicFn);
    }
  }

  @Override
  public boolean isQueueNonExclusive(String queueName) throws IOException {
    return isQueueNonExclusiveFn.apply(queueName);
  }

  @Override
  public Queue createQueueForTopic(String queueName, String topicName) throws IOException {
    createQueueForTopicFn.apply(queueName);
    return JCSMPFactory.onlyInstance().createQueue(queueName);
  }

  @Override
  public long getBacklogBytes(String queueName) throws IOException {
    return getBacklogBytesFn.apply(queueName);
  }
}
