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
package org.apache.beam.sdk.io.solace.data;

/**
 * A record to be written to a Solace topic.
 *
 * <p>You need to transform to {@link Solace.Record} to be able to write to Solace. For that, you
 * can use the {@link Solace.Record.Builder} provided with this class.
 *
 * <p>For instance, to create a record, use the following code:
 *
 * <pre>{@code
 * Solace.Record record = Solace.Record.builder()
 *         .setMessageId(messageId)
 *         .setSenderTimestamp(timestampMillis)
 *         .setPayload(payload)
 *         .build();
 * }</pre>
 *
 * Setting the message id and the timestamp is mandatory.
 */
public class Solace {

  public static class Queue {
    private final String name;

    private Queue(String name) {
      this.name = name;
    }

    public static Queue fromName(String name) {
      return new Queue(name);
    }

    public String getName() {
      return name;
    }
  }

  public static class Topic {
    private final String name;

    private Topic(String name) {
      this.name = name;
    }

    public static Topic fromName(String name) {
      return new Topic(name);
    }

    public String getName() {
      return name;
    }
  }
}
