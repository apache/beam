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
package org.apache.beam.sdk.io.solace.read;

import java.io.IOException;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.io.solace.broker.SempClient;
import org.apache.beam.sdk.io.solace.broker.SempClientFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.util.Preconditions;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Internal
public class AckMessageDoFn extends DoFn<Long, Void> {
  private static final Logger LOG = LoggerFactory.getLogger(AckMessageDoFn.class);
  private String queueName;
  private final SempClientFactory sempClientFactory;
  @Nullable SempClient sempClient;

  public AckMessageDoFn(String queueName, SempClientFactory sempClientFactory) {
    this.queueName = queueName;
    this.sempClientFactory = sempClientFactory;
  }

  @StartBundle
  public void startBundle() {
    sempClient = sempClientFactory.create();
  }

  @Teardown
  public void tearDown() {
    if (sempClient != null) {
      sempClient = null;
    }
  }

  @ProcessElement
  public void processElement(@Element Long msgId) throws IOException {
    Preconditions.checkStateNotNull(sempClient).ack(queueName, msgId);
  }

  @FinishBundle
  public void finishBundle() {
    if (sempClient != null) {
      sempClient = null;
    }
  }
}
