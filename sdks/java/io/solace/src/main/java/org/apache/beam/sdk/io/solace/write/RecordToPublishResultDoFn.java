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
package org.apache.beam.sdk.io.solace.write;

import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.io.solace.data.Solace;
import org.apache.beam.sdk.transforms.DoFn;

/**
 * This class just transforms to PublishResult to be able to capture the windowing with the right
 * strategy. The output is not used for anything else.
 */
@Internal
public class RecordToPublishResultDoFn extends DoFn<Solace.Record, Solace.PublishResult> {
  @ProcessElement
  public void processElement(
      @Element Solace.Record record, OutputReceiver<Solace.PublishResult> receiver) {
    Solace.PublishResult result =
        Solace.PublishResult.builder()
            .setPublished(true)
            .setMessageId(record.getMessageId())
            .setLatencyNanos(0L)
            .build();
    receiver.output(result);
  }
}
