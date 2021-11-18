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
package org.apache.beam.sdk.io.gcp.spanner.changestreams.dofn;

import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ChangeStreamSourceDescriptor;
import org.apache.beam.sdk.transforms.DoFn;

public class ChangeStreamSourceDoFn extends DoFn<byte[], ChangeStreamSourceDescriptor> {

  private static final long serialVersionUID = 5324139871217005480L;
  private final String changeStreamName;
  private final com.google.cloud.Timestamp startTimestamp;
  private final com.google.cloud.Timestamp endTimestamp;

  public ChangeStreamSourceDoFn(
      String changeStreamName,
      com.google.cloud.Timestamp startTimestamp,
      com.google.cloud.Timestamp endTimestamp) {
    this.changeStreamName = changeStreamName;
    this.startTimestamp = startTimestamp;
    this.endTimestamp = endTimestamp;
  }

  @ProcessElement
  public void processElement(OutputReceiver<ChangeStreamSourceDescriptor> outputReceiver) {
    outputReceiver.output(
        ChangeStreamSourceDescriptor.of(changeStreamName, startTimestamp, endTimestamp));
  }
}
