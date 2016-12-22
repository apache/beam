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
package org.apache.beam.sdk.io.distributedlog;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkNotNull;

import com.twitter.distributedlog.LogSegmentMetadata;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * A log segment represents a bundle of approximately <code>desiredBundleSizeBytes</code>.
 */
class LogSegmentBundle implements Externalizable {

  private static final long serialVersionUID = -1924976223810524339L;

  private String streamName;
  private LogSegmentMetadata metadata;

  public LogSegmentBundle() {}

  public LogSegmentBundle(
      String streamName,
      LogSegmentMetadata metadata) {
    checkNotNull(metadata, "Log Segment is null");
    this.streamName = streamName;
    this.metadata = metadata;
  }

  String getStreamName() {
    return streamName;
  }

  LogSegmentMetadata getMetadata() {
    return metadata;
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeObject(streamName);
    out.writeObject(metadata.getFinalisedData());
  }

  @Override
  public void readExternal(ObjectInput in)
      throws IOException, ClassNotFoundException {
    this.streamName = (String) in.readObject();
    String metadataStr = (String) in.readObject();
    this.metadata = LogSegmentMetadata.parseData(streamName, metadataStr.getBytes(UTF_8));
  }

}
