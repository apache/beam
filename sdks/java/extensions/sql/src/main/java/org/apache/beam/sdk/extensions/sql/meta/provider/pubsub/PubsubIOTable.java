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
package org.apache.beam.sdk.extensions.sql.meta.provider.pubsub;

import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.extensions.sql.impl.BeamTableStatistics;
import org.apache.beam.sdk.extensions.sql.meta.BaseBeamTable;
import org.apache.beam.sdk.extensions.sql.meta.provider.pubsub.PubsubTableProvider.PubsubIOTableConfiguration;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.values.PCollection.IsBounded;

@Internal
public abstract class PubsubIOTable extends BaseBeamTable {

  protected abstract PubsubIOTableConfiguration getConfig();

  @Override
  public IsBounded isBounded() {
    return IsBounded.UNBOUNDED;
  }

  @Override
  public BeamTableStatistics getTableStatistics(PipelineOptions options) {
    return BeamTableStatistics.UNBOUNDED_UNKNOWN;
  }

  protected PubsubIO.Read<PubsubMessage> readMessagesWithAttributes() {
    PubsubIO.Read<PubsubMessage> read =
        PubsubIO.readMessagesWithAttributes().fromTopic(getConfig().getTopic());

    return getConfig().useTimestampAttribute()
        ? read.withTimestampAttribute(getConfig().getTimestampAttribute())
        : read;
  }

  protected PubsubIO.Write<PubsubMessage> createPubsubMessageWrite() {
    PubsubIO.Write<PubsubMessage> write = PubsubIO.writeMessages().to(getConfig().getTopic());
    if (getConfig().useTimestampAttribute()) {
      write = write.withTimestampAttribute(getConfig().getTimestampAttribute());
    }
    return write;
  }

  protected PubsubIO.Write<PubsubMessage> writeMessagesToDlq() {
    PubsubIO.Write<PubsubMessage> write =
        PubsubIO.writeMessages().to(getConfig().getDeadLetterQueue());

    return getConfig().useTimestampAttribute()
        ? write.withTimestampAttribute(getConfig().getTimestampAttribute())
        : write;
  }
}
