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
package org.apache.beam.sdk.io.gcp.pubsub;

import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.TopicPath;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

@SuppressWarnings({
  "rawtypes", // TODO(https://issues.apache.org/jira/browse/BEAM-10556)
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
/**
 * A {@link PTransform} which represents a runner implemented Pubsub sink. {@link
 * RunnerImplementedSinkTranslator} will translate this transform into well-known composite.
 */
@Internal
public class RunnerImplementedSink extends PTransform<PCollection<byte[]>, PDone> {
  private final PubsubUnboundedSink sink;

  public RunnerImplementedSink(PubsubUnboundedSink sink) {
    this.sink = sink;
  }

  public PubsubUnboundedSink getOverrideSink() {
    return sink;
  }

  public ValueProvider<TopicPath> getTopicProvider() {
    return sink.getTopicProvider();
  }

  public String getTimestampAttribute() {
    return sink.getTimestampAttribute();
  }

  public String getIdAttribute() {
    return sink.getIdAttribute();
  }

  @Override
  public PDone expand(PCollection<byte[]> input) {
    return PDone.in(input.getPipeline());
  }

  @Override
  protected String getKindString() {
    return "RunnerImplementedSink";
  }
}
