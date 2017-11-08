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

package org.apache.beam.sdk.nexmark.sinks;

import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.NexmarkOptions;
import org.apache.beam.sdk.nexmark.sinks.bigquery.BigQueryResultsSink;
import org.apache.beam.sdk.nexmark.sinks.pubsub.PubsubResultsSink;
import org.apache.beam.sdk.nexmark.sinks.text.TextFileResultsSink;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;

/**
 * Creates a sink for formatted query execution results.
 * Query is applied and results are formatted
 * in {@link org.apache.beam.sdk.nexmark.NexmarkLauncher}.
 */
public class QueryResultsSinkFactory {

  public static PTransform<PCollection<String>, ? extends POutput> createSink(
      NexmarkConfiguration configuration,
      NexmarkOptions options,
      String queryName,
      long now) {

    switch (configuration.sinkType) {
      case PUBSUB:
        return PubsubResultsSink.createPubsubSink(configuration, options, queryName, now);
      case TEXT:
        return TextFileResultsSink.createSink(options, queryName, now);
      case BIGQUERY:
        return BigQueryResultsSink.createSink(configuration, options, queryName, now);
      default:
        throw new IllegalStateException("Unexpected sink " + configuration.sinkType);
    }
  }
}
