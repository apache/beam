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
package org.apache.beam.examples.adk;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.SpannerOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Redistribute;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

public class AmlPipeline {
  public static final TupleTag<Mutation> MUTATION_TAG = new TupleTag<Mutation>() {};
  public static final TupleTag<String> REVIEW_TAG = new TupleTag<String>() {};

  public static void main(String[] args) {
    AmlPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(AmlPipelineOptions.class);

    Pipeline pipeline = Pipeline.create(options);
    SpannerOptions.enableOpenTelemetryTraces();
    SpannerOptions.disableOpenCensusMetrics();
    SpannerOptions.enableOpenTelemetryMetrics();
    SpannerConfig spannerConfig =
        SpannerConfig.create()
            .withProjectId(options.getProject())
            .withInstanceId(options.getSpannerInstance())
            .withDatabaseId(options.getSpannerDatabase());
    pipeline
        .apply(
            "Add transaction",
            PubsubIO.readMessages()
                .withEnableOpenTelemetryTracing()
                .fromSubscription("projects/radoslaws-playground-pso/subscriptions/txn-sub"))
        .apply("Redistribute", Redistribute.arbitrarily())
        .apply("Create mutation", ParDo.of(new NewTransactionDoFn(options.getOutputTable())))
        .apply(
            "Write New Tx to Spanner Table",
            SpannerIO.write()
                .withSpannerConfig(spannerConfig)
                .withEnableOpenTelemetryTracing(true));
    PCollectionTuple amlResult =
        pipeline
            .apply(
                "Read Spanner Change Stream",
                SpannerIO.readChangeStream()
                    .withSpannerConfig(spannerConfig)
                    .withLowLatency()
                    .withEnableOpenTelemetryTracing(true)
                    .withChangeStreamName(options.getChangeStreamName())
                    .withInclusiveStartAt(
                        com.google.cloud.Timestamp.ofTimeSecondsAndNanos(
                            com.google.cloud.Timestamp.now().getSeconds(), 0)))
            .apply("Filter New Transactions", ParDo.of(new AmlChangeStreamFilterDoFn()))
            //   .apply("Redistribute", Redistribute.arbitrarily())
            .apply("Key", WithKeys.<String, TransactionEvent>of(TransactionEvent::getSenderId))
            .setCoder(
                KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(TransactionEvent.class)))
            .apply(
                "ADK Graph AML Analysis",
                ParDo.of(
                        new AmlAgentDoFn(
                            options.getSpannerInstance(),
                            options.getSpannerDatabase(),
                            options.getOutputTable()))
                    .withOutputTags(MUTATION_TAG, TupleTagList.of(REVIEW_TAG)));
    amlResult
        .get(REVIEW_TAG)
        .apply(
            "Write to pubsub",
            PubsubIO.writeStrings()
                .withEnableOpenTelemetryTracing()
                .to("projects/radoslaws-playground-pso/topics/review"));
    amlResult
        .get(MUTATION_TAG)
        .apply(
            "Write to Spanner Table",
            SpannerIO.write()
                .withSpannerConfig(spannerConfig)
                .withEnableOpenTelemetryTracing(true));

    pipeline.run().waitUntilFinish();
  }
}
