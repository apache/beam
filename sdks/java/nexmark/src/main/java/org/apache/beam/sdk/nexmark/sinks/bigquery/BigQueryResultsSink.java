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

package org.apache.beam.sdk.nexmark.sinks.bigquery;

import com.google.common.base.Strings;

import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.NexmarkOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

/**
 * BigQueryResultsSink.
 */
public class BigQueryResultsSink extends PTransform<PCollection<String>, POutput> {

  private static final TupleTag<String> MAIN = new TupleTag<String>() { };
  private static final TupleTag<String> SIDE = new TupleTag<String>() { };
  private NexmarkConfiguration configuration;
  private NexmarkOptions options;
  private String queryName;
  private long now;

  public static PTransform<PCollection<String>, POutput> createSink(
      NexmarkConfiguration configuration,
      NexmarkOptions options,
      String queryName,
      long now) {

    return new BigQueryResultsSink(configuration, options, queryName, now);
  }

  private BigQueryResultsSink(
      NexmarkConfiguration configuration,
      NexmarkOptions options,
      String queryName,
      long now) {

    this.configuration = configuration;
    this.options = options;
    this.queryName = queryName;
    this.now = now;
  }

  @Override
  public POutput expand(PCollection<String> input) {
    // Multiple BigQuery backends to mimic what most customers do.

    PCollectionTuple res =
        input.apply(
            queryName + ".Partition",
            partitionByOddHashcode().withOutputTags(MAIN, TupleTagList.of(SIDE)));

    res.get(MAIN).apply(writeTo("main"));
    res.get(SIDE).apply(writeTo("side"));

    return input.apply(writeTo("copy"));
  }

  private ParDo.SingleOutput<String, String> partitionByOddHashcode() {
    return ParDo.of(new DoFn<String, String>() {
      @DoFn.ProcessElement
      public void processElement(DoFn.ProcessContext c) {
        if (c.element().hashCode() % 2 == 0) {
          c.output(c.element());
        } else {
          c.output(SIDE, c.element());
        }
      }
    });
  }

  private String tableName(String version) {
    String baseTableName = options.getBigQueryTable();

    if (Strings.isNullOrEmpty(baseTableName)) {
      throw new RuntimeException("Missing --bigQueryTable");
    }

    switch (options.getResourceNameMode()) {
      case VERBATIM:
        return String.format("%s:nexmark.%s_%s",
            options.getProject(), baseTableName, version);
      case QUERY:
        return String.format("%s:nexmark.%s_%s_%s",
            options.getProject(), baseTableName, queryName, version);
      case QUERY_AND_SALT:
        return String.format("%s:nexmark.%s_%s_%s_%d",
            options.getProject(), baseTableName, queryName, version, now);
      default:
        throw new RuntimeException("Unrecognized enum " + options.getResourceNameMode());
    }
  }

  private BigQueryTableWriter writeTo(String version) {
    return new BigQueryTableWriter(options, queryName, tableName(version));
  }



}
