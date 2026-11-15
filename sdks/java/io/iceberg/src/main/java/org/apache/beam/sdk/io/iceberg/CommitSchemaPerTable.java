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
package org.apache.beam.sdk.io.iceberg;

import static org.apache.beam.sdk.metrics.Metrics.counter;

import java.util.List;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.iceberg.catalog.Catalog;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Commits one schema union per table per window and emits a verdict for each, so that a table that
 * cannot be registered safely is refused on its own while the others load.
 *
 * <p>Never throws: a refusal is a value. The gate downstream reads these verdicts as a side input
 * and decides which tables' files may proceed.
 */
class CommitSchemaPerTable
    extends DoFn<
        KV<String, List<CollectDistinctSchemas.SchemaGroup>>, KV<String, SchemaCommitVerdict>> {

  private static final Logger LOG = LoggerFactory.getLogger(CommitSchemaPerTable.class);

  static final String COMMITS_COUNTER = "numSchemaCommits";
  static final String REFUSALS_COUNTER = "numTablesRefused";
  private static final Counter numSchemaCommits =
      counter(CommitSchemaPerTable.class, COMMITS_COUNTER);
  private static final Counter numTablesRefused =
      counter(CommitSchemaPerTable.class, REFUSALS_COUNTER);

  private final IcebergCatalogConfig catalogConfig;
  private final CommitSchemaUnion.Settings settings;
  private final CommitSchemaUnion.Committer committer;
  private transient @MonotonicNonNull Catalog catalog;

  CommitSchemaPerTable(
      IcebergCatalogConfig catalogConfig,
      CommitSchemaUnion.Settings settings,
      CommitSchemaUnion.Committer committer) {
    this.catalogConfig = catalogConfig;
    this.settings = settings;
    this.committer = committer;
  }

  @ProcessElement
  public void process(
      @Element KV<String, List<CollectDistinctSchemas.SchemaGroup>> element,
      OutputReceiver<KV<String, SchemaCommitVerdict>> out) {
    if (catalog == null) {
      catalog = catalogConfig.catalog();
    }
    String table = element.getKey();
    CommitSchemaUnion.Committer countingCommitter =
        txn -> {
          committer.commit(txn);
          numSchemaCommits.inc();
        };
    SchemaCommitVerdict verdict =
        CommitSchemaUnion.tryCommit(
            catalog, table, element.getValue(), settings, countingCommitter);
    if (verdict.refused()) {
      numTablesRefused.inc();
      LOG.warn("Table {} refused by the schema pre-pass: {}", table, verdict.getRefusal());
    }
    out.output(KV.of(table, verdict));
  }
}
