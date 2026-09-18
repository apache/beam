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
import org.apache.beam.sdk.io.iceberg.SchemaEvolutionConfig.IncompatibleSchemaHandling;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;

/**
 * Commits one schema union per window (the combine output is one element per window) and emits the
 * resulting schema id as the signal for the Wait.on gate ahead of file registration.
 */
class CommitSchemaOnce extends DoFn<List<CollectDistinctSchemas.SchemaGroup>, Long> {
  static final String COMMITS_COUNTER = "numSchemaCommits";
  private static final Counter numSchemaCommits = counter(CommitSchemaOnce.class, COMMITS_COUNTER);

  private final IcebergCatalogConfig catalogConfig;
  private final String identifier;
  private final SchemaEvolutionConfig config;
  private final IncompatibleSchemaHandling handling;
  private final CommitSchemaUnion.TableCreation creation;
  private final CommitSchemaUnion.Committer committer;
  private transient @MonotonicNonNull Catalog catalog;

  CommitSchemaOnce(
      IcebergCatalogConfig catalogConfig,
      String identifier,
      SchemaEvolutionConfig config,
      IncompatibleSchemaHandling handling,
      CommitSchemaUnion.TableCreation creation) {
    this(
        catalogConfig, identifier, config, handling, creation, CommitSchemaUnion.DEFAULT_COMMITTER);
  }

  CommitSchemaOnce(
      IcebergCatalogConfig catalogConfig,
      String identifier,
      SchemaEvolutionConfig config,
      IncompatibleSchemaHandling handling,
      CommitSchemaUnion.TableCreation creation,
      CommitSchemaUnion.Committer committer) {
    this.catalogConfig = catalogConfig;
    this.identifier = identifier;
    this.config = config;
    this.handling = handling;
    this.creation = creation;
    this.committer = committer;
  }

  @ProcessElement
  public void process(
      @Element List<CollectDistinctSchemas.SchemaGroup> schemas, OutputReceiver<Long> out) {
    if (catalog == null) {
      catalog = catalogConfig.catalog();
    }
    TableIdentifier tableId = IcebergUtils.parseTableIdentifier(identifier);
    // The committer runs only when a transaction is actually committed, so wrapping it counts
    // real commits and skips no-op windows.
    CommitSchemaUnion.Committer counting =
        txn -> {
          committer.commit(txn);
          numSchemaCommits.inc();
        };
    long schemaId =
        CommitSchemaUnion.commit(catalog, tableId, schemas, config, handling, creation, counting);
    out.output(schemaId);
  }
}
