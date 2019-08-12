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
package org.apache.beam.sdk.extensions.sql.meta.provider.test;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.service.AutoService;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.sql.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.impl.BeamTableStatistics;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.provider.InMemoryMetaTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunctions;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;

/**
 * Test in-memory table provider for use in tests.
 *
 * <p>Keeps global state and tracks class instances. Works only in DirectRunner.
 */
@AutoService(TableProvider.class)
public class TestTableProvider extends InMemoryMetaTableProvider {
  static final Map<Long, Map<String, TableWithRows>> GLOBAL_TABLES = new ConcurrentHashMap<>();

  private static final AtomicLong INSTANCES = new AtomicLong(0);
  private final long instanceId = INSTANCES.getAndIncrement();

  public TestTableProvider() {
    GLOBAL_TABLES.put(instanceId, new ConcurrentHashMap<>());
  }

  @Override
  public String getTableType() {
    return "test";
  }

  public Map<String, TableWithRows> tables() {
    return GLOBAL_TABLES.get(instanceId);
  }

  @Override
  public void createTable(Table table) {
    tables().put(table.getName(), new TableWithRows(instanceId, table));
  }

  @Override
  public void dropTable(String tableName) {
    tables().remove(tableName);
  }

  @Override
  public Map<String, Table> getTables() {
    return tables().entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().table));
  }

  @Override
  public synchronized BeamSqlTable buildBeamSqlTable(Table table) {
    return new InMemoryTable(tables().get(table.getName()));
  }

  public void addRows(String tableName, Row... rows) {
    checkArgument(tables().containsKey(tableName), "Table not found: " + tableName);
    tables().get(tableName).rows.addAll(Arrays.asList(rows));
  }

  public List<Row> tableRows(String tableName) {
    return tables().get(tableName).rows;
  }

  /** TableWitRows. */
  public static class TableWithRows implements Serializable {
    private Table table;
    private List<Row> rows;
    private long tableProviderInstanceId;

    public TableWithRows(long tableProviderInstanceId, Table table) {
      this.tableProviderInstanceId = tableProviderInstanceId;
      this.table = table;
      this.rows = new CopyOnWriteArrayList<>();
    }

    public List<Row> getRows() {
      return rows;
    }
  }

  private static class InMemoryTable implements BeamSqlTable {
    private TableWithRows tableWithRows;

    @Override
    public PCollection.IsBounded isBounded() {
      return PCollection.IsBounded.BOUNDED;
    }

    public InMemoryTable(TableWithRows tableWithRows) {
      this.tableWithRows = tableWithRows;
    }

    public Coder<Row> rowCoder() {
      return SchemaCoder.of(
          tableWithRows.table.getSchema(),
          SerializableFunctions.identity(),
          SerializableFunctions.identity());
    }

    @Override
    public BeamTableStatistics getTableStatistics(PipelineOptions options) {
      return BeamTableStatistics.createBoundedTableStatistics(
          (double) tableWithRows.getRows().size());
    }

    @Override
    public PCollection<Row> buildIOReader(PBegin begin) {
      TableWithRows tableWithRows =
          GLOBAL_TABLES
              .get(this.tableWithRows.tableProviderInstanceId)
              .get(this.tableWithRows.table.getName());
      return begin.apply(Create.of(tableWithRows.rows).withCoder(rowCoder()));
    }

    @Override
    public POutput buildIOWriter(PCollection<Row> input) {
      input.apply(ParDo.of(new CollectorFn(tableWithRows)));
      return PDone.in(input.getPipeline());
    }

    @Override
    public Schema getSchema() {
      return tableWithRows.table.getSchema();
    }
  }

  private static final class CollectorFn extends DoFn<Row, Row> {
    private TableWithRows tableWithRows;

    CollectorFn(TableWithRows tableWithRows) {
      this.tableWithRows = tableWithRows;
    }

    @ProcessElement
    public void procesElement(ProcessContext context) {
      long instanceId = tableWithRows.tableProviderInstanceId;
      String tableName = tableWithRows.table.getName();
      GLOBAL_TABLES.get(instanceId).get(tableName).rows.add(context.element());
      context.output(context.element());
    }
  }
}
