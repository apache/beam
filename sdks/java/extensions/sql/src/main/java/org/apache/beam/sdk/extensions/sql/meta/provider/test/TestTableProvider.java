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

import static org.apache.beam.vendor.calcite.v1_20_0.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.service.AutoService;
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.sql.impl.BeamTableStatistics;
import org.apache.beam.sdk.extensions.sql.meta.BaseBeamTable;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTableFilter;
import org.apache.beam.sdk.extensions.sql.meta.DefaultTableFilter;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.provider.InMemoryMetaTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.transforms.Filter;
import org.apache.beam.sdk.schemas.transforms.Select;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexCall;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexInputRef;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexLiteral;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexNode;

/**
 * Test in-memory table provider for use in tests.
 *
 * <p>Keeps global state and tracks class instances. Works only in DirectRunner.
 */
@AutoService(TableProvider.class)
public class TestTableProvider extends InMemoryMetaTableProvider {
  static final Map<Long, Map<String, TableWithRows>> GLOBAL_TABLES = new ConcurrentHashMap<>();
  public static final String PUSH_DOWN_OPTION = "push_down";

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

  private static class InMemoryTable extends BaseBeamTable {
    private TableWithRows tableWithRows;
    private PushDownOptions options;

    @Override
    public PCollection.IsBounded isBounded() {
      return PCollection.IsBounded.BOUNDED;
    }

    public InMemoryTable(TableWithRows tableWithRows) {
      this.tableWithRows = tableWithRows;

      // The reason for introducing a property here is to simplify writing unit tests, testing
      // project and predicate push-down behavior when run separate and together.
      if (tableWithRows.table.getProperties().containsKey(PUSH_DOWN_OPTION)) {
        options =
            PushDownOptions.valueOf(
                tableWithRows.table.getProperties().getString(PUSH_DOWN_OPTION).toUpperCase());
      } else {
        options = PushDownOptions.NONE;
      }
    }

    public Coder<Row> rowCoder() {
      return SchemaCoder.of(tableWithRows.table.getSchema());
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
    public PCollection<Row> buildIOReader(
        PBegin begin, BeamSqlTableFilter filters, List<String> fieldNames) {
      if (!(filters instanceof DefaultTableFilter)
          && (options == PushDownOptions.NONE || options == PushDownOptions.PROJECT)) {
        throw new RuntimeException(
            "Filter push-down is not supported, yet non-default filter was passed.");
      }
      if ((!fieldNames.isEmpty() && fieldNames.size() < getSchema().getFieldCount())
          && (options == PushDownOptions.NONE || options == PushDownOptions.FILTER)) {
        //throw new RuntimeException(
        //    "Project push-down is not supported, yet a list of fieldNames was passed.");
      }

      PCollection<Row> withAllFields = buildIOReader(begin);
      if (options == PushDownOptions.NONE) { // needed for testing purposes
        return withAllFields;
      }

      PCollection<Row> result = withAllFields;
      if ((options == PushDownOptions.FILTER || options == PushDownOptions.BOTH)
          && filters instanceof TestTableFilter) {
        for (RexNode node : ((TestTableFilter) filters).getSupported()) {
          result = result.apply("PushDownFilter_" + node.toString(), filterFromNode(node));
        }
      }

      if ((options == PushDownOptions.PROJECT || options == PushDownOptions.BOTH)
          && !fieldNames.isEmpty()) {
        result = result.apply("PushDownProject", Select.fieldAccess(FieldAccessDescriptor.withFieldNames(fieldNames).withOrderByFieldInsertionOrder()));
      }

      return result;
    }

    @Override
    public POutput buildIOWriter(PCollection<Row> input) {
      input.apply(ParDo.of(new CollectorFn(tableWithRows)));
      return PDone.in(input.getPipeline());
    }

    @Override
    public BeamSqlTableFilter constructFilter(List<RexNode> filter) {
      if (options == PushDownOptions.FILTER || options == PushDownOptions.BOTH) {
        return new TestTableFilter(filter);
      } else {
        return super.constructFilter(filter);
      }
    }

    @Override
    public boolean supportsProjects() {
      return options == PushDownOptions.BOTH || options == PushDownOptions.PROJECT;
    }

    @Override
    public Schema getSchema() {
      return tableWithRows.table.getSchema();
    }

    private PTransform<PCollection<Row>, PCollection<Row>> filterFromNode(RexNode node) {
      if (!(node instanceof RexCall)) {
        throw new RuntimeException("Was expecting a RexCall, but received: " + node.getClass().getSimpleName());
      }

      List<RexNode> operands = ((RexCall) node).getOperands();
      List<Integer> fieldIds = new ArrayList<>();
      List<RexLiteral> literals = new ArrayList<>();
      List<RexInputRef> inputRefs = new ArrayList<>();

      for (RexNode operand : operands) {
        if (operand instanceof RexInputRef) {
          RexInputRef inputRef = (RexInputRef) operand;
          fieldIds.add(inputRef.getIndex());
          inputRefs.add(inputRef);
        } else if (operand instanceof RexLiteral) {
          RexLiteral literal = (RexLiteral) operand;
          literals.add(literal);
        } else {
          throw new RuntimeException("Encountered an unexpected operand: " + operand.getClass().getSimpleName());
        }
      }

      // TODO: add support for expressions like:
      //  =(CAST($3):INTEGER NOT NULL, 200)
      switch (node.getKind()) {
        case IN:
          break;
        case LESS_THAN:
          // Cast Object to Comparable
          break;
        case GREATER_THAN:
          break;
        case LESS_THAN_OR_EQUAL:
          break;
        case GREATER_THAN_OR_EQUAL:
          break;
        case EQUALS:
          SerializableFunction<Row, Boolean> filter;
          // Comparing 2 columns.
          if (inputRefs.size() == 2) {
            final int op0 = fieldIds.indexOf(inputRefs.get(0).getIndex());
            final int op1 = fieldIds.indexOf(inputRefs.get(1).getIndex());
            filter = row -> row.getValue(op0).equals(op1);
          // Comparing a column to a literal.
          } else {
            int fieldSchemaIndex = inputRefs.get(0).getIndex();
            TypeName beamFieldType = getSchema().getField(fieldSchemaIndex).getType().getTypeName();
            final int op0 = fieldIds.indexOf(fieldSchemaIndex);

            // Find Java type of the op0 in Schema
            final Comparable op1 = literals.get(0).<Comparable>getValueAs(lookupJavaClass(beamFieldType));
            filter = row -> row.getValue(op0).equals(op1);
          }
          // Case where we compare 2 Literals should never appear and get optimized away.
          return Filter.<Row>create().whereFieldIds(fieldIds, filter);
        case NOT_EQUALS:
          break;
        default:
          throw new RuntimeException("Unsupported node kind: " + node.getKind().toString());
      }

      return null;
    }

    private static Class lookupJavaClass(TypeName type) {
      switch (type) {
        case BYTE:
          return Byte.class;
        case BYTES:
          return byte[].class;
        case INT16:
          return Short.class;
        case INT32:
          return Integer.class;
        case INT64:
          return Long.class;
        case DECIMAL:
          return BigDecimal.class;
        case FLOAT:
          return Float.class;
        case DOUBLE:
          return Double.class;
        case STRING:
          return String.class;
        case BOOLEAN:
          return Boolean.class;
        default:
          throw new RuntimeException("Could not resolve beam type [" + type.toString() + "] to Java class");
      }
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

  public enum PushDownOptions {
    NONE,
    PROJECT,
    FILTER,
    BOTH
  }
}
