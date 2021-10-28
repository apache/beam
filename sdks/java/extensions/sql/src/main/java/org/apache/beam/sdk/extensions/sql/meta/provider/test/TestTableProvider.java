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

import static org.apache.beam.vendor.calcite.v1_26_0.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.service.AutoService;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.beam.sdk.extensions.sql.impl.BeamTableStatistics;
import org.apache.beam.sdk.extensions.sql.meta.BaseBeamTable;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTableFilter;
import org.apache.beam.sdk.extensions.sql.meta.DefaultTableFilter;
import org.apache.beam.sdk.extensions.sql.meta.ProjectSupport;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.provider.InMemoryMetaTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.FieldTypeDescriptors;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
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
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rex.RexCall;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rex.RexInputRef;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rex.RexLiteral;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.type.SqlTypeName;

/**
 * Test in-memory table provider for use in tests.
 *
 * <p>Keeps global state and tracks class instances. Works only in DirectRunner.
 */
@AutoService(TableProvider.class)
@SuppressWarnings({
  "rawtypes", // TODO(https://issues.apache.org/jira/browse/BEAM-10556)
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
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
      return begin.apply(Create.of(tableWithRows.rows).withRowSchema(getSchema()));
    }

    @Override
    public PCollection<Row> buildIOReader(
        PBegin begin, BeamSqlTableFilter filters, List<String> fieldNames) {
      if (!(filters instanceof DefaultTableFilter)
          && (options == PushDownOptions.NONE || options == PushDownOptions.PROJECT)) {
        throw new UnsupportedOperationException(
            "Filter push-down is not supported, yet non-default filter was passed.");
      }
      if ((!fieldNames.isEmpty() && fieldNames.size() < getSchema().getFieldCount())
          && (options == PushDownOptions.NONE || options == PushDownOptions.FILTER)) {
        throw new UnsupportedOperationException(
            "Project push-down is not supported, yet a list of fieldNames was passed.");
      }

      PCollection<Row> withAllFields = buildIOReader(begin);
      if (options == PushDownOptions.NONE) { // needed for testing purposes
        return withAllFields;
      }

      PCollection<Row> result = withAllFields;
      // When filter push-down is supported.
      if (options == PushDownOptions.FILTER || options == PushDownOptions.BOTH) {
        if (filters instanceof TestTableFilter) {
          // Create a filter for each supported node.
          for (RexNode node : ((TestTableFilter) filters).getSupported()) {
            result = result.apply("IOPushDownFilter_" + node.toString(), filterFromNode(node));
          }
        } else {
          throw new UnsupportedOperationException(
              "Was expecting a filter of type TestTableFilter, but received: "
                  + filters.getClass().getSimpleName());
        }
      }

      // When project push-down is supported or field reordering is needed.
      if ((options == PushDownOptions.PROJECT || options == PushDownOptions.BOTH)
          && !fieldNames.isEmpty()) {
        result =
            result.apply(
                "IOPushDownProject",
                Select.fieldAccess(FieldAccessDescriptor.withFieldNames(fieldNames)));
      }

      return result;
    }

    @Override
    public POutput buildIOWriter(PCollection<Row> input) {
      input.apply(ParDo.of(new CollectorFn(tableWithRows))).setRowSchema(input.getSchema());
      return PDone.in(input.getPipeline());
    }

    @Override
    public BeamSqlTableFilter constructFilter(List<RexNode> filter) {
      if (options == PushDownOptions.FILTER || options == PushDownOptions.BOTH) {
        return new TestTableFilter(filter);
      }
      return super.constructFilter(filter);
    }

    @Override
    public ProjectSupport supportsProjects() {
      return (options == PushDownOptions.BOTH || options == PushDownOptions.PROJECT)
          ? ProjectSupport.WITH_FIELD_REORDERING
          : ProjectSupport.NONE;
    }

    @Override
    public Schema getSchema() {
      return tableWithRows.table.getSchema();
    }

    /**
     * A helper method to create a {@code Filter} from {@code RexNode}.
     *
     * @param node {@code RexNode} to create a filter from.
     * @return {@code Filter} PTransform.
     */
    private PTransform<PCollection<Row>, PCollection<Row>> filterFromNode(RexNode node) {
      List<RexNode> operands = new ArrayList<>();
      List<Integer> fieldIds = new ArrayList<>();
      List<RexLiteral> literals = new ArrayList<>();
      List<RexInputRef> inputRefs = new ArrayList<>();

      if (node instanceof RexCall) {
        operands.addAll(((RexCall) node).getOperands());
      } else if (node instanceof RexInputRef) {
        operands.add(node);
        operands.add(RexLiteral.fromJdbcString(node.getType(), SqlTypeName.BOOLEAN, "true"));
      } else {
        throw new UnsupportedOperationException(
            "Was expecting a RexCall or a boolean RexInputRef, but received: "
                + node.getClass().getSimpleName());
      }

      for (RexNode operand : operands) {
        if (operand instanceof RexInputRef) {
          RexInputRef inputRef = (RexInputRef) operand;
          fieldIds.add(inputRef.getIndex());
          inputRefs.add(inputRef);
        } else if (operand instanceof RexLiteral) {
          RexLiteral literal = (RexLiteral) operand;
          literals.add(literal);
        } else {
          throw new UnsupportedOperationException(
              "Encountered an unexpected operand: " + operand.getClass().getSimpleName());
        }
      }

      SerializableFunction<Integer, Boolean> comparison;
      // TODO: add support for expressions like:
      //  =(CAST($3):INTEGER NOT NULL, 200)
      switch (node.getKind()) {
        case LESS_THAN:
          comparison = i -> i < 0;
          break;
        case GREATER_THAN:
          comparison = i -> i > 0;
          break;
        case LESS_THAN_OR_EQUAL:
          comparison = i -> i <= 0;
          break;
        case GREATER_THAN_OR_EQUAL:
          comparison = i -> i >= 0;
          break;
        case EQUALS:
        case INPUT_REF:
          comparison = i -> i == 0;
          break;
        case NOT_EQUALS:
          comparison = i -> i != 0;
          break;
        default:
          throw new UnsupportedOperationException(
              "Unsupported node kind: " + node.getKind().toString());
      }

      return Filter.<Row>create()
          .whereFieldIds(
              fieldIds, createFilter(operands, fieldIds, inputRefs, literals, comparison));
    }

    /**
     * A helper method to create a serializable function comparing row fields.
     *
     * @param operands A list of operands used in a comparison.
     * @param fieldIds A list of operand ids.
     * @param inputRefs A list of operands, which are an instanceof {@code RexInputRef}.
     * @param literals A list of operands, which are an instanceof {@code RexLiteral}.
     * @param comparison A comparison to perform between operands.
     * @return A filter comparing row fields to literals/other fields.
     */
    private SerializableFunction<Row, Boolean> createFilter(
        List<RexNode> operands,
        List<Integer> fieldIds,
        List<RexInputRef> inputRefs,
        List<RexLiteral> literals,
        SerializableFunction<Integer, Boolean> comparison) {
      // Filter push-down only supports comparisons between 2 operands (for now).
      assert operands.size() == 2;
      // Comparing two columns (2 input refs).
      assert inputRefs.size() <= 2;
      // Case where we compare 2 Literals should never appear and get optimized away.
      assert literals.size() < 2;

      if (inputRefs.size() == 2) { // Comparing 2 columns.
        final int op0 = fieldIds.indexOf(inputRefs.get(0).getIndex());
        final int op1 = fieldIds.indexOf(inputRefs.get(1).getIndex());
        return row -> comparison.apply(row.<Comparable>getValue(op0).compareTo(op1));
      }
      // Comparing a column to a literal.
      int fieldSchemaIndex = inputRefs.get(0).getIndex();
      FieldType beamFieldType = getSchema().getField(fieldSchemaIndex).getType();
      final int op0 = fieldIds.indexOf(fieldSchemaIndex);

      // Find Java type of the op0 in Schema
      final Comparable op1 =
          literals
              .get(0)
              .<Comparable>getValueAs(
                  FieldTypeDescriptors.javaTypeForFieldType(beamFieldType).getRawType());
      if (operands.get(0) instanceof RexLiteral) { // First operand is a literal
        return row -> comparison.apply(op1.compareTo(row.getValue(op0)));
      } else if (operands.get(0) instanceof RexInputRef) { // First operand is a column value
        return row -> comparison.apply(row.<Comparable>getValue(op0).compareTo(op1));
      } else {
        throw new UnsupportedOperationException(
            "Was expecting a RexLiteral and a RexInputRef, but received: "
                + operands.stream()
                    .map(o -> o.getClass().getSimpleName())
                    .collect(Collectors.joining(", ")));
      }
    }
  }

  private static final class CollectorFn extends DoFn<Row, Row> {
    private TableWithRows tableWithRows;

    CollectorFn(TableWithRows tableWithRows) {
      this.tableWithRows = tableWithRows;
    }

    @ProcessElement
    public void processElement(@Element Row element, OutputReceiver<Row> o) {
      long instanceId = tableWithRows.tableProviderInstanceId;
      String tableName = tableWithRows.table.getName();
      GLOBAL_TABLES.get(instanceId).get(tableName).rows.add(element);
      o.output(element);
    }
  }

  public enum PushDownOptions {
    NONE,
    PROJECT,
    FILTER,
    BOTH
  }
}
