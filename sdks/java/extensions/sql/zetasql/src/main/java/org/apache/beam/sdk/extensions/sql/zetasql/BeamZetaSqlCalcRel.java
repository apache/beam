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
package org.apache.beam.sdk.extensions.sql.zetasql;

import com.google.zetasql.AnalyzerOptions;
import com.google.zetasql.PreparedExpression;
import com.google.zetasql.Value;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.IntFunction;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlPipelineOptions;
import org.apache.beam.sdk.extensions.sql.impl.QueryPlanner.QueryParameters;
import org.apache.beam.sdk.extensions.sql.impl.rel.AbstractBeamCalcRel;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.extensions.sql.meta.provider.bigquery.BeamBigQuerySqlDialect;
import org.apache.beam.sdk.extensions.sql.meta.provider.bigquery.BeamSqlUnparseContext;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelOptCluster;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelTraitSet;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.core.Calc;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.rel2sql.SqlImplementor;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexBuilder;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexProgram;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlDialect;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlIdentifier;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;

/**
 * BeamRelNode to replace {@code Project} and {@code Filter} node based on the {@code ZetaSQL}
 * expression evaluator.
 */
@Internal
public class BeamZetaSqlCalcRel extends AbstractBeamCalcRel {

  private static final SqlDialect DIALECT = BeamBigQuerySqlDialect.DEFAULT;
  private final SqlImplementor.Context context;

  private static String columnName(int i) {
    return "_" + i;
  }

  public BeamZetaSqlCalcRel(
      RelOptCluster cluster, RelTraitSet traits, RelNode input, RexProgram program) {
    super(cluster, traits, input, program);
    final IntFunction<SqlNode> fn = i -> new SqlIdentifier(columnName(i), SqlParserPos.ZERO);
    context = new BeamSqlUnparseContext(fn);
  }

  @Override
  public Calc copy(RelTraitSet traitSet, RelNode input, RexProgram program) {
    return new BeamZetaSqlCalcRel(getCluster(), traitSet, input, program);
  }

  @Override
  public PTransform<PCollectionList<Row>, PCollection<Row>> buildPTransform() {
    return new Transform();
  }

  private class Transform extends PTransform<PCollectionList<Row>, PCollection<Row>> {
    @Override
    public PCollection<Row> expand(PCollectionList<Row> pinput) {
      Preconditions.checkArgument(
          pinput.size() == 1,
          "%s expected a single input PCollection, but received %d.",
          BeamZetaSqlCalcRel.class.getSimpleName(),
          pinput.size());
      PCollection<Row> upstream = pinput.get(0);

      final RexBuilder rexBuilder = getCluster().getRexBuilder();
      RexNode rex = rexBuilder.makeCall(SqlStdOperatorTable.ROW, getProgram().getProjectList());

      final RexNode condition = getProgram().getCondition();
      if (condition != null) {
        rex =
            rexBuilder.makeCall(
                SqlStdOperatorTable.CASE, condition, rex, rexBuilder.makeNullLiteral(getRowType()));
      }

      BeamSqlPipelineOptions options =
          pinput.getPipeline().getOptions().as(BeamSqlPipelineOptions.class);
      Schema outputSchema = CalciteUtils.toSchema(getRowType());
      CalcFn calcFn =
          new CalcFn(
              context.toSql(getProgram(), rex).toSqlString(DIALECT).getSql(),
              upstream.getSchema(),
              outputSchema,
              options.getZetaSqlDefaultTimezone(),
              options.getVerifyRowValues());

      // validate prepared expressions
      calcFn.setup();

      return upstream.apply(ParDo.of(calcFn)).setRowSchema(outputSchema);
    }
  }

  /**
   * {@code CalcFn} is the executor for a {@link BeamZetaSqlCalcRel} step. The implementation is
   * based on the {@code ZetaSQL} expression evaluator.
   */
  private static class CalcFn extends DoFn<Row, Row> {
    private final String sql;
    private final Schema inputSchema;
    private final Schema outputSchema;
    private final String defaultTimezone;
    private final boolean verifyRowValues;
    private transient PreparedExpression exp;

    CalcFn(
        String sql,
        Schema inputSchema,
        Schema outputSchema,
        String defaultTimezone,
        boolean verifyRowValues) {
      this.sql = sql;
      this.inputSchema = inputSchema;
      this.outputSchema = outputSchema;
      this.defaultTimezone = defaultTimezone;
      this.verifyRowValues = verifyRowValues;
    }

    @Setup
    public void setup() {
      // TODO[BEAM-9182]: support parameters in expression evaluation
      // Query parameters are not set because they have already been substituted.
      AnalyzerOptions options =
          SqlAnalyzer.getAnalyzerOptions(QueryParameters.ofNone(), defaultTimezone);
      for (int i = 0; i < inputSchema.getFieldCount(); i++) {
        options.addExpressionColumn(
            columnName(i),
            ZetaSqlBeamTranslationUtils.beamFieldTypeToZetaSqlType(
                inputSchema.getField(i).getType()));
      }

      exp = new PreparedExpression(sql);
      exp.prepare(options);
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      Map<String, Value> columns = new HashMap<>();
      Row row = c.element();
      for (int i = 0; i < inputSchema.getFieldCount(); i++) {
        columns.put(
            columnName(i),
            ZetaSqlBeamTranslationUtils.javaObjectToZetaSqlValue(
                row.getBaseValue(i, Object.class), inputSchema.getField(i).getType()));
      }

      // TODO[BEAM-9182]: support parameters in expression evaluation
      // The map is empty because parameters in the query string have already been substituted.
      Map<String, Value> params = Collections.emptyMap();

      Value v = exp.execute(columns, params);
      System.out.println(exp);
      if (!v.isNull()) {
        Row outputRow =
            ZetaSqlBeamTranslationUtils.zetaSqlStructValueToBeamRow(
                v, outputSchema, verifyRowValues);
        c.output(outputRow);
      }
    }

    @Teardown
    public void teardown() {
      exp.close();
    }
  }
}
