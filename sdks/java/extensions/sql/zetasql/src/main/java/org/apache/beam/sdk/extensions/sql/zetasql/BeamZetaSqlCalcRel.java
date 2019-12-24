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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.extensions.sql.impl.rel.AbstractBeamCalcRel;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.extensions.sql.meta.provider.bigquery.BeamBigQuerySqlDialect;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
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
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexProgram;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlDialect;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlIdentifier;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;

/**
 * TODO[BEAM-8630]: This class is currently a prototype and not used in runtime.
 *
 * <p>BeamRelNode to replace {@code Project} and {@code Filter} node based on the {@code ZetaSQL}
 * expression evaluator.
 */
@Internal
public class BeamZetaSqlCalcRel extends AbstractBeamCalcRel {

  private static final SqlDialect DIALECT = BeamBigQuerySqlDialect.DEFAULT;
  private final SqlImplementor.Context context;

  public BeamZetaSqlCalcRel(
      RelOptCluster cluster, RelTraitSet traits, RelNode input, RexProgram program) {
    super(cluster, traits, input, program);
    final IntFunction<SqlNode> fn =
        i ->
            new SqlIdentifier(
                getProgram().getInputRowType().getFieldList().get(i).getName(), SqlParserPos.ZERO);
    context = new SqlImplementor.SimpleContext(DIALECT, fn);
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

      final List<String> projects =
          getProgram().getProjectList().stream()
              .map(BeamZetaSqlCalcRel.this::unparseRexNode)
              .collect(Collectors.toList());
      final RexNode condition = getProgram().getCondition();

      // TODO[BEAM-8630]: validate sql expressions at pipeline construction time
      Schema outputSchema = CalciteUtils.toSchema(getRowType());
      CalcFn calcFn =
          new CalcFn(
              projects,
              condition == null ? null : unparseRexNode(condition),
              upstream.getSchema(),
              outputSchema);
      return upstream.apply(ParDo.of(calcFn)).setRowSchema(outputSchema);
    }
  }

  private String unparseRexNode(RexNode rex) {
    return context.toSql(getProgram(), rex).toSqlString(DIALECT).getSql();
  }

  /**
   * {@code CalcFn} is the executor for a {@link BeamZetaSqlCalcRel} step. The implementation is
   * based on the {@code ZetaSQL} expression evaluator.
   */
  private static class CalcFn extends DoFn<Row, Row> {
    private final List<String> projects;
    @Nullable private final String condition;
    private final Schema inputSchema;
    private final Schema outputSchema;
    private transient List<PreparedExpression> projectExps;
    @Nullable private transient PreparedExpression conditionExp;

    CalcFn(
        List<String> projects,
        @Nullable String condition,
        Schema inputSchema,
        Schema outputSchema) {
      Preconditions.checkArgument(projects.size() == outputSchema.getFieldCount());
      this.projects = ImmutableList.copyOf(projects);
      this.condition = condition;
      this.inputSchema = inputSchema;
      this.outputSchema = outputSchema;
    }

    @Setup
    public void setup() {
      AnalyzerOptions options = SqlAnalyzer.initAnalyzerOptions();
      for (Field field : inputSchema.getFields()) {
        options.addExpressionColumn(
            sanitize(field.getName()), ZetaSqlUtils.beamFieldTypeToZetaSqlType(field.getType()));
      }

      // TODO[BEAM-8630]: use a single PreparedExpression for all condition and projects
      projectExps = new ArrayList<>();
      for (String project : projects) {
        PreparedExpression projectExp = new PreparedExpression(sanitize(project));
        projectExp.prepare(options);
        projectExps.add(projectExp);
      }
      if (condition != null) {
        conditionExp = new PreparedExpression(sanitize(condition));
        conditionExp.prepare(options);
      }
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      Map<String, Value> columns = new HashMap<>();
      Row row = c.element();
      for (Field field : inputSchema.getFields()) {
        columns.put(
            sanitize(field.getName()),
            ZetaSqlUtils.javaObjectToZetaSqlValue(row.getValue(field.getName()), field.getType()));
      }

      // TODO[BEAM-8630]: support parameters in expression evaluation
      // The map is empty because parameters in the query string have already been substituted.
      Map<String, Value> params = Collections.emptyMap();

      if (conditionExp != null && !conditionExp.execute(columns, params).getBoolValue()) {
        return;
      }

      Row.Builder output = Row.withSchema(outputSchema);
      for (int i = 0; i < outputSchema.getFieldCount(); i++) {
        // TODO[BEAM-8630]: performance optimization by bundling the gRPC calls
        Value v = projectExps.get(i).execute(columns, params);
        output.addValue(
            ZetaSqlUtils.zetaSqlValueToJavaObject(v, outputSchema.getField(i).getType()));
      }
      c.output(output.build());
    }

    @Teardown
    public void teardown() {
      for (PreparedExpression projectExp : projectExps) {
        projectExp.close();
      }
      if (conditionExp != null) {
        conditionExp.close();
      }
    }

    // Replaces "$" with "_" because "$" is not allowed in a valid ZetaSQL identifier
    // (ZetaSQL identifier syntax: [A-Za-z_][A-Za-z_0-9]*)
    // TODO[BEAM-8630]: check if this is sufficient and correct, or even better fix this in Calcite
    private static String sanitize(String identifier) {
      return identifier.replaceAll("\\$", "_");
    }
  }
}
