package org.beam.sdk.java.sql.transform;

import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.Setup;
import org.beam.sdk.java.sql.schema.BeamSQLRecordType;
import org.beam.sdk.java.sql.schema.BeamSQLRow;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.SpelParserConfiguration;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

public class BeamSQLProjectFn extends DoFn<BeamSQLRow, BeamSQLRow> {

  /**
   * 
   */
  private static final long serialVersionUID = -1046605249999014608L;
  private String stepName;
  private BeamSQLRecordType recordType;
  private List<ProjectRule> rules;

  private List<Expression> ruleExps;

  public BeamSQLProjectFn(String stepName, BeamSQLRecordType recordType, List<ProjectRule> rules) {
    super();
    this.stepName = stepName;
    this.recordType = recordType;
    this.rules = rules;
  }

  @Setup
  public void setup() {
    ruleExps = new ArrayList<>(rules.size());
    SpelParserConfiguration config = new SpelParserConfiguration(true, true);
    ExpressionParser parser = new SpelExpressionParser(config);
    for (int idx = 0; idx < rules.size(); ++idx) {
      if (rules.get(idx).getType().equals(ProjectType.RexCall)
          || rules.get(idx).getType().equals(ProjectType.RexLiteral)) {
        ruleExps.add(parser.parseExpression(rules.get(idx).getProjectExp()));
      } else {
        ruleExps.add(null);
      }
    }
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    StandardEvaluationContext mapContext = new StandardEvaluationContext();
    mapContext.setVariable("map", c.element());

    BeamSQLRow outRow = new BeamSQLRow(recordType);
    for (int idx = 0; idx < rules.size(); ++idx) {
      ProjectRule rule = rules.get(idx);
      switch (rule.getType()) {
      case RexInputRef:
        outRow.addField(recordType.getFieldsName().get(idx),
            c.element().getFieldValue(rule.getSourceIndex()));
        break;
      case RexCall:
      case RexLiteral:
        outRow.addField(recordType.getFieldsName().get(idx),
            ruleExps.get(idx).getValue(mapContext));
        break;
      default:
        break;
      }
    }

    c.output(outRow);
  }

}
