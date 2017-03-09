package org.beam.sdk.java.sql.transform;

import org.apache.beam.sdk.transforms.DoFn;
import org.beam.sdk.java.sql.schema.BeamSQLRow;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.SpelParserConfiguration;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

public class BeamSQLFilterFn extends DoFn<BeamSQLRow, BeamSQLRow> {
  /**
   * 
   */
  private static final long serialVersionUID = -1256111753670606705L;

  private String stepName;
  private String filterInString;
  private Expression expression;

  public BeamSQLFilterFn(String stepName, String filterInString) {
    super();
    this.stepName = stepName;
    this.filterInString = filterInString;
  }

  @Setup
  public void setup() {
    SpelParserConfiguration config = new SpelParserConfiguration(true, true);
    ExpressionParser parser = new SpelExpressionParser(config);
    expression = parser.parseExpression(filterInString);
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    BeamSQLRow map = c.element();

    StandardEvaluationContext mapContext = new StandardEvaluationContext();
    mapContext.setVariable("map", map);
    boolean trueValue = expression.getValue(mapContext, Boolean.class);

    if (trueValue) {
      c.output(map);
    }
  }

}
