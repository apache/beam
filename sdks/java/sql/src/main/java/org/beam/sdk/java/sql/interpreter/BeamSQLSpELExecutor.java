package org.beam.sdk.java.sql.interpreter;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.beam.sdk.java.sql.planner.BeamSqlUnsupportedException;
import org.beam.sdk.java.sql.rel.BeamFilterRel;
import org.beam.sdk.java.sql.rel.BeamProjectRel;
import org.beam.sdk.java.sql.rel.BeamRelNode;
import org.beam.sdk.java.sql.schema.BeamSQLRow;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.SpelParserConfiguration;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

public class BeamSQLSpELExecutor implements BeamSQLExpressionExecutor {
  /**
   * 
   */
  private static final long serialVersionUID = 6777232573390074408L;

  private List<String> spelString;
  private List<Expression> spelExpressions;

  public BeamSQLSpELExecutor(BeamRelNode relNode) {
    this.spelString = new ArrayList<>();
    if (relNode instanceof BeamFilterRel) {
      String filterSpEL = CalciteToSpEL
          .rexcall2SpEL((RexCall) ((BeamFilterRel) relNode).getCondition());
      spelString.add(filterSpEL);
    } else if (relNode instanceof BeamProjectRel) {
      List<ProjectRule> projectRules = createProjectRules((BeamProjectRel) relNode);
      for (int idx = 0; idx < projectRules.size(); ++idx) {
//        if (projectRules.get(idx).getType().equals(ProjectType.RexCall)
//            || projectRules.get(idx).getType().equals(ProjectType.RexLiteral)) {
          spelString.add(projectRules.get(idx).getProjectExp());
//        } else {
//          spelString.add(null); // TODO
//        }
      }
    } else {
      throw new BeamSqlUnsupportedException(
          String.format("%s is not supported yet", relNode.getClass().toString()));
    }
  }

  @Override
  public void prepare() {
    this.spelExpressions = new ArrayList<>();

    SpelParserConfiguration config = new SpelParserConfiguration(true, true);
    ExpressionParser parser = new SpelExpressionParser(config);
    for (String el : spelString) {
      spelExpressions.add(parser.parseExpression(el));
    }
  }

  @Override
  public List<Object> execute(BeamSQLRow inputRecord) {
    StandardEvaluationContext inContext = new StandardEvaluationContext();
    inContext.setVariable("in", inputRecord);

    List<Object> results = new ArrayList<>();
    for (Expression ep : spelExpressions) {
      results.add(ep.getValue(inContext));
    }
    return results;
  }

  @Override
  public void close() {

  }

  private List<ProjectRule> createProjectRules(BeamProjectRel projectRel) {
    List<ProjectRule> rules = new ArrayList<>();

    List<RexNode> exps = projectRel.getProjects();

    for (int idx = 0; idx < exps.size(); ++idx) {
      RexNode node = exps.get(idx);
      ProjectRule rule = new ProjectRule();

      if (node instanceof RexLiteral) {
        rule.setType(ProjectType.RexLiteral);
        rule.setProjectExp(((RexLiteral) node).getValue() + "");
      }else{
        
      if (node instanceof RexInputRef) {
        rule.setType(ProjectType.RexInputRef);
//        rule.setSourceIndex( ((RexInputRef) node).getIndex() );
        rule.setProjectExp("#in.getFieldValue("+((RexInputRef) node).getIndex()+")");
      }
      if (node instanceof RexCall) {
        rule.setType(ProjectType.RexCall);
        rule.setProjectExp(CalciteToSpEL.rexcall2SpEL((RexCall) node));
      }
      rules.add(rule);
    }
    }

    return rules;
  }

}
