package org.beam.sdk.java.sql.transform;

import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.Setup;
import org.apache.beam.sdk.transforms.DoFn.Teardown;
import org.beam.sdk.java.sql.interpreter.BeamSQLExpressionExecutor;
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
  private BeamSQLExpressionExecutor executor;
  private BeamSQLRecordType outputRecordType;

  public BeamSQLProjectFn(String stepName, BeamSQLExpressionExecutor executor, BeamSQLRecordType outputRecordType) {
    super();
    this.stepName = stepName;
    this.executor = executor;
    this.outputRecordType = outputRecordType;
  }

  @Setup
  public void setup() {
    executor.prepare();
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    List<Object> results = executor.execute(c.element());
    
    BeamSQLRow outRow = new BeamSQLRow(outputRecordType);
    for(int idx=0; idx<results.size(); ++idx){
      outRow.addField(idx, results.get(idx));
    }
    
    c.output(outRow);
  }
  
  @Teardown
  public void close(){
    executor.close();
  }

}
