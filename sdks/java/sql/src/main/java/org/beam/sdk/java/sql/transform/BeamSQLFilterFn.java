package org.beam.sdk.java.sql.transform;

import java.util.List;

import org.apache.beam.sdk.transforms.DoFn;
import org.beam.sdk.java.sql.interpreter.BeamSQLExpressionExecutor;
import org.beam.sdk.java.sql.schema.BeamSQLRow;

public class BeamSQLFilterFn extends DoFn<BeamSQLRow, BeamSQLRow> {
  /**
   * 
   */
  private static final long serialVersionUID = -1256111753670606705L;

  private String stepName;
  private BeamSQLExpressionExecutor executor;

  public BeamSQLFilterFn(String stepName, BeamSQLExpressionExecutor executor) {
    super();
    this.stepName = stepName;
    this.executor = executor;
  }

  @Setup
  public void setup() {
    executor.prepare();
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    BeamSQLRow in = c.element();

    List<Object> result = executor.execute(in);

    if ((Boolean)result.get(0)) {
      c.output(in);
    }
  }
  
  @Teardown
  public void close(){
    executor.close();
  }

}
