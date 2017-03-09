package org.beam.sdk.java.sql.transform;

import org.apache.beam.sdk.transforms.DoFn;
import org.beam.sdk.java.sql.schema.BeamSQLRow;

public class BeamSQLOutputToConsoleFn extends DoFn<BeamSQLRow, Void> {
  /**
   * 
   */
  private static final long serialVersionUID = -1256111753670606705L;

  private String stepName;

  public BeamSQLOutputToConsoleFn(String stepName) {
    super();
    this.stepName = stepName;
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    System.out.println("Output: " + c.element().getDataMap());
  }

}
