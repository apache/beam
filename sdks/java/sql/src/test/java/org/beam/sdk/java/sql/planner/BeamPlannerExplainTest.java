package org.beam.sdk.java.sql.planner;

import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.junit.Test;

public class BeamPlannerExplainTest extends BasePlanner {

  @Test
  public void selectAll() throws ValidationException, RelConversionException, SqlParseException{
    String sql = "SELECT * FROM ORDER_DETAILS";
    runner.explainQuery(sql);
  }

}
