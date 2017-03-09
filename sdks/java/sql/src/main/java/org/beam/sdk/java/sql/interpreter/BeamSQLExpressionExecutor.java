package org.beam.sdk.java.sql.interpreter;

import java.io.Serializable;
import java.util.List;

import org.beam.sdk.java.sql.schema.BeamSQLRow;

public interface BeamSQLExpressionExecutor extends Serializable {

  public void prepare();

  public List<Object> execute(BeamSQLRow inputRecord);

  public void close();
}
