package org.apache.beam.sdk.extensions.sql.impl.rel;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.impl.InnerBeamSqlEnv;
import org.apache.beam.sdk.values.BeamRecord;
import org.apache.beam.sdk.values.PCollection;

/**
 * Base class for rel test.
 */
public class BaseRelTest {
  public PCollection<BeamRecord> compilePipeline (
      String sql, Pipeline pipeline, InnerBeamSqlEnv sqlEnv) throws Exception {
    return sqlEnv.getPlanner().compileBeamPipeline(sql, pipeline, sqlEnv);
  }
}
