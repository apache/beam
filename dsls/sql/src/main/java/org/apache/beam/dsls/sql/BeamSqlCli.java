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
package org.apache.beam.dsls.sql;

//import static org.apache.beam.dsls.sql.BeamSqlEnv.planner;

import org.apache.beam.dsls.sql.rel.BeamRelNode;
import org.apache.beam.dsls.sql.schema.BeamSqlRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;
import org.apache.calcite.plan.RelOptUtil;

/**
 * {@link BeamSqlCli} provides methods to execute Beam SQL with an interactive client.
 */
@Experimental
public class BeamSqlCli {
  /**
   * Returns a human readable representation of the query execution plan.
   */
//<<<<<<< HEAD
  public static String explainQuery(String sqlString, BeamSqlEnv sqlEnv) throws Exception {
//    BeamRelNode exeTree = planner.convertToBeamRel(sqlString);
//=======
//  public static String explainQuery(String sqlString, BeamSqlEnv sqlEnv)
//      throws ValidationException, RelConversionException, SqlParseException {
    BeamRelNode exeTree = sqlEnv.planner.convertToBeamRel(sqlString);
//>>>>>>> eb5852b... restrict the scope of BeamSqlEnv
    String beamPlan = RelOptUtil.toString(exeTree);
    return beamPlan;
  }

  /**
   * compile SQL, and return a {@link Pipeline}.
   */
  public static PCollection<BeamSqlRow> compilePipeline(String sqlStatement, BeamSqlEnv sqlEnv)
      throws Exception{
    PipelineOptions options = PipelineOptionsFactory.fromArgs(new String[] {}).withValidation()
        .as(PipelineOptions.class); // FlinkPipelineOptions.class
    options.setJobName("BeamPlanCreator");
    Pipeline pipeline = Pipeline.create(options);

    return compilePipeline(sqlStatement, pipeline, sqlEnv);
  }

  /**
   * compile SQL, and return a {@link Pipeline}.
   */
  public static PCollection<BeamSqlRow> compilePipeline(String sqlStatement, Pipeline basePipeline
      , BeamSqlEnv sqlEnv) throws Exception{
    PCollection<BeamSqlRow> resultStream =
//<<<<<<< HEAD
//        planner.compileBeamPipeline(sqlStatement, basePipeline);
//=======
        sqlEnv.planner.compileBeamPipeline(sqlStatement, basePipeline, sqlEnv);
//>>>>>>> eb5852b... restrict the scope of BeamSqlEnv
    return resultStream;
  }
}
