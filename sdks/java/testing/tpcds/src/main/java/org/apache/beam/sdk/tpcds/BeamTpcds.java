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
package org.apache.beam.sdk.tpcds;


/**
 * To execute this main() method, run the following example command from the command line.
 *
 * ./gradlew :sdks:java:testing:tpcds:run -Ptpcds.args="--dataSize=1G \
 *         --queries=3,26,55 \
 *         --tpcParallel=2 \
 *         --project=apache-beam-testing \
 *         --stagingLocation=gs://beamsql_tpcds_1/staging \
 *         --tempLocation=gs://beamsql_tpcds_2/temp \
 *         --runner=DataflowRunner \
 *         --region=us-west1 \
 *         --maxNumWorkers=10"
 *
 *
 * To run query using ZetaSQL planner (currently query96 can be run using ZetaSQL), set the plannerName as below. If not specified, the default planner is Calcite.
 *
 * ./gradlew :sdks:java:testing:tpcds:run -Ptpcds.args="--dataSize=1G \
 *         --queries=96 \
 *         --tpcParallel=2 \
 *         --plannerName=org.apache.beam.sdk.extensions.sql.zetasql.ZetaSQLQueryPlanner \
 *         --project=apache-beam-testing \
 *         --stagingLocation=gs://beamsql_tpcds_1/staging \
 *         --tempLocation=gs://beamsql_tpcds_2/temp \
 *         --runner=DataflowRunner \
 *         --region=us-west1 \
 *         --maxNumWorkers=10"
 */
public class BeamTpcds {
    /**
     * The main method can choose to run either SqlTransformRunner.runUsingSqlTransform() or BeamSqlEnvRunner.runUsingBeamSqlEnv()
     * Currently the former has better performance so it is chosen.
     *
     * @param args Command line arguments
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        SqlTransformRunner.runUsingSqlTransform(args);
    }
}
