#!groovy
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

t = new TestScripts(args)

/*
 * Run the mobile game examples on Dataflow.
 * https://beam.apache.org/get-started/mobile-gaming-example/
 */

t.describe ('Run Apache Beam Java SDK Mobile Gaming Examples using GCP BOM - Dataflow')

GoogleCloudPlatformBomArchetype.generate(t)

def runner = "DataflowRunner"
String command_output_text

/**
 *  Run the UserScore example on DataflowRunner
 * */

mobileGamingCommands = new MobileGamingCommands(testScripts: t, testRunId: UUID.randomUUID().toString())

t.intent("Running: UserScore example with Beam GCP BOM on DataflowRunner")
t.run(mobileGamingCommands.createPipelineCommand("UserScore", runner))
command_output_text = t.run "gsutil cat gs://${t.gcsBucket()}/${mobileGamingCommands.getUserScoreOutputName(runner)}* | grep user19_BananaWallaby"
t.see "total_score: 231, user: user19_BananaWallaby", command_output_text
t.success("UserScore successfully run on DataflowRunner.")
t.run "gsutil rm gs://${t.gcsBucket()}/${mobileGamingCommands.getUserScoreOutputName(runner)}*"


/**
 * Run the HourlyTeamScore example on DataflowRunner
 * */

mobileGamingCommands = new MobileGamingCommands(testScripts: t, testRunId: UUID.randomUUID().toString())

t.intent("Running: HourlyTeamScore example with Beam GCP BOM on DataflowRunner")
t.run(mobileGamingCommands.createPipelineCommand("HourlyTeamScore", runner))
command_output_text = t.run "gsutil cat gs://${t.gcsBucket()}/${mobileGamingCommands.getHourlyTeamScoreOutputName(runner)}* | grep AzureBilby "
t.see "total_score: 2788, team: AzureBilby", command_output_text
t.success("HourlyTeamScore successfully run on DataflowRunner.")
t.run "gsutil rm gs://${t.gcsBucket()}/${mobileGamingCommands.getHourlyTeamScoreOutputName(runner)}*"

new LeaderBoardRunner().run(runner, t, mobileGamingCommands, false)
new LeaderBoardRunner().run(runner, t, mobileGamingCommands, true)

t.done()
