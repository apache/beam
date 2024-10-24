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

t.describe ('Run Apache Beam Java SDK Mobile Gaming Examples - Dataflow')

QuickstartArchetype.generate(t)

def runner = "DataflowRunner"
String command_output_text

/**
 *  Run the UserScore example on DataflowRunner
 * */

mobileGamingCommands = new MobileGamingCommands(testScripts: t, testRunId: UUID.randomUUID().toString())

t.intent("Running: UserScore example on DataflowRunner")
t.run(mobileGamingCommands.createPipelineCommand("UserScore", runner))
command_output_text = t.run "gsutil cat gs://${t.gcsBucket()}/${mobileGamingCommands.getUserScoreOutputName(runner)}* | grep user19_BananaWallaby"
t.see "total_score: 231, user: user19_BananaWallaby", command_output_text
t.success("UserScore successfully run on DataflowRunner.")
t.run "gsutil rm gs://${t.gcsBucket()}/${mobileGamingCommands.getUserScoreOutputName(runner)}*"


/**
 * Run the HourlyTeamScore example on DataflowRunner
 * */

mobileGamingCommands = new MobileGamingCommands(testScripts: t, testRunId: UUID.randomUUID().toString())

t.intent("Running: HourlyTeamScore example on DataflowRunner")
t.run(mobileGamingCommands.createPipelineCommand("HourlyTeamScore", runner))
command_output_text = t.run "gsutil cat gs://${t.gcsBucket()}/${mobileGamingCommands.getHourlyTeamScoreOutputName(runner)}* | grep AzureBilby "
t.see "total_score: 2788, team: AzureBilby", command_output_text
t.success("HourlyTeamScore successfully run on DataflowRunner.")
t.run "gsutil rm gs://${t.gcsBucket()}/${mobileGamingCommands.getHourlyTeamScoreOutputName(runner)}*"


/**
 * Run the LeaderBoard example on DataflowRunner with and without Streaming Engine
 * */
class LeaderBoardRunner {
  def run(runner, TestScripts t, MobileGamingCommands mobileGamingCommands, boolean useStreamingEngine) {
    t.intent("Running: LeaderBoard example on DataflowRunner" +
            (useStreamingEngine ? " with Streaming Engine" : ""))
    t.run("bq rm -f -t ${t.bqDataset()}.leaderboard_DataflowRunner_user")
    t.run("bq rm -f -t ${t.bqDataset()}.leaderboard_DataflowRunner_team")
    // It will take couple seconds to clean up tables.
    // This loop makes sure tables are completely deleted before running the pipeline
    String tables = ""
    while ({
      sleep(3000)
      tables = t.run("bq query SELECT table_id FROM ${t.bqDataset()}.__TABLES_SUMMARY__")
      tables.contains("leaderboard_${}_user") || tables.contains("leaderboard_${runner}_team")
    }());

    def InjectorThread = Thread.start() {
      t.run(mobileGamingCommands.createInjectorCommand())
    }

    String jobName = "leaderboard-validation-" + new Date().getTime() + "-" + new Random().nextInt(1000)
    def LeaderBoardThread = Thread.start() {
      if (useStreamingEngine) {
        t.run(mobileGamingCommands.createPipelineCommand(
                "LeaderBoardWithStreamingEngine", runner, jobName, "LeaderBoard"))
      } else {
        t.run(mobileGamingCommands.createPipelineCommand("LeaderBoard", runner, jobName))
      }
    }

    t.run("gcloud dataflow jobs list | grep pyflow-wordstream-candidate | grep Running | cut -d' ' -f1")

    // verify outputs in BQ tables
    def startTime = System.currentTimeMillis()
    def isSuccess = false
    String query_result = ""
    while ((System.currentTimeMillis() - startTime) / 60000 < mobileGamingCommands.EXECUTION_TIMEOUT_IN_MINUTES) {
      tables = t.run "bq query SELECT table_id FROM ${t.bqDataset()}.__TABLES_SUMMARY__"
      if (tables.contains("leaderboard_${runner}_user") && tables.contains("leaderboard_${runner}_team")) {
        query_result = t.run """bq query --batch "SELECT user FROM [${t.gcpProject()}:${
          t.bqDataset()
        }.leaderboard_${runner}_user] LIMIT 10\""""
        if (t.seeAnyOf(mobileGamingCommands.COLORS, query_result)) {
          isSuccess = true
          break
        }
      }
      println "Waiting for pipeline to produce more results..."
      sleep(60000) // wait for 1 min
    }
    InjectorThread.stop()
    LeaderBoardThread.stop()
    t.run("""RUNNING_JOB=`gcloud dataflow jobs list | grep ${jobName} | grep Running | cut -d' ' -f1`
if [ ! -z "\${RUNNING_JOB}" ] 
  then 
    gcloud dataflow jobs cancel \${RUNNING_JOB}
  else 
    echo "Job '${jobName}' is not running."
fi 
""")

    if (!isSuccess) {
      t.error("FAILED: Failed running LeaderBoard on DataflowRunner" +
              (useStreamingEngine ? " with Streaming Engine" : ""))
    }
    t.success("LeaderBoard successfully run on DataflowRunner." + (useStreamingEngine ? " with Streaming Engine" : ""))
  }
}

new LeaderBoardRunner().run(runner, t, mobileGamingCommands, false)
new LeaderBoardRunner().run(runner, t, mobileGamingCommands, true)

t.done()
