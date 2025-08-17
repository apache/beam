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

int retries = 5
int waitTime = 15 // seconds
def outputPath = "gs://${t.gcsBucket()}/${mobileGamingCommands.getUserScoreOutputName(runner)}"
def outputFound = false
for (int i = 0; i < retries; i++) {
  def files = t.run("gsutil ls ${outputPath}*")
  if (files?.trim()) {
    outputFound = true
    break
  }
  t.intent("Output not found yet. Waiting ${waitTime}s...")
  Thread.sleep(waitTime * 1000)
}

if (!outputFound) {
  throw new RuntimeException("No output files found for HourlyTeamScore after ${retries * waitTime} seconds.")
}

command_output_text = t.run "gsutil cat ${outputPath}* | grep user19_BananaWallaby"
t.see "total_score: 231, user: user19_BananaWallaby", command_output_text
t.success("UserScore successfully run on DataflowRunner.")
t.run "gsutil rm gs://${t.gcsBucket()}/${mobileGamingCommands.getUserScoreOutputName(runner)}*"


/**
 * Run the HourlyTeamScore example on DataflowRunner
 * */

mobileGamingCommands = new MobileGamingCommands(testScripts: t, testRunId: UUID.randomUUID().toString())

t.intent("Running: HourlyTeamScore example on DataflowRunner")
t.run(mobileGamingCommands.createPipelineCommand("HourlyTeamScore", runner))

outputPath = "gs://${t.gcsBucket()}/${mobileGamingCommands.getHourlyTeamScoreOutputName(runner)}"
outputFound = false
for (int i = 0; i < retries; i++) {
  def files = t.run("gsutil ls ${outputPath}*")
  if (files?.trim()) {
    outputFound = true
    break
  }
  t.intent("Output not found yet. Waiting ${waitTime}s...")
  Thread.sleep(waitTime * 1000)
}

if (!outputFound) {
  throw new RuntimeException("No output files found for UserScore after ${retries * waitTime} seconds.")
}

command_output_text = t.run "gsutil cat ${outputPath}* | grep AzureBilby "
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

    def dataset = t.bqDataset()
    def userTable = "leaderboard_DataflowRunner_user"
    def teamTable = "leaderboard_DataflowRunner_team"
    def userSchema = [
            "user:STRING",
            "total_score:INTEGER",
            "processing_time:STRING"
    ].join(",")
    def teamSchema = [
            "team:STRING",
            "total_score:INTEGER",
            "window_start:STRING",
            "processing_time:STRING",
            "timing:STRING"
    ].join(",")

    String tables = t.run("bq query --use_legacy_sql=false 'SELECT table_name FROM ${dataset}.INFORMATION_SCHEMA.TABLES'")

    if (!tables.contains(userTable)) {
      t.intent("Creating table: ${userTable}")
      t.run("bq mk --table ${dataset}.${userTable} ${userSchema}")
    }
    if (!tables.contains(teamTable)) {
      t.intent("Creating table: ${teamTable}")
      t.run("bq mk --table ${dataset}.${teamTable} ${teamSchema}")
    }

    // Verify that the tables have been created successfully
    tables = t.run("bq query --use_legacy_sql=false 'SELECT table_name FROM ${dataset}.INFORMATION_SCHEMA.TABLES'")
    while (!tables.contains(userTable) || !tables.contains(teamTable)) {
      sleep(3000)
      tables = t.run("bq query --use_legacy_sql=false 'SELECT table_name FROM ${dataset}.INFORMATION_SCHEMA.TABLES'")
    }
    println "Tables ${userTable} and ${teamTable} created successfully."

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
      try {
        tables = t.run "bq query --use_legacy_sql=false SELECT table_name FROM ${dataset}.INFORMATION_SCHEMA.TABLES"
        if (tables.contains(userTable) && tables.contains(teamTable)) {
          query_result = t.run """bq query --batch "SELECT user FROM [${dataset}.${userTable}] LIMIT 10\""""
          if (t.seeAnyOf(mobileGamingCommands.COLORS, query_result)) {
            isSuccess = true
            break
          }
        }
      } catch (Exception e) {
        println "Warning: Exception while checking tables: ${e.message}"
        println "Retrying..."
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

    tables = t.run("bq query --use_legacy_sql=false 'SELECT table_name FROM ${dataset}.INFORMATION_SCHEMA.TABLES'")
    if (tables.contains(userTable)) {
      t.run("bq rm -f -t ${dataset}.${userTable}")
    }
    if (tables.contains(teamTable)) {
      t.run("bq rm -f -t ${dataset}.${teamTable}")
    }

    // It will take couple seconds to clean up tables.
    // This loop makes sure tables are completely deleted before running the pipeline
    tables = t.run("bq query --use_legacy_sql=false 'SELECT table_name FROM ${dataset}.INFORMATION_SCHEMA.TABLES'")
    while (tables.contains(userTable) || tables.contains(teamTable)) {
      sleep(3000)
      tables = t.run("bq query --use_legacy_sql=false 'SELECT table_name FROM ${dataset}.INFORMATION_SCHEMA.TABLES'")
    }
  }
}

new LeaderBoardRunner().run(runner, t, mobileGamingCommands, false)
new LeaderBoardRunner().run(runner, t, mobileGamingCommands, true)

t.done()
