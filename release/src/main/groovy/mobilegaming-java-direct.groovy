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
 * Run the mobile game examples on DirectRunner.
 * https://beam.apache.org/get-started/mobile-gaming-example/
 */

t.describe ('Run Apache Beam Java SDK Mobile Gaming Examples - Direct')

QuickstartArchetype.generate(t)

def runner = "DirectRunner"
String command_output_text

/**
 * Run the UserScore example with DirectRunner
 * */

mobileGamingCommands = new MobileGamingCommands(testScripts: t, testRunId: UUID.randomUUID().toString())

t.intent("Running: UserScore example on DirectRunner")
t.run(mobileGamingCommands.createPipelineCommand("UserScore", runner))
command_output_text = t.run "grep user19_BananaWallaby ${mobileGamingCommands.getUserScoreOutputName(runner)}* "
t.see "total_score: 231, user: user19_BananaWallaby", command_output_text
t.success("UserScore successfully run on DirectRunners.")


/**
 * Run the HourlyTeamScore example with DirectRunner
 * */

mobileGamingCommands = new MobileGamingCommands(testScripts: t, testRunId: UUID.randomUUID().toString())

t.intent("Running: HourlyTeamScore example on DirectRunner")
t.run(mobileGamingCommands.createPipelineCommand("HourlyTeamScore", runner))
command_output_text = t.run "grep AzureBilby ${mobileGamingCommands.getHourlyTeamScoreOutputName(runner)}* "
t.see "total_score: 2788, team: AzureBilby", command_output_text
t.success("HourlyTeamScore successfully run on DirectRunners.")


/**
 * Run the LeaderBoard example with DirectRunner
 * */

t.intent("Running: LeaderBoard example on DirectRunner")

def dataset = t.bqDataset()
def userTable = "leaderboard_DirectRunner_user"
def teamTable = "leaderboard_DirectRunner_team"
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

// Verify that the tables have been created
tables = t.run("bq query --use_legacy_sql=false 'SELECT table_name FROM ${dataset}.INFORMATION_SCHEMA.TABLES'")
while (!tables.contains(userTable) || !tables.contains(teamTable)) {
  sleep(3000)
  tables = t.run("bq query --use_legacy_sql=false 'SELECT table_name FROM ${dataset}.INFORMATION_SCHEMA.TABLES'")
}
println "Tables ${userTable} and ${teamTable} created successfully."

def InjectorThread = Thread.start() {
  t.run(mobileGamingCommands.createInjectorCommand())
}

jobName = "leaderboard-validation-" + new Date().getTime() + "-" + new Random().nextInt(1000)
def LeaderBoardThread = Thread.start() {
  t.run(mobileGamingCommands.createPipelineCommand("LeaderBoard", runner, jobName))
}

// verify outputs in BQ tables
def startTime = System.currentTimeMillis()
def isSuccess = false
String query_result = ""
while ((System.currentTimeMillis() - startTime)/60000 < mobileGamingCommands.EXECUTION_TIMEOUT_IN_MINUTES) {
  try {
    tables = t.run "bq query --use_legacy_sql=false SELECT table_name FROM ${dataset}.INFORMATION_SCHEMA.TABLES"
    if (tables.contains(userTable) && tables.contains(teamTable)) {
      query_result = t.run """bq query --batch "SELECT user FROM [${dataset}.${userTable}] LIMIT 10\""""
      if (t.seeAnyOf(mobileGamingCommands.COLORS, query_result)){
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

if(!isSuccess){
  t.error("FAILED: Failed running LeaderBoard on DirectRunner")
}
t.success("LeaderBoard successfully run on DirectRunner.")

t.done()
