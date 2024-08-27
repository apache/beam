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


class MobileGamingCommands {

  private TestScripts testScripts
  private String testRunId

  private static final INPUT_GAMING_DATA = "gs://dataflow-samples/game/5000_gaming_data.csv"

  public static final RUNNERS = [DirectRunner: "direct-runner",
    DataflowRunner: "dataflow-runner",
    SparkRunner: "spark-runner",
    FlinkRunner: "flink-runner"]

  public static final EXECUTION_TIMEOUT_IN_MINUTES = 40

  // Lists used to verify team names generated in the LeaderBoard example.
  // This list should be kept sync with COLORS in org.apache.beam.examples.complete.game.injector.Injector.
  public static final COLORS = new ArrayList<>(Arrays.asList(
    "Magenta",
    "AliceBlue",
    "Almond",
    "Amaranth",
    "Amber",
    "Amethyst",
    "AndroidGreen",
    "AntiqueBrass",
    "Fuchsia",
    "Ruby",
    "AppleGreen",
    "Apricot",
    "Aqua",
    "ArmyGreen",
    "Asparagus",
    "Auburn",
    "Azure",
    "Banana",
    "Beige",
    "Bisque",
    "BarnRed",
    "BattleshipGrey"))

  public String getUserScoreOutputName(String runner){
    return "java-userscore-result-${RUNNERS[runner]}/${testRunId}/output"
  }

  public String getHourlyTeamScoreOutputName(String runner){
    return "java-hourlyteamscore-result-${RUNNERS[runner]}/${testRunId}/output"
  }

  public String createPipelineCommand(String exampleName, String runner, String jobName='', String className=null){
    if (className == null) {
      className = exampleName
    }
    return """mvn compile exec:java -q \
      -Dmaven.wagon.http.retryHandler.class=default \
      -Dmaven.wagon.http.retryHandler.count=5 \
      -Dmaven.wagon.http.pool=false \
      -Dmaven.wagon.httpconnectionManager.ttlSeconds=120 \
      -Dhttp.keepAlive=false \
      -Dexec.mainClass=org.apache.beam.examples.complete.game.${className} \
      -Dexec.args=\"${getArgs(exampleName, runner, jobName)}\" \
      -P${RUNNERS[runner]}"""
  }

  public String createInjectorCommand(){
    return """mvn compile exec:java \
      -Dexec.mainClass=org.apache.beam.examples.complete.game.injector.Injector \
      -Dexec.args=\"${testScripts.gcpProject()} ${testScripts.pubsubTopic()} none\""""
  }


  private String getArgs(String exampleName, String runner, String jobName){
    def args
    switch (exampleName) {
      case "UserScore":
        args = getUserScoreArgs(runner)
        break
      case "HourlyTeamScore":
        args = getHourlyTeamScoreArgs(runner)
        break
      case "LeaderBoard":
        args = getLeaderBoardArgs(runner, jobName)
        break
      case "LeaderBoardWithStreamingEngine":
        args = getLeaderBoardWithStreamingEngineArgs(runner, jobName)
        break
      case "GameStats":
        args = getGameStatsArgs(runner, jobName)
        break
      default:
        testScripts.error("Cannot find example ${exampleName} in archetypes.")
    }

    StringBuilder exampleArgs = new StringBuilder("--tempLocation=gs://${testScripts.gcsBucket()}/tmp --runner=${runner} ")
    args.each{argName, argValue -> exampleArgs.append("--${argName}=${argValue} ")}
    return exampleArgs
  }

  private Map getUserScoreArgs(String runner){
    if(runner == "DataflowRunner"){
      return [input: INPUT_GAMING_DATA,
        project: testScripts.gcpProject(),
        region: testScripts.gcpRegion(),
        output: "gs://${testScripts.gcsBucket()}/${getUserScoreOutputName(runner)}"]
    }
    return [input: INPUT_GAMING_DATA,
      output: "${getUserScoreOutputName(runner)}"]
  }

  private Map getHourlyTeamScoreArgs(String runner){
    if(runner == "DataflowRunner"){
      return [input: INPUT_GAMING_DATA,
        project: testScripts.gcpProject(),
        region: testScripts.gcpRegion(),
        output: "gs://${testScripts.gcsBucket()}/${getHourlyTeamScoreOutputName(runner)}"]
    }
    return [input: INPUT_GAMING_DATA,
      output: "${getHourlyTeamScoreOutputName(runner)}"]
  }

  private Map getLeaderBoardArgs(String runner, String jobName){
    def args = [project: testScripts.gcpProject(),
      dataset: testScripts.bqDataset(),
      topic: "projects/${testScripts.gcpProject()}/topics/${testScripts.pubsubTopic()}",
      leaderBoardTableName: "leaderboard_${runner}",
      teamWindowDuration: 5,
      jobName: jobName]
    if (runner == "DataflowRunner") {
      args["region"] = testScripts.gcpRegion()
    }
    return args
  }

  private Map getLeaderBoardWithStreamingEngineArgs(String runner, String jobName){
    def args = [project: testScripts.gcpProject(),
            dataset: testScripts.bqDataset(),
            topic: "projects/${testScripts.gcpProject()}/topics/${testScripts.pubsubTopic()}",
            leaderBoardTableName: "leaderboard_${runner}",
            teamWindowDuration: 5,
            jobName: jobName,
            experiments: "enable_streaming_engine"]
    if (runner == "DataflowRunner") {
      args["region"] = testScripts.gcpRegion()
    }
    return args
  }

  private Map getGameStatsArgs(String runner, String jobName){
    def args = [project: testScripts.gcpProject(),
      dataset: testScripts.bqDataset(),
      topic: "projects/${testScripts.gcpProject()}/topics/${testScripts.pubsubTopic()}",
      fixedWindowDuration: 5,
      userActivityWindowDuration: 5,
      sessionGap: 1,
      gameStatsTablePrefix: "gamestats_${runner}",
      jobName: jobName]
    if (runner == "DataflowRunner") {
      args["region"] = testScripts.gcpRegion()
    }
    return args
  }
}
