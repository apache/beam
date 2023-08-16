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
package org.apache.beam.examples.complete.game.injector;

import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.model.PublishRequest;
import com.google.api.services.pubsub.model.PubsubMessage;
import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.apache.beam.examples.complete.game.utils.GameConstants;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

/**
 * This is a generator that simulates usage data from a mobile game, and either publishes the data
 * to a pubsub topic or writes it to a file.
 *
 * <p>The general model used by the generator is the following. There is a set of teams with team
 * members. Each member is scoring points for their team. After some period, a team will dissolve
 * and a new one will be created in its place. There is also a set of 'Robots', or spammer users.
 * They hop from team to team. The robots are set to have a higher 'click rate' (generate more
 * events) than the regular team members.
 *
 * <p>Each generated line of data has the following form:
 * username,teamname,score,timestamp_in_ms,readable_time e.g.:
 * user2_AsparagusPig,AsparagusPig,10,1445230923951,2015-11-02 09:09:28.224
 *
 * <p>The Injector writes either to a PubSub topic, or a file. It will use the PubSub topic if
 * specified. It takes the following arguments: {@code Injector project-name (topic-name|none)
 * (filename|none)}.
 *
 * <p>To run the Injector in the mode where it publishes to PubSub, you will need to authenticate
 * locally using project-based service account credentials to avoid running over PubSub quota. See
 * https://developers.google.com/identity/protocols/application-default-credentials for more
 * information on using service account credentials. Set the GOOGLE_APPLICATION_CREDENTIALS
 * environment variable to point to your downloaded service account credentials before starting the
 * program, e.g.: {@code export GOOGLE_APPLICATION_CREDENTIALS=/path/to/your/credentials-key.json}.
 * If you do not do this, then your injector will only run for a few minutes on your 'user account'
 * credentials before you will start to see quota error messages like: "Request throttled due to
 * user QPS limit being reached", and see this exception:
 * ".com.google.api.client.googleapis.json.GoogleJsonResponseException: 429 Too Many Requests". Once
 * you've set up your credentials, run the Injector like this":
 *
 * <pre>{@code
 * Injector <project-name> <topic-name> none
 * }</pre>
 *
 * The pubsub topic will be created if it does not exist.
 *
 * <p>To run the injector in write-to-file-mode, set the topic name to "none" and specify the
 * filename:
 *
 * <pre>{@code
 * Injector <project-name> none <filename>
 * }</pre>
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
class Injector {
  private static Pubsub pubsub;
  private static Random random = new Random();
  private static String topic;
  private static String project;

  // QPS ranges from 800 to 1000.
  private static final int MIN_QPS = 800;
  private static final int QPS_RANGE = 200;
  // How long to sleep, in ms, between creation of the threads that make API requests to PubSub.
  private static final int THREAD_SLEEP_MS = 500;

  // Lists used to generate random team names.
  // If COLORS is changed, please also make changes in
  // release/src/main/groovy/MobileGamingCommands.COLORS
  private static final ArrayList<String> COLORS =
      new ArrayList<>(
          Arrays.asList(
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
              "BattleshipGrey"));

  private static final ArrayList<String> ANIMALS =
      new ArrayList<>(
          Arrays.asList(
              "Echidna",
              "Koala",
              "Wombat",
              "Marmot",
              "Quokka",
              "Kangaroo",
              "Dingo",
              "Numbat",
              "Emu",
              "Wallaby",
              "CaneToad",
              "Bilby",
              "Possum",
              "Cassowary",
              "Kookaburra",
              "Platypus",
              "Bandicoot",
              "Cockatoo",
              "Antechinus"));

  // The list of live teams.
  private static ArrayList<TeamInfo> liveTeams = new ArrayList<>();

  // The total number of robots in the system.
  private static final int NUM_ROBOTS = 20;
  // Determines the chance that a team will have a robot team member.
  private static final int ROBOT_PROBABILITY = 3;
  private static final int NUM_LIVE_TEAMS = 15;
  private static final int BASE_MEMBERS_PER_TEAM = 5;
  private static final int MEMBERS_PER_TEAM = 15;
  private static final int MAX_SCORE = 20;
  private static final int LATE_DATA_RATE = 5 * 60 * 2; // Every 10 minutes
  private static final int BASE_DELAY_IN_MILLIS = 5 * 60 * 1000; // 5-10 minute delay
  private static final int FUZZY_DELAY_IN_MILLIS = 5 * 60 * 1000;

  // The minimum time a 'team' can live.
  private static final int BASE_TEAM_EXPIRATION_TIME_IN_MINS = 20;
  private static final int TEAM_EXPIRATION_TIME_IN_MINS = 20;

  /**
   * A class for holding team info: the name of the team, when it started, and the current team
   * members. Teams may but need not include one robot team member.
   */
  private static class TeamInfo {
    String teamName;
    long startTimeInMillis;
    int expirationPeriod;
    // The team might but need not include 1 robot. Will be non-null if so.
    String robot;
    int numMembers;

    private TeamInfo(String teamName, long startTimeInMillis, String robot) {
      this.teamName = teamName;
      this.startTimeInMillis = startTimeInMillis;
      // How long until this team is dissolved.
      this.expirationPeriod =
          random.nextInt(TEAM_EXPIRATION_TIME_IN_MINS) + BASE_TEAM_EXPIRATION_TIME_IN_MINS;
      this.robot = robot;
      // Determine the number of team members.
      numMembers = random.nextInt(MEMBERS_PER_TEAM) + BASE_MEMBERS_PER_TEAM;
    }

    String getTeamName() {
      return teamName;
    }

    String getRobot() {
      return robot;
    }

    long getStartTimeInMillis() {
      return startTimeInMillis;
    }

    long getEndTimeInMillis() {
      return startTimeInMillis + (expirationPeriod * 60L * 1000L);
    }

    String getRandomUser() {
      int userNum = random.nextInt(numMembers);
      return "user" + userNum + "_" + teamName;
    }

    int numMembers() {
      return numMembers;
    }

    @Override
    public String toString() {
      return "("
          + teamName
          + ", num members: "
          + numMembers()
          + ", starting at: "
          + startTimeInMillis
          + ", expires in: "
          + expirationPeriod
          + ", robot: "
          + robot
          + ")";
    }
  }

  /** Utility to grab a random element from an array of Strings. */
  private static String randomElement(ArrayList<String> list) {
    int index = random.nextInt(list.size());
    return list.get(index);
  }

  /**
   * Get and return a random team. If the selected team is too old w.r.t its expiration, remove it,
   * replacing it with a new team.
   */
  private static TeamInfo randomTeam(ArrayList<TeamInfo> list) {
    int index = random.nextInt(list.size());
    TeamInfo team = list.get(index);
    // If the selected team is expired, remove it and return a new team.
    long currTime = System.currentTimeMillis();
    if ((team.getEndTimeInMillis() < currTime) || team.numMembers() == 0) {
      System.out.println("\nteam " + team + " is too old; replacing.");
      System.out.println(
          "start time: "
              + team.getStartTimeInMillis()
              + ", end time: "
              + team.getEndTimeInMillis()
              + ", current time:"
              + currTime);
      removeTeam(index);
      // Add a new team in its stead.
      return addLiveTeam();
    } else {
      return team;
    }
  }

  /** Create and add a team. Possibly add a robot to the team. */
  private static synchronized TeamInfo addLiveTeam() {
    String teamName = randomElement(COLORS) + randomElement(ANIMALS);
    String robot = null;
    // Decide if we want to add a robot to the team.
    if (random.nextInt(ROBOT_PROBABILITY) == 0) {
      robot = "Robot-" + random.nextInt(NUM_ROBOTS);
    }
    // Create the new team.
    TeamInfo newTeam = new TeamInfo(teamName, System.currentTimeMillis(), robot);
    liveTeams.add(newTeam);
    System.out.println("[+" + newTeam + "]");
    return newTeam;
  }

  /** Remove a specific team. */
  private static synchronized void removeTeam(int teamIndex) {
    TeamInfo removedTeam = liveTeams.remove(teamIndex);
    System.out.println("[-" + removedTeam + "]");
  }

  /** Generate a user gaming event. */
  private static String generateEvent(Long currTime, int delayInMillis) {
    TeamInfo team = randomTeam(liveTeams);
    String teamName = team.getTeamName();
    String user;
    final int parseErrorRate = 900000;

    String robot = team.getRobot();
    // If the team has an associated robot team member...
    if (robot != null) {
      // Then use that robot for the message with some probability.
      // Set this probability to higher than that used to select any of the 'regular' team
      // members, so that if there is a robot on the team, it has a higher click rate.
      if (random.nextInt(team.numMembers() / 2) == 0) {
        user = robot;
      } else {
        user = team.getRandomUser();
      }
    } else { // No robot.
      user = team.getRandomUser();
    }
    String event = user + "," + teamName + "," + random.nextInt(MAX_SCORE);
    // Randomly introduce occasional parse errors.
    if (random.nextInt(parseErrorRate) == 0) {
      System.out.println("Introducing a parse error.");
      event = "THIS LINE REPRESENTS CORRUPT DATA AND WILL CAUSE A PARSE ERROR";
    }
    return addTimeInfoToEvent(event, currTime, delayInMillis);
  }

  /** Add time info to a generated gaming event. */
  private static String addTimeInfoToEvent(String message, Long currTime, int delayInMillis) {
    String eventTimeString = Long.toString((currTime - delayInMillis) / 1000 * 1000);
    // Add a (redundant) 'human-readable' date string to make the data semantics more clear.
    String dateString = GameConstants.DATE_TIME_FORMATTER.print(currTime);
    message = message + "," + eventTimeString + "," + dateString;
    return message;
  }

  /**
   * Publish 'numMessages' arbitrary events from live users with the provided delay, to a PubSub
   * topic.
   */
  public static void publishData(int numMessages, int delayInMillis) throws IOException {
    List<PubsubMessage> pubsubMessages = new ArrayList<>();

    for (int i = 0; i < Math.max(1, numMessages); i++) {
      Long currTime = System.currentTimeMillis();
      String message = generateEvent(currTime, delayInMillis);
      PubsubMessage pubsubMessage = new PubsubMessage().encodeData(message.getBytes("UTF-8"));
      pubsubMessage.setAttributes(
          ImmutableMap.of(
              GameConstants.TIMESTAMP_ATTRIBUTE,
              Long.toString((currTime - delayInMillis) / 1000 * 1000)));
      if (delayInMillis != 0) {
        System.out.println(pubsubMessage.getAttributes());
        System.out.println("late data for: " + message);
      }
      pubsubMessages.add(pubsubMessage);
    }

    PublishRequest publishRequest = new PublishRequest();
    publishRequest.setMessages(pubsubMessages);
    pubsub.projects().topics().publish(topic, publishRequest).execute();
  }

  /** Publish generated events to a file. */
  public static void publishDataToFile(String fileName, int numMessages, int delayInMillis)
      throws IOException {
    PrintWriter out =
        new PrintWriter(
            new OutputStreamWriter(
                new BufferedOutputStream(new FileOutputStream(fileName, true)), "UTF-8"));

    try {
      for (int i = 0; i < Math.max(1, numMessages); i++) {
        Long currTime = System.currentTimeMillis();
        String message = generateEvent(currTime, delayInMillis);
        out.println(message);
      }
    } catch (Exception e) {
      System.err.print("Error in writing generated events to file");
      e.printStackTrace();
    } finally {
      out.flush();
      out.close();
    }
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    if (args.length < 3) {
      System.out.println("Usage: Injector project-name (topic-name|none) (filename|none)");
      System.exit(1);
    }
    boolean writeToFile = false;
    boolean writeToPubsub = true;
    project = args[0];
    String topicName = args[1];
    String fileName = args[2];
    // The Injector writes either to a PubSub topic, or a file. It will use the PubSub topic if
    // specified; otherwise, it will try to write to a file.
    if ("none".equalsIgnoreCase(topicName)) {
      writeToFile = true;
      writeToPubsub = false;
    }
    if (writeToPubsub) {
      // Create the PubSub client.
      pubsub = InjectorUtils.getClient();
      // Create the PubSub topic as necessary.
      topic = InjectorUtils.getFullyQualifiedTopicName(project, topicName);
      InjectorUtils.createTopic(pubsub, topic);
      System.out.println("Injecting to topic: " + topic);
    } else {
      if ("none".equalsIgnoreCase(fileName)) {
        System.out.println("Filename not specified.");
        System.exit(1);
      }
      System.out.println("Writing to file: " + fileName);
    }
    System.out.println("Starting Injector");

    // Start off with some random live teams.
    while (liveTeams.size() < NUM_LIVE_TEAMS) {
      addLiveTeam();
    }

    // Publish messages at a rate determined by the QPS and Thread sleep settings.
    for (int i = 0; true; i++) {
      if (Thread.activeCount() > 10) {
        System.err.println("I'm falling behind!");
      }

      // Decide if this should be a batch of late data.
      final int numMessages;
      final int delayInMillis;
      if (i % LATE_DATA_RATE == 0) {
        // Insert delayed data for one user (one message only)
        delayInMillis = BASE_DELAY_IN_MILLIS + random.nextInt(FUZZY_DELAY_IN_MILLIS);
        numMessages = 1;
        System.out.println("DELAY(" + delayInMillis + ", " + numMessages + ")");
      } else {
        System.out.print(".");
        delayInMillis = 0;
        numMessages = MIN_QPS + random.nextInt(QPS_RANGE);
      }

      if (writeToFile) { // Won't use threading for the file write.
        publishDataToFile(fileName, numMessages, delayInMillis);
      } else { // Write to PubSub.
        // Start a thread to inject some data.
        new Thread(
                () -> {
                  try {
                    publishData(numMessages, delayInMillis);
                  } catch (IOException e) {
                    System.err.println(e);
                  }
                })
            .start();
      }

      // Wait before creating another injector thread.
      Thread.sleep(THREAD_SLEEP_MS);
    }
  }
}
