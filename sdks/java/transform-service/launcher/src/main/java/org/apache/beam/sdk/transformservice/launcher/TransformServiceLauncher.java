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
package org.apache.beam.sdk.transformservice.launcher;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.ByteStreams;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.Files;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A utility that can be used to manage a Beam Transform Service.
 *
 * <p>Can be either used programatically or as an executable jar.
 */
public class TransformServiceLauncher {

  private static final Logger LOG = LoggerFactory.getLogger(TransformServiceLauncher.class);

  private static final String DEFAULT_PROJECT_NAME = "apache.beam.transform.service";

  private static final String COMMAND_POSSIBLE_VALUES = "\"up\", \"down\" and \"ps\"";

  private static Map<String, TransformServiceLauncher> launchers = new HashMap<>();

  private List<String> dockerComposeStartCommandPrefix = new ArrayList<>();

  private Map<String, String> environmentVariables = new HashMap<>();

  // Amount of time (in milliseconds) to wait till the Docker Compose starts up.
  private static final int DEFAULT_START_WAIT_TIME = 25000;
  private static final int STATUS_LOGGER_WAIT_TIME = 3000;

  @SuppressWarnings("argument")
  private TransformServiceLauncher(@Nullable String projectName, int port) throws IOException {
    LOG.info("Initializing the Beam Transform Service {}.", projectName);

    String tmpDirLocation = System.getProperty("java.io.tmpdir");
    // We use Docker Compose project name as the name of the temporary directory to isolate
    // different transform service instances that may be running in the same machine.
    Path tmpDirPath = Paths.get(tmpDirLocation, projectName);
    java.nio.file.Files.createDirectories(tmpDirPath);

    String tmpDir = tmpDirPath.toFile().getAbsolutePath();

    File dockerComposeFile = Paths.get(tmpDir, "docker-compose.yml").toFile();
    try (FileOutputStream fout = new FileOutputStream(dockerComposeFile)) {
      ByteStreams.copy(getClass().getResourceAsStream("/docker-compose.yml"), fout);
    }

    File envFile = Paths.get(tmpDir, ".env").toFile();
    try (FileOutputStream fout = new FileOutputStream(envFile)) {
      ByteStreams.copy(getClass().getResourceAsStream("/.env"), fout);
    }

    File credentialsDir = Paths.get(tmpDir, "credentials_dir").toFile();
    LOG.info(
        "Creating a temporary directory for storing credentials: "
            + credentialsDir.getAbsolutePath());

    if (credentialsDir.exists()) {
      LOG.info("Reusing the existing credentials directory " + credentialsDir.getAbsolutePath());
    } else {
      if (!credentialsDir.mkdir()) {
        throw new IOException(
            "Could not create a temporary directory for storing credentials: "
                + credentialsDir.getAbsolutePath());
      }

      LOG.info("Copying the Google Application Default Credentials file.");

      File applicationDefaultCredentialsFileCopied =
          Paths.get(credentialsDir.getAbsolutePath(), "application_default_credentials.json")
              .toFile();

      boolean isWindows =
          System.getProperty("os.name").toLowerCase(Locale.ENGLISH).contains("windows");
      String applicationDefaultFilePathSuffix =
          isWindows
              ? "\\gcloud\\application_default_credentials.json"
              : "/.config/gcloud/application_default_credentials.json";
      String applicationDefaultFilePath =
          System.getProperty("user.home") + applicationDefaultFilePathSuffix;

      File applicationDefaultCredentialsFile = Paths.get(applicationDefaultFilePath).toFile();
      if (applicationDefaultCredentialsFile.exists()) {
        Files.copy(applicationDefaultCredentialsFile, applicationDefaultCredentialsFileCopied);
      } else {
        LOG.error(
            "GCP credentials will not be available for the transform service since the Google "
                + "Cloud application default credentials file could not be found at the expected "
                + "location {}.",
            applicationDefaultFilePath);
      }
    }

    // Setting environment variables used by the docker-compose.yml file.
    environmentVariables.put("CREDENTIALS_VOLUME", credentialsDir.getAbsolutePath());
    environmentVariables.put("TRANSFORM_SERVICE_PORT", String.valueOf(port));

    // Building the Docker Compose command.
    dockerComposeStartCommandPrefix.add("docker-compose");
    dockerComposeStartCommandPrefix.add("-p");
    dockerComposeStartCommandPrefix.add(projectName);
    dockerComposeStartCommandPrefix.add("-f");
    dockerComposeStartCommandPrefix.add(dockerComposeFile.getAbsolutePath());
  }

  public void setBeamVersion(String beamVersion) {
    environmentVariables.put("BEAM_VERSION", beamVersion);
  }

  public void setPythonExtraPackages(String pythonExtraPackages) {
    environmentVariables.put("$PYTHON_EXTRA_PACKAGES", pythonExtraPackages);
  }

  public static synchronized TransformServiceLauncher forProject(
      @Nullable String projectName, int port) throws IOException {
    if (projectName == null || projectName.isEmpty()) {
      projectName = DEFAULT_PROJECT_NAME;
    }
    if (!launchers.containsKey(projectName)) {
      launchers.put(projectName, new TransformServiceLauncher(projectName, port));
    }
    return launchers.get(projectName);
  }

  private void runDockerComposeCommand(String command) throws IOException {
    this.runDockerComposeCommand(command, null);
  }

  private void runDockerComposeCommand(String command, @Nullable File outputOverride)
      throws IOException {
    List<String> shellCommand = new ArrayList<>();
    shellCommand.addAll(dockerComposeStartCommandPrefix);
    shellCommand.add(command);
    System.out.println("Executing command: " + String.join(" ", command));
    ProcessBuilder processBuilder =
        new ProcessBuilder(shellCommand).redirectError(ProcessBuilder.Redirect.INHERIT);

    if (outputOverride != null) {
      processBuilder.redirectOutput(outputOverride);
    } else {
      processBuilder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
    }

    Map<String, String> env = processBuilder.environment();
    env.putAll(this.environmentVariables);

    processBuilder.start();

    try {
      this.wait(STATUS_LOGGER_WAIT_TIME);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public synchronized void start() throws IOException, TimeoutException {
    runDockerComposeCommand("up");
  }

  public synchronized void shutdown() throws IOException {
    runDockerComposeCommand("down");
  }

  public synchronized void status() throws IOException {
    runDockerComposeCommand("ps");
  }

  public synchronized void waitTillUp(int timeout) throws IOException, TimeoutException {
    timeout = timeout <= 0 ? DEFAULT_START_WAIT_TIME : timeout;
    String statusFileName = getStatus();

    long startTime = System.currentTimeMillis();
    while (System.currentTimeMillis() - startTime < timeout) {
      try {
        // We are just waiting for a local process. No need for exponential backoff.
        this.wait(1000);
      } catch (InterruptedException e) {
        // Ignore and retry.
      }

      String output = String.join(" ", java.nio.file.Files.readAllLines(Paths.get(statusFileName)));
      if (!output.isEmpty()) {
        if (output.contains("transform-service")) {
          // Transform service was started since we found matching logs.
          return;
        }
      }
    }

    throw new TimeoutException(
        "Transform Service did not start in " + timeout / 1000 + " seconds.");
  }

  private synchronized String getStatus() throws IOException {
    File outputOverride = File.createTempFile("output_override", null);
    runDockerComposeCommand("ps", outputOverride);

    return outputOverride.getAbsolutePath();
  }

  private static class ArgConfig {

    static final String PROJECT_NAME_ARG_NAME = "project_name";
    static final String COMMAND_ARG_NAME = "command";
    static final String PORT_ARG_NAME = "port";
    static final String BEAM_VERSION_ARG_NAME = "beam_version";

    @Option(name = "--" + PROJECT_NAME_ARG_NAME, usage = "Docker compose project name")
    private String projectName = "";

    @Option(name = "--" + COMMAND_ARG_NAME, usage = "Command to execute")
    private String command = "";

    @Option(name = "--" + PORT_ARG_NAME, usage = "Port for the transform service")
    private int port = -1;

    @Option(name = "--" + BEAM_VERSION_ARG_NAME, usage = "Beam version to use.")
    private String beamVersion = "";
  }

  public static void main(String[] args) throws IOException, TimeoutException {

    ArgConfig config = new ArgConfig();
    CmdLineParser parser = new CmdLineParser(config);

    try {
      parser.parseArgument(args);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      System.err.println("Valid options are:");
      // print the list of available options
      parser.printUsage(System.err);
      System.err.println();

      return;
    }

    if (config.command.isEmpty()) {
      throw new IllegalArgumentException(
          "\""
              + ArgConfig.COMMAND_ARG_NAME
              + "\" argument must be specified, Valid values are "
              + COMMAND_POSSIBLE_VALUES);
    }
    if (config.beamVersion.isEmpty()) {
      throw new IllegalArgumentException(
          "\"" + ArgConfig.BEAM_VERSION_ARG_NAME + "\" argument must be specified.");
    }

    System.out.println("===================================================");
    System.out.println(
        "Starting the Beam Transform Service at "
            + (config.port < 0
                ? "the default port."
                : ("port " + Integer.toString(config.port) + ".")));
    System.out.println("===================================================");

    TransformServiceLauncher service =
        TransformServiceLauncher.forProject(config.projectName, config.port);
    if (!config.beamVersion.isEmpty()) {
      service.setBeamVersion(config.beamVersion);
    }

    if (config.command.equals("up")) {
      service.start();
      service.waitTillUp(-1);
    } else if (config.command.equals("down")) {
      service.shutdown();
    } else if (config.command.equals("ps")) {
      service.status();
    } else {
      throw new IllegalArgumentException(
          String.format("Unknown command \"%s\". Possible values are {}", config.command));
    }
  }
}
