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
package io.jenkins.plugins;

import hudson.Launcher;
import hudson.Extension;
import hudson.FilePath;
import hudson.util.FormValidation;
import hudson.model.AbstractProject;
import hudson.model.Run;
import hudson.model.TaskListener;
import hudson.tasks.Builder;
import hudson.tasks.BuildStepDescriptor;
import jenkins.util.SystemProperties;
import org.kohsuke.stapler.DataBoundConstructor;
import org.kohsuke.stapler.QueryParameter;

import javax.servlet.ServletException;
import java.io.*;
import java.util.*;

import jenkins.tasks.SimpleBuildStep;

public class ExecuteBeamPipelineOnDataflowBuilder extends Builder implements SimpleBuildStep {

    private ProcessBuilder processBuilder;
    private final String fileName = System.getProperty("user.dir") + "/src/main/java/io/jenkins/plugins/executePythonBeamPipeline.sh";
    private final String pathToCreds;
    private final String pathToMainClass;
    private final String pipelineOptions;
    private final String buildReleaseOptions;
    private boolean useJava; // if false, use Python
    private boolean useGradle; // if false, use Maven
    private ArrayList<String> command;
    private boolean test; // if we are testing, the mock ProcessBuilder never starts

    @DataBoundConstructor
    public ExecuteBeamPipelineOnDataflowBuilder(String pathToCreds, String pathToMainClass, String pipelineOptions, String buildReleaseOptions, boolean useJava, boolean useGradle) {
        this.pathToCreds = pathToCreds;
        this.pathToMainClass = pathToMainClass;
        this.pipelineOptions = pipelineOptions;
        this.buildReleaseOptions = buildReleaseOptions;
        this.useJava = useJava;
        this.useGradle = useGradle;
        this.test = false;
    }

    public void setProcessBuilder(ProcessBuilder processBuilder) {
        this.processBuilder = processBuilder;
        this.test = true;
    }

    public String getPathToCreds() { return pathToCreds; }

    public String getPathToMainClass() {
        return pathToMainClass;
    }

    public String getPipelineOptions() {
        return pipelineOptions;
    }

    public String getBuildReleaseOptions() {
        return buildReleaseOptions;
    }

    public boolean getUseJava() {
        return useJava;
    }

    public boolean getUseGradle() {
        return useGradle;
    }

    public ArrayList<String> getCommand() { return command; }

    /**
     * Builds and sets the command on the ProcessBuilder depending on configurations set by the user
     * */
    private void buildCommand(Run<?, ?> run, String workspace, PrintStream logger) {
        if (this.useJava) {
            String pipelineOptions = this.pipelineOptions.replaceAll("[\\t\\n]+"," ");
            if (this.useGradle) { // gradle
                command = new ArrayList<>(Arrays.asList("gradle", "clean", "execute", "-DmainClass=" + this.pathToMainClass, "-Dexec.args=" + pipelineOptions));
            } else { // maven
                command = new ArrayList<>(Arrays.asList("mvn", "compile", "exec:java", "-Dexec.mainClass=" + this.pathToMainClass, "-Dexec.args=" + pipelineOptions));
            }

            // add pipeline and build release options if included
            if (!this.buildReleaseOptions.equals("")) {
                String[] buildReleaseOptions = this.buildReleaseOptions.split("\\s+"); // split build release options by whitespace
                command.addAll(Arrays.asList(buildReleaseOptions)); // add build release options as separate list elements
            }
        } else { // python
            // Get Path to the current build directory to create Virtual Environment in
            String jobBuildPathDirectory = getJobBuildDirectory(run);

            // Execute Bash Script
            command = new ArrayList<>(Arrays.asList(this.fileName, workspace, jobBuildPathDirectory, this.pathToMainClass, this.pipelineOptions, this.buildReleaseOptions));
        }
        logger.println("Command: " + command);
        this.processBuilder.command(command);
    }

    /**
     * @return absolute path to current build folder
     * */
    private String getJobBuildDirectory(Run<?, ?> run) {
        String jenkinsHome = System.getProperty("JENKINS_HOME");
        String jobName = run.getFullDisplayName().split(" ")[0];
        int buildNumber = run.getNumber();
        return jenkinsHome + "/jobs/" + jobName + "/builds/" + buildNumber;
    }

    @Override
    public void perform(Run<?, ?> run, FilePath workspace, Launcher launcher, TaskListener listener) throws InterruptedException, IOException {
//        ProcessBuilder processBuilder = new ProcessBuilder();
        if (processBuilder == null) {
            this.processBuilder = new ProcessBuilder();
        }

        // see that all configurations are received correctly
        listener.getLogger().println("path to google cloud credentials : " + this.pathToCreds);
        listener.getLogger().println("path to main class : " + this.pathToMainClass);
        listener.getLogger().println("pipeline options : " + this.pipelineOptions);
        listener.getLogger().println("build release options : " + this.buildReleaseOptions);
        listener.getLogger().println("use java: " + this.useJava);
        listener.getLogger().println("use gradle: " + this.useGradle);

        Map<String, String> env = this.processBuilder.environment();
        env.put("GOOGLE_APPLICATION_CREDENTIALS", this.pathToCreds);

        // set correct directory to be running command
        this.processBuilder.directory(new File(workspace.toURI()));

        // build and set command to processBuilder based on configurations
        buildCommand(run, workspace.toURI().getPath(), listener.getLogger());

        if (this.test) { // if we are testing commands only, return before starting process
            listener.getLogger().println("Test finished successfully.");
            return;
        }

        Process process = this.processBuilder.start();

        BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
        String line;
        while((line = reader.readLine()) != null) {
            listener.getLogger().println(line);
        }

        BufferedReader reader2 = new BufferedReader(new InputStreamReader(process.getErrorStream()));
        while((line = reader2.readLine()) != null) {
            listener.getLogger().println(line);
        }

        int exitCode = process.waitFor();
        listener.getLogger().println("\n Exited with error code : " + exitCode);
    }

    @Extension
    public static final class DescriptorImpl extends BuildStepDescriptor<Builder> {

        public FormValidation doCheckPathToMainClass(@QueryParameter String value) {
            if (value.length() == 0)
                return FormValidation.error("Missing Path to Main Class");
            return FormValidation.ok();
        }

        public FormValidation doCheckPipelineOptions(@QueryParameter String value) {
            if (value.length() == 0)
                return FormValidation.error("Missing Pipeline Options");
            return FormValidation.ok();
        }

        @Override
        public boolean isApplicable(Class<? extends AbstractProject> aClass) {
            return true;
        }

        @Override
        public String getDisplayName() {
            return "Execute Beam Pipeline on Dataflow";
        }

    }

}
