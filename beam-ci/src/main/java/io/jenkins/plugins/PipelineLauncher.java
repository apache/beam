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

import java.io.*;
import java.util.List;
import java.util.Map;

public class PipelineLauncher {

    private ProcessBuilder processBuilder;

    public PipelineLauncher(String pathToCredentials, File workspace) {
        this.processBuilder = new ProcessBuilder();

        // add environment variable
        Map<String, String> env = this.processBuilder.environment();
        env.put("GOOGLE_APPLICATION_CREDENTIALS", pathToCredentials);

        // set correct directory to be running command
        this.processBuilder.directory(workspace);
    }

    public void command(List<String> command) {
        this.processBuilder.command(command);
    }

    public LaunchedProcess start() throws IOException {
        Process process = this.processBuilder.start();
        return new LaunchedProcess(process);
    }

    public static class LaunchedProcess {

        private Process process;

        public LaunchedProcess(Process process) {
            this.process = process;
        }

        public BufferedReader getInputStream() {
            BufferedReader reader = new BufferedReader(new InputStreamReader(this.process.getInputStream()));
            return reader;
        }

        public BufferedReader getErrorStream() {
            BufferedReader reader = new BufferedReader(new InputStreamReader(this.process.getErrorStream()));
            return reader;
        }

        public int waitFor() throws InterruptedException {
            return this.process.waitFor();
        }

        public void destroy() {
            this.process.destroy();
        }
    }
}
