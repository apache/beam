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

    public LaunchProcess start() throws IOException {
        Process process = this.processBuilder.start();
        return new LaunchProcess(process);
    }

    public static class LaunchProcess {

        private Process process;

        public LaunchProcess(Process process) {
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
