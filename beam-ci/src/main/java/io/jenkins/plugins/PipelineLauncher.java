package io.jenkins.plugins;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
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

        public InputStream getInputStream() {
            return this.process.getInputStream();
        }

        public InputStream getErrorStream() {
            return this.process.getErrorStream();
        }

        public int waitFor() throws InterruptedException {
            return this.process.waitFor();
        }

        public void destroy() {
            this.process.destroy();
        }
    }
}
