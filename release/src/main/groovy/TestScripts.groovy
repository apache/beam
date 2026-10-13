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

import groovy.util.CliBuilder

/*
 * Scripting functions to make writing a test similar to the quickstart
 * instructions from https://beam.apache.org/get-started/quickstart-java/
 */
class TestScripts {

    class BackgroundProcessInfo {
      String cmd
    }

   // Global state to maintain when running the steps
   class var {
     static File startDir
     static File curDir
     static String repoUrl
     static String ver
     static String gcpProject
     static String gcpRegion
     static String gcsBucket
     static String bqDataset
     static String pubsubTopic
     static String mavenLocalPath
     static List<Process> backgroundProcesses = Collections.synchronizedList(new ArrayList<Process>())
     static Map<Process, BackgroundProcessInfo> backgroundProcessInfo = Collections.synchronizedMap(new HashMap<Process, BackgroundProcessInfo>())
   }

   def TestScripts(String[] args) {
     def cli = new CliBuilder()
     cli.ver(args:1, 'SDL Version')
     cli.repourl(args:1, 'Repository URL')
     cli.gcpProject(args:1, 'Google Cloud Project')
     cli.gcpRegion(args:1, 'Google Cloud Region')
     cli.gcsBucket(args:1, 'Google Cloud Storage Bucket')
     cli.bqDataset(args:1, "BigQuery Dataset")
     cli.pubsubTopic(args:1, "PubSub Topic")
     cli.mavenLocalPath(args:1, "Maven local path")

     def options = cli.parse(args)
     var.repoUrl = options.repourl
     var.ver = options.ver
     println "Repository URL: ${var.repoUrl}"
     println "Version: ${var.ver}"
     if (options.gcpProject) {
       var.gcpProject = options.gcpProject
       println "GCS Project: ${var.gcpProject}"
     }
     if (options.gcpRegion) {
       var.gcpRegion = options.gcpRegion
       println "GCS Region: ${var.gcpRegion}"
     }
     if (options.gcsBucket) {
       var.gcsBucket = options.gcsBucket
       println "GCS Storage bucket: ${var.gcsBucket}"
     }
     if (options.bqDataset) {
         var.bqDataset = options.bqDataset
         println "BigQuery Dataset: ${var.bqDataset}"
     }
     if (options.pubsubTopic) {
         var.pubsubTopic = options.pubsubTopic
         println "PubSub Topic: ${var.pubsubTopic}"
     }
     if (options.mavenLocalPath) {
         var.mavenLocalPath = options.mavenLocalPath
         println "Maven local path: ${var.mavenLocalPath}"
     }

     Runtime.getRuntime().addShutdownHook(new Thread({
       stopAllBackgroundProcesses()
     }))
   }

   def ver() {
     return var.ver
   }

   def gcpProject() {
     return var.gcpProject
   }

  def gcpRegion() {
     return var.gcpRegion
   }

   def gcsBucket() {
     return var.gcsBucket
   }

   def bqDataset() {
     return var.bqDataset
   }

   def pubsubTopic() {
     return var.pubsubTopic
   }

   // Both documents the overal scenario and creates a clean temp directory
   def describe(String desc) {
     var.startDir = File.createTempDir()
     var.startDir.deleteOnExit()
     var.curDir = var.startDir
     print "**************************************\n* Scenario: ${desc}\n**************************************\n"
   }

   // Just document the intention of a set of steps
   def intent(String desc) {
     print "\n**************************************\n* Test: ${desc}\n**************************************\n\n"
   }

   def success(String desc) {
     print "\n**************************************\n* SUCCESS: ${desc}\n**************************************\n\n"
   }

   // Run a command
   public String run(String cmd) {
     println cmd
     if (cmd.startsWith("cd ")) {
       _chdir(cmd.substring(3))
       return ""
     } else if (cmd.startsWith("mvn ")) {
       return _mvn(cmd.substring(4))
     } else {
       return _execute(cmd)
     }
   }

   // Run a command in the background, returning the Process object.
   public Process runBackground(String cmd) {
     println cmd
     if (cmd.startsWith("mvn ")) {
       return _mvnBackground(cmd.substring(4))
     } else {
       return _executeBackground(cmd)
     }
   }

   // Check whether any background processes exited unexpectedly with a non-zero exit code
   public void checkBackgroundProcesses() {
     def procs = new ArrayList<>(var.backgroundProcesses)
     for (Process proc : procs) {
       if (proc != null && !proc.isAlive()) {
         int exitVal = proc.exitValue()
         if (exitVal != 0) {
           def info = var.backgroundProcessInfo.get(proc)
           String cmd = info ? info.cmd : "unknown command"
           error("Background command failed with exit code ${exitVal}: ${cmd}")
         }
       }
     }
   }

   // Stop/kill a background process and all its descendants.
   public void stopProcess(Process proc) {
     if (proc != null) {
       if (!proc.isAlive()) {
         int exitVal = proc.exitValue()
         var.backgroundProcesses.remove(proc)
         def info = var.backgroundProcessInfo.remove(proc)
         if (exitVal != 0) {
           String cmd = info ? info.cmd : "unknown command"
           error("Background command failed with exit code ${exitVal}: ${cmd}")
         }
       } else {
         try {
           proc.descendants().forEach { it.destroyForcibly() }
         } catch (Throwable ignored) {
         }
         proc.destroyForcibly()
         proc.waitFor(10, java.util.concurrent.TimeUnit.SECONDS)
         var.backgroundProcesses.remove(proc)
         var.backgroundProcessInfo.remove(proc)
       }
     }
   }

   // Stop all active background processes.
   public void stopAllBackgroundProcesses() {
     def procs = new ArrayList<>(var.backgroundProcesses)
     procs.each { proc ->
       if (proc != null && proc.isAlive()) {
         try {
           proc.descendants().forEach { it.destroyForcibly() }
         } catch (Throwable ignored) {
         }
         proc.destroyForcibly()
         try {
           proc.waitFor(10, java.util.concurrent.TimeUnit.SECONDS)
         } catch (Throwable ignored) {
         }
       }
       var.backgroundProcesses.remove(proc)
       var.backgroundProcessInfo.remove(proc)
     }
   }

   // Check for expected results in actual stdout from previous command, if fails, log errors then exit.
   public void see(String expected, String actual) {
     if (!actual.contains(expected)) {
       var.startDir.deleteDir()
       println "Cannot find ${expected} in ${actual}"
       error("Cannot find expected text")
     }
     println "Verified $expected"
   }

   // Check if there are one or more matches in stdout of the last command.
   public boolean seeAnyOf(List<String> expecteds, String actual) {
     for (String expected: expecteds) {
       if(actual.contains(expected)) {
         println "Verified $expected"
         return true
       }
     }
     println "Cannot find ${expecteds} in text"
     return false
   }

   // Cleanup and print success
   public void done() {
     checkBackgroundProcesses()
     stopAllBackgroundProcesses()
     var.startDir.deleteDir()
     println "[SUCCESS]"
     System.exit(0)
   }

   // Run a single command, capture output, verify return code is 0
   private String _execute(String cmd) {
     checkBackgroundProcesses()
     def shell = "sh -c cmd".split(' ')
     shell[2] = cmd
     def pb = new ProcessBuilder(shell)
     pb.directory(var.curDir)
     pb.redirectErrorStream(true)
     def proc = pb.start()
     String output_text = ""
     def text = new StringBuilder()
     proc.inputStream.eachLine {
       println it
       text.append(it + "\n")
     }
     proc.waitFor()
     output_text = text.toString().trim()
     if (proc.exitValue() != 0) {
       println output_text
       error("Failed command")
     }
     return output_text
   }

   // Run a single command asynchronously in the background
   private Process _executeBackground(String cmd) {
     def shell = "sh -c cmd".split(' ')
     shell[2] = cmd
     def pb = new ProcessBuilder(shell)
     pb.directory(var.curDir)
     pb.redirectErrorStream(true)
     def proc = pb.start()
     var.backgroundProcesses.add(proc)
     var.backgroundProcessInfo.put(proc, new BackgroundProcessInfo(cmd: cmd))
     Thread.startDaemon {
       try {
         proc.inputStream.eachLine {
           println it
         }
       } catch (Throwable ignored) {
       }
     }
     return proc
   }

   // Change directory
   private void _chdir(String subdir) {
     var.curDir = new File(var.curDir.absolutePath, subdir)
     if (!var.curDir.exists()) {
       error("Directory ${var.curDir} not found")
     }
   }

   // Build the maven command string with custom repository and settings.xml
   private String _buildMvnCmd(String args) {
     String mvnlocalPath = var.mavenLocalPath
     if (!(var.mavenLocalPath)) {
       mvnlocalPath = var.startDir
     }
     def m2 = new File(mvnlocalPath, ".m2/repository")
     m2.mkdirs()
     def settings = new File(mvnlocalPath, "settings.xml")
     if (!settings.exists()) {
       settings.write """
        <settings>
          <localRepository>${m2.absolutePath}</localRepository>
            <profiles>
              <profile>
                <id>testrel</id>
                  <repositories>
                    <repository>
                      <id>test.release</id>
                      <url>${var.repoUrl}</url>
                    </repository>
                  </repositories>
                </profile>
              </profiles>
         </settings>
          """
     }
     def cmd = "mvn ${args} -s ${settings.absolutePath} -Ptestrel -B"
     String path = System.getenv("PATH")
     // Set the path on jenkins executors to use a recent maven
     // MAVEN_HOME is not set on some executors, so default to 3.5.2
     String maven_home = System.getenv("MAVEN_HOME") ?: '/usr/local/maven'
     println "Using maven ${maven_home}"
     def mvnPath = "${maven_home}/bin"
     def setPath = "export PATH=\"${mvnPath}:${path}\" && "
     return setPath + cmd
   }

   // Run a maven command, setting up a new local repository and a settings.xml with a custom repository if needed
   private String _mvn(String args) {
     return _execute(_buildMvnCmd(args))
   }

   // Run a maven command in the background
   private Process _mvnBackground(String args) {
     return _executeBackground(_buildMvnCmd(args))
   }

   // Clean up and report error
   public void error(String text) {
     stopAllBackgroundProcesses()
     var.startDir.deleteDir()
     println "[ERROR] $text"
     System.exit(1)
   }
}
