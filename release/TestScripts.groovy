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

/*
 * Scripting functions to make writing a test similar to the quickstart
 * instructions from https://beam.apache.org/get-started/quickstart-java/
 */
class TestScripts {
     
   // Global state to maintain when running the steps
   class var {
     static File startDir 
     static File curDir
     static String lastText
   }
   
   // Both documents the overal scenario and creates a clean temp directory
   def describe(String desc) {
     var.startDir = File.createTempDir()
     var.startDir.deleteOnExit()
     var.curDir = var.startDir
     print "*****\n* Scenario: ${desc}\n*****\n"
   }
   
   // Just document the intention of a set of steps
   def intent(String desc) { 
     print "\n*****\n* Test: ${desc}\n*****\n\n"
   }
   
     
   // Run a command
   public void run(String cmd) {
     if (cmd.startsWith("cd ")) {
       _chdir(cmd.substring(3))
     } else if (cmd.startsWith("mvn ")) {
       _mvn(cmd.substring(4))
     } else {
       _execute(cmd)
     }
   }
   
   // Check for expected results in stdout of the last command
   public void see(String expected) {
     if (!var.lastText.contains(expected)) {
       var.startDir.deleteDir()
       println "Cannot find ${expected} in ${var.lastText}"
       _error("Cannot find expected text")
     }
     println "Verified $expected"
   }
   
   // Cleanup and print success
   public void done() {
     var.startDir.deleteDir()
     println "[SUCCESS]"
     System.exit(0)
   }

   // Run a single command, capture output, verify return code is 0
   private void _execute(String cmd) {
     def shell = "sh -c cmd".split(' ')
     shell[2] = cmd
     def pb = new ProcessBuilder(shell)
     pb.directory(var.curDir)
     def proc = pb.start()
     var.lastText = ""
     proc.inputStream.eachLine {
       println it
       var.lastText += it + "\n";
     }
     var.lastText = var.lastText.trim()
     proc.waitFor()
     if (proc.exitValue() != 0) {
       println var.lastText
       _error("Failed command")
     }
   }
   
   // Change directory
   private void _chdir(String subdir) {
     var.curDir = new File(var.curDir.absolutePath, subdir)
     if (!var.curDir.exists()) {
       _error("Directory ${var.curDir} not found")
     }
     _execute("pwd")
     if (var.lastText != var.curDir.absolutePath) {
       _error("Directory mismatch, ${var.lastText} != ${var.curDir.absolutePath}")
   
     }
   }
   
   // Run a maven command, setting up a new local repository and a settings.xml with the snapshot repository
   private void _mvn(String args) {
     def m2 = new File(var.startDir, ".m2/repository")
     m2.mkdirs()
     def settings = new File(var.startDir, "settings.xml")
     def repo = System.env.snapshot_repository ?: "https://repository.apache.org/content/repositories/snapshots"
     settings.write """
       <settings>
         <localRepository>${m2.absolutePath}</localRepository>
           <profiles>
             <profile>
               <id>snapshot</id>
                 <repositories>
                   <repository>
                     <id>apache.snapshots</id>
                     <url>${repo}</url>
                   </repository>
                 </repositories>
               </profile>
             </profiles>
        </settings>
        """
       def cmd = "mvn ${args} -s${settings.absolutePath} -Psnapshot -B"
       _execute(cmd)
   }

   // Clean up and report error
   private void _error(String text) {
     var.startDir.deleteDir()
     println "[ERROR] $text"
     System.exit(1)
   }
}
