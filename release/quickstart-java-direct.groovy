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

t = new TestScripts()  

/*
 * Run the direct quickstart from https://beam.apache.org/get-started/quickstart-java/
 */

t.describe 'Run Apache Beam Java SDK Quickstart - Direct'

  t.intent 'Gets the WordCount Code'
    ver = System.env.snapshot_version ?: "2.3.0-SNAPSHOT"

    // Generate a maven project from the snapshot repository
    t.run """mvn archetype:generate \
      -DarchetypeGroupId=org.apache.beam \
      -DarchetypeArtifactId=beam-sdks-java-maven-archetypes-examples \
      -DarchetypeVersion=$ver \
      -DgroupId=org.example \
      -DartifactId=word-count-beam \
      -Dversion="0.1" \
      -Dpackage=org.apache.beam.examples \
      -DinteractiveMode=false"""

    // Check if it was generated
    t.see "[INFO] BUILD SUCCESS"
    t.run "cd word-count-beam"
    t.run "ls"
    t.see "pom.xml"
    t.see "src"
    t.run "ls src/main/java/org/apache/beam/examples/"
    t.see "WordCount.java"

  t.intent 'Runs the WordCount Code with Direct runner'

    // Run the workcount example with the direct runner
    t.run """mvn compile exec:java \
      -Dexec.mainClass=org.apache.beam.examples.WordCount \
      -Dexec.args="--inputFile=pom.xml --output=counts" \
      -Pdirect-runner"""

    // Verify text from the pom.xml input file
    t.run "grep Foundation counts*"
    t.see "Foundation: 1"

    // Clean up
    t.done()
